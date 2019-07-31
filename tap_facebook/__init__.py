#!/usr/bin/env python3
import json
import os
import os.path
import sys
import time

import datetime
from datetime import timezone
import dateutil

import attr
import pendulum
import requests
import backoff

import singer
import singer.metrics as metrics
from singer import utils
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)
from singer.catalog import Catalog, CatalogEntry

from facebook_business import FacebookAdsApi
import facebook_business.adobjects.adcreative as adcreative
import facebook_business.adobjects.ad as fb_ad
import facebook_business.adobjects.adset as adset
import facebook_business.adobjects.campaign as fb_campaign
import facebook_business.adobjects.adsinsights as adsinsights
import facebook_business.adobjects.user as fb_user

from facebook_business.exceptions import FacebookRequestError

TODAY = pendulum.today()

INSIGHTS_MAX_WAIT_TO_START_SECONDS = 2 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60
INSIGHTS_MAX_ASYNC_SLEEP_SECONDS = 5 * 60

RESULT_RETURN_LIMIT = 100

STREAMS = [
    'adcreative',
    'ads',
    'adsets',
    'campaigns',
    'ads_insights',
    'ads_insights_age_and_gender',
    'ads_insights_country',
    'ads_insights_platform_and_device',
    'ads_insights_region',
    'ads_insights_dma',
]

REQUIRED_CONFIG_KEYS = ['start_date', 'account_id', 'access_token']
UPDATED_TIME_KEY = 'updated_time'
START_DATE_KEY = 'date_start'

BOOKMARK_KEYS = {
    'ads': UPDATED_TIME_KEY,
    'adsets': UPDATED_TIME_KEY,
    'campaigns': UPDATED_TIME_KEY,
    'ads_insights': START_DATE_KEY,
    'ads_insights_age_and_gender': START_DATE_KEY,
    'ads_insights_country': START_DATE_KEY,
    'ads_insights_platform_and_device': START_DATE_KEY,
    'ads_insights_region': START_DATE_KEY,
    'ads_insights_dma': START_DATE_KEY,
}

LOGGER = singer.get_logger()

CONFIG = {}

class TapFacebookException(Exception):
    pass

class InsightsJobTimeout(TapFacebookException):
    pass

def transform_datetime_string(dts):
    parsed_dt = dateutil.parser.parse(dts)
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    else:
        parsed_dt = parsed_dt.astimezone(timezone.utc)
    return singer.strftime(parsed_dt)

def iter_delivery_info_filter(stream_type):
    filt = {
        "field": stream_type + ".delivery_info",
        "operator": "IN",
    }

    filt_values = [
        "active", "archived", "completed",
        "limited", "not_delivering", "deleted",
        "not_published", "pending_review", "permanently_deleted",
        "recently_completed", "recently_rejected", "rejected",
        "scheduled", "inactive"]

    sub_list_length = 3
    for i in range(0, len(filt_values), sub_list_length):
        filt['value'] = filt_values[i:i+sub_list_length]
        yield filt

def retry_pattern(backoff_type, exception, **wait_gen_kwargs):
    # HACK: Workaround added due to bug with Facebook prematurely deprecating 'relevance_score'
    # Issue being tracked here: https://developers.facebook.com/support/bugs/2489592517771422
    def is_relevance_score(exception):
        if getattr(exception, "body", None):
            return exception.body().get("error", {}).get("message") == '(#100) relevance_score is not valid for fields param. please check https://developers.facebook.com/docs/marketing-api/reference/ads-insights/ for all valid values'
        else:
            return False

    def log_retry_attempt(details):
        _, exception, _ = sys.exc_info()
        if is_relevance_score(exception):
            raise Exception("Due to a bug with Facebook prematurely deprecating 'relevance_score' that is "
                            "not affecting all tap-facebook users in the same way, you need to "
                            "deselect `relevance_score` from your Insights export. For further "
                            "information, please see this Facebook bug report thread: "
                            "https://developers.facebook.com/support/bugs/2489592517771422") from exception
        LOGGER.info(exception)
        LOGGER.info('Caught retryable error after %s tries. Waiting %s more seconds then retrying...',
                    details["tries"],
                    details["wait"])

    def should_retry_api_error(exception):
        if isinstance(exception, FacebookRequestError):
            return exception.api_transient_error() or exception.api_error_subcode() == 99 or is_relevance_score(exception)
        elif isinstance(exception, InsightsJobTimeout):
            return True
        return False

    return backoff.on_exception(
        backoff_type,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda exc: not should_retry_api_error(exc),
        **wait_gen_kwargs
    )

@attr.s
class Stream(object):

    name = attr.ib()
    account = attr.ib()
    stream_alias = attr.ib()
    annotated_schema = attr.ib()

    def automatic_fields(self):
        fields = set()
        if self.annotated_schema:
            props = self.annotated_schema.properties # pylint: disable=no-member
            for k, val in props.items():
                inclusion = val.inclusion
                if inclusion == 'automatic':
                    fields.add(k)
        return fields


    def fields(self):
        fields = set()
        if self.annotated_schema:
            props = self.annotated_schema.properties # pylint: disable=no-member
            for k, val in props.items():
                inclusion = val.inclusion
                selected = val.selected
                if selected or inclusion == 'automatic':
                    fields.add(k)
        return fields

@attr.s
class IncrementalStream(Stream):

    state = attr.ib()

    def __attrs_post_init__(self):
        self.current_bookmark = get_start(self, UPDATED_TIME_KEY)

    def _iterate(self, generator, record_preparation):
        max_bookmark = None
        for recordset in generator:
            for record in recordset:
                updated_at = pendulum.parse(record[UPDATED_TIME_KEY])

                if self.current_bookmark and self.current_bookmark >= updated_at:
                    continue
                if not max_bookmark or updated_at > max_bookmark:
                    max_bookmark = updated_at

                record = record_preparation(record)
                yield {'record': record}

            if max_bookmark:
                yield {'state': advance_bookmark(self, UPDATED_TIME_KEY, str(max_bookmark))}

class AdCreative(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    '''

    field_class = adcreative.AdCreative.Field
    key_properties = ['id']

    def __iter__(self):
        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request():
            return self.account.get_ad_creatives(fields=self.fields(), # pylint: disable=no-member
                                                 params={'limit': RESULT_RETURN_LIMIT})
        ad_creative = do_request()
        for a in ad_creative: # pylint: disable=invalid-name
            yield {'record': a.export_all_data()}


class Ads(IncrementalStream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
    '''

    field_class = fb_ad.Ad.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request():
            params = {'limit': RESULT_RETURN_LIMIT}
            if self.current_bookmark:
                params.update({'filtering': [{'field': 'ad.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp}]})
            yield self.account.get_ads(fields=self.automatic_fields(), params=params) # pylint: disable=no-member

        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request_multiple():
            params = {'limit': RESULT_RETURN_LIMIT}
            bookmark_params = []
            if self.current_bookmark:
                bookmark_params.append({'field': 'ad.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp})
            for del_info_filt in iter_delivery_info_filter('ad'):
                params.update({'filtering': [del_info_filt] + bookmark_params})
                filt_ads = self.account.get_ads(fields=self.automatic_fields(), params=params) # pylint: disable=no-member
                yield filt_ads

        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def prepare_record(ad):
            return ad.remote_read(fields=self.fields()).export_all_data()

        if CONFIG.get('include_deleted', 'false').lower() == 'true':
            ads = do_request_multiple()
        else:
            ads = do_request()
        for message in self._iterate(ads, prepare_record):
            yield message


class AdSets(IncrementalStream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign
    '''

    field_class = adset.AdSet.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request():
            params = {'limit': RESULT_RETURN_LIMIT}
            if self.current_bookmark:
                params.update({'filtering': [{'field': 'adset.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp}]})
            yield self.account.get_ad_sets(fields=self.automatic_fields(), params=params) # pylint: disable=no-member

        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request_multiple():
            params = {'limit': RESULT_RETURN_LIMIT}
            bookmark_params = []
            if self.current_bookmark:
                bookmark_params.append({'field': 'adset.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp})
            for del_info_filt in iter_delivery_info_filter('adset'):
                params.update({'filtering': [del_info_filt] + bookmark_params})
                filt_adsets = self.account.get_ad_sets(fields=self.automatic_fields(), params=params) # pylint: disable=no-member
                yield filt_adsets

        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def prepare_record(ad_set):
            return ad_set.remote_read(fields=self.fields()).export_all_data()

        if CONFIG.get('include_deleted', 'false').lower() == 'true':
            ad_sets = do_request_multiple()
        else:
            ad_sets = do_request()

        for message in self._iterate(ad_sets, prepare_record):
            yield message

class Campaigns(IncrementalStream):

    field_class = fb_campaign.Campaign.Field
    key_properties = ['id']

    def __iter__(self):
        props = self.fields()
        fields = [k for k in props if k != 'ads']
        pull_ads = 'ads' in props

        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request():
            params = {'limit': RESULT_RETURN_LIMIT}
            if self.current_bookmark:
                params.update({'filtering': [{'field': 'campaign.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp}]})
            yield self.account.get_campaigns(fields=self.automatic_fields(), params=params) # pylint: disable=no-member

        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request_multiple():
            params = {'limit': RESULT_RETURN_LIMIT}
            bookmark_params = []
            if self.current_bookmark:
                bookmark_params.append({'field': 'campaign.' + UPDATED_TIME_KEY, 'operator': 'GREATER_THAN', 'value': self.current_bookmark.int_timestamp})
            for del_info_filt in iter_delivery_info_filter('campaign'):
                params.update({'filtering': [del_info_filt] + bookmark_params})
                filt_campaigns = self.account.get_campaigns(fields=self.automatic_fields(), params=params) # pylint: disable=no-member
                yield filt_campaigns

        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def prepare_record(campaign):
            campaign_out = campaign.remote_read(fields=fields).export_all_data()
            if pull_ads:
                campaign_out['ads'] = {'data': []}
                ids = [ad['id'] for ad in campaign.get_ads()]
                for ad_id in ids:
                    campaign_out['ads']['data'].append({'id': ad_id})
            return campaign_out

        if CONFIG.get('include_deleted', 'false').lower() == 'true':
            campaigns = do_request_multiple()
        else:
            campaigns = do_request()

        for message in self._iterate(campaigns, prepare_record):
            yield message


ALL_ACTION_ATTRIBUTION_WINDOWS = [
    '1d_click',
    '7d_click',
    '28d_click',
    '1d_view',
    '7d_view',
    '28d_view'
]

ALL_ACTION_BREAKDOWNS = [
    'action_type',
    'action_target_id',
    'action_destination'
]

def get_start(stream, bookmark_key):
    tap_stream_id = stream.name
    state = stream.state or {}
    current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        if isinstance(stream, IncrementalStream):
            return None
        else:
            LOGGER.info("no bookmark found for %s, using start_date instead...%s", tap_stream_id, CONFIG['start_date'])
            return pendulum.parse(CONFIG['start_date'])
    LOGGER.info("found current bookmark for %s:  %s", tap_stream_id, current_bookmark)
    return pendulum.parse(current_bookmark)

def advance_bookmark(stream, bookmark_key, date):
    tap_stream_id = stream.name
    state = stream.state or {}
    LOGGER.info('advance(%s, %s)', tap_stream_id, date)
    date = pendulum.parse(date) if date else None
    current_bookmark = get_start(stream, bookmark_key)

    if date is None:
        LOGGER.info('Did not get a date for stream %s '+
                    ' not advancing bookmark',
                    tap_stream_id)
    elif not current_bookmark or date > current_bookmark:
        LOGGER.info('Bookmark for stream %s is currently %s, ' +
                    'advancing to %s',
                    tap_stream_id, current_bookmark, date)
        state = singer.write_bookmark(state, tap_stream_id, bookmark_key, str(date))
    else:
        LOGGER.info('Bookmark for stream %s is currently %s ' +
                    'not changing to %s',
                    tap_stream_id, current_bookmark, date)
    return state

@attr.s
class AdsInsights(Stream):
    field_class = adsinsights.AdsInsights.Field
    base_properties = ['campaign_id', 'adset_id', 'ad_id', 'date_start']

    state = attr.ib()
    options = attr.ib()
    action_breakdowns = attr.ib(default=ALL_ACTION_BREAKDOWNS)
    level = attr.ib(default='ad')
    action_attribution_windows = attr.ib(
        default=ALL_ACTION_ATTRIBUTION_WINDOWS)
    time_increment = attr.ib(default=1)
    limit = attr.ib(default=RESULT_RETURN_LIMIT)

    bookmark_key = START_DATE_KEY

    invalid_insights_fields = ['impression_device', 'publisher_platform', 'platform_position',
                               'age', 'gender', 'country', 'placement', 'region', 'dma']

    # pylint: disable=no-member,unsubscriptable-object,attribute-defined-outside-init
    def __attrs_post_init__(self):
        self.breakdowns = self.options.get('breakdowns') or []
        self.key_properties = self.base_properties[:]
        if self.options.get('primary-keys'):
            self.key_properties.extend(self.options['primary-keys'])

    def job_params(self):
        start_date = get_start(self, self.bookmark_key)

        buffer_days = 28
        if CONFIG.get('insights_buffer_days'):
            buffer_days = int(CONFIG.get('insights_buffer_days'))

        buffered_start_date = start_date.subtract(days=buffer_days)

        end_date = pendulum.now()
        if CONFIG.get('end_date'):
            end_date = pendulum.parse(CONFIG.get('end_date'))

        # Some automatic fields (primary-keys) cannot be used as 'fields' query params.
        while buffered_start_date <= end_date:
            yield {
                'level': self.level,
                'action_breakdowns': list(self.action_breakdowns),
                'breakdowns': list(self.breakdowns),
                'limit': self.limit,
                'fields': list(self.fields().difference(self.invalid_insights_fields)),
                'time_increment': self.time_increment,
                'action_attribution_windows': list(self.action_attribution_windows),
                'time_ranges': [{'since': buffered_start_date.to_date_string(),
                                 'until': buffered_start_date.to_date_string()}]
            }
            buffered_start_date = buffered_start_date.add(days=1)

    @retry_pattern(backoff.expo, (FacebookRequestError, InsightsJobTimeout), max_tries=5, factor=5)
    def run_job(self, params):
        LOGGER.info('Starting adsinsights job with params %s', params)
        job = self.account.get_insights( # pylint: disable=no-member
            params=params,
            is_async=True)
        status = None
        time_start = time.time()
        sleep_time = 10
        while status != "Job Completed":
            duration = time.time() - time_start
            job = job.remote_read()
            status = job['async_status']
            percent_complete = job['async_percent_completion']

            job_id = job['id']
            LOGGER.info('%s, %d%% done', status, percent_complete)

            if status == "Job Completed":
                return job

            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                pretty_error_message = ('Insights job {} did not start after {} seconds. ' +
                                        'This is an intermittent error and may resolve itself on subsequent queries to the Facebook API. ' +
                                        'You should deselect fields from the schema that are not necessary, ' +
                                        'as that may help improve the reliability of the Facebook API.')
                raise InsightsJobTimeout(pretty_error_message.format(job_id, INSIGHTS_MAX_WAIT_TO_START_SECONDS))
            elif duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS and status != "Job Completed":
                pretty_error_message = ('Insights job {} did not complete after {} seconds. ' +
                                        'This is an intermittent error and may resolve itself on subsequent queries to the Facebook API. ' +
                                        'You should deselect fields from the schema that are not necessary, ' +
                                        'as that may help improve the reliability of the Facebook API.')
                raise InsightsJobTimeout(pretty_error_message.format(job_id,
                                                                     INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS//60))

            LOGGER.info("sleeping for %d seconds until job is done", sleep_time)
            time.sleep(sleep_time)
            if sleep_time < INSIGHTS_MAX_ASYNC_SLEEP_SECONDS:
                sleep_time = 2 * sleep_time
        return job

    def __iter__(self):
        for params in self.job_params():
            with metrics.job_timer('insights'):
                job = self.run_job(params)

            min_date_start_for_job = None
            count = 0
            for obj in job.get_result():
                count += 1
                rec = obj.export_all_data()
                if not min_date_start_for_job or rec['date_stop'] < min_date_start_for_job:
                    min_date_start_for_job = rec['date_stop']
                yield {'record': rec}
            LOGGER.info('Got %d results for insights job', count)

            # when min_date_start_for_job stays None, we should
            # still update the bookmark using 'until' in time_ranges
            if min_date_start_for_job is None:
                for time_range in params['time_ranges']:
                    if time_range['until']:
                        min_date_start_for_job = time_range['until']
            yield {'state': advance_bookmark(self, self.bookmark_key,
                                             min_date_start_for_job)} # pylint: disable=no-member


INSIGHTS_BREAKDOWNS_OPTIONS = {
    'ads_insights': {"breakdowns": []},
    'ads_insights_age_and_gender': {"breakdowns": ['age', 'gender'],
                                    "primary-keys": ['age', 'gender']},
    'ads_insights_country': {"breakdowns": ['country']},
    'ads_insights_platform_and_device': {"breakdowns": ['publisher_platform',
                                                        'platform_position', 'impression_device'],
                                         "primary-keys": ['publisher_platform',
                                                          'platform_position', 'impression_device']},
    'ads_insights_region': {'breakdowns': ['region'],
                            'primary-keys': ['region']},
    'ads_insights_dma': {"breakdowns": ['dma'],
                         "primary-keys": ['dma']},
}


def initialize_stream(name, account, stream_alias, annotated_schema, state): # pylint: disable=too-many-return-statements

    if name in INSIGHTS_BREAKDOWNS_OPTIONS:
        return AdsInsights(name, account, stream_alias, annotated_schema, state=state,
                           options=INSIGHTS_BREAKDOWNS_OPTIONS[name])
    elif name == 'campaigns':
        return Campaigns(name, account, stream_alias, annotated_schema, state=state)
    elif name == 'adsets':
        return AdSets(name, account, stream_alias, annotated_schema, state=state)
    elif name == 'ads':
        return Ads(name, account, stream_alias, annotated_schema, state=state)
    elif name == 'adcreative':
        return AdCreative(name, account, stream_alias, annotated_schema)
    else:
        raise TapFacebookException('Unknown stream {}'.format(name))


def get_streams_to_sync(account, catalog, state):
    streams = []
    for stream in STREAMS:
        selected_stream = next((s for s in catalog.streams if s.tap_stream_id == stream), None)
        if selected_stream and selected_stream.schema.selected:
            schema = selected_stream.schema
            name = selected_stream.stream
            stream_alias = selected_stream.stream_alias
            streams.append(initialize_stream(name, account, stream_alias, schema, state))
    return streams

def transform_date_hook(data, typ, schema):
    if typ == 'string' and schema.get('format') == 'date-time' and isinstance(data, str):
        transformed = transform_datetime_string(data)
        return transformed
    return data

def do_sync(account, catalog, state):
    streams_to_sync = get_streams_to_sync(account, catalog, state)
    refs = load_shared_schema_refs()
    for stream in streams_to_sync:
        LOGGER.info('Syncing %s, fields %s', stream.name, stream.fields())
        schema = singer.resolve_schema_references(load_schema(stream), refs)
        bookmark_key = BOOKMARK_KEYS.get(stream.name)
        singer.write_schema(stream.name, schema, stream.key_properties, bookmark_key, stream.stream_alias)
        with Transformer(pre_hook=transform_date_hook) as transformer:
            with metrics.record_counter(stream.name) as counter:
                for message in stream:
                    if 'record' in message:
                        counter.increment()
                        time_extracted = utils.now()
                        record = transformer.transform(message['record'], schema)
                        singer.write_record(stream.name, record, stream.stream_alias, time_extracted)
                    elif 'state' in message:
                        singer.write_state(message['state'])
                    else:
                        raise TapFacebookException('Unrecognized message {}'.format(message))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream.name))
    field_class = stream.field_class
    schema = utils.load_json(path)
    for k in schema['properties']:
        if k in set(stream.key_properties) or k == UPDATED_TIME_KEY:
            schema['properties'][k]['inclusion'] = 'automatic'
        else:
            if k not in field_class.__dict__:
                LOGGER.warning(
                    'Property %s.%s is not defined in the facebook_business library',
                    stream.name, k)
            schema['properties'][k]['inclusion'] = 'available'

    return schema


def initialize_streams_for_discovery(): # pylint: disable=invalid-name
    return [initialize_stream(name, None, None, None, None)
            for name in STREAMS]

def discover_schemas():
    # Load Facebook's shared schemas
    refs = load_shared_schema_refs()

    result = {'streams': []}
    streams = initialize_streams_for_discovery()
    for stream in streams:
        LOGGER.info('Loading schema for %s', stream.name)
        schema = singer.resolve_schema_references(load_schema(stream), refs)
        result['streams'].append({'stream': stream.name,
                                  'tap_stream_id': stream.name,
                                  'schema': schema})
    return result

def load_shared_schema_refs():
    shared_schemas_path = get_abs_path('schemas/shared')

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs[shared_file] = json.load(data_file)

    return shared_schema_refs

def do_discover():
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    account_id = args.config['account_id']
    access_token = args.config['access_token']

    CONFIG.update(args.config)

    FacebookAdsApi.init(access_token=access_token)
    user = fb_user.User(fbid='me')
    accounts = user.get_ad_accounts()
    account = None
    for acc in accounts:
        if acc['account_id'] == account_id:
            account = acc
    if not account:
        raise TapFacebookException("Couldn't find account with id {}".format(account_id))

    if args.discover:
        do_discover()
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        do_sync(account, catalog, args.state)
    else:
        LOGGER.info("No properties were selected")

def main():

    try:
        main_impl()
    except TapFacebookException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.exception(e)
        for line in str(e).splitlines():
            LOGGER.critical(line)
        raise e
