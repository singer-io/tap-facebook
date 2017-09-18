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

from facebookads import FacebookAdsApi
import facebookads.adobjects.adcreative as adcreative
import facebookads.adobjects.ad as fb_ad
import facebookads.adobjects.adset as adset
import facebookads.adobjects.campaign as fb_campaign
import facebookads.adobjects.adsinsights as adsinsights
import facebookads.adobjects.user as fb_user

TODAY = pendulum.today()

INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
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
    'ads_insights_platform_and_device']


REQUIRED_CONFIG_KEYS = ['start_date', 'account_id', 'access_token']
LOGGER = singer.get_logger()

CONFIG = {}

def transform_datetime_string(dts):
    parsed_dt = dateutil.parser.parse(dts)
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    else:
        parsed_dt = parsed_dt.astimezone(timezone.utc)
    return singer.strftime(parsed_dt)

@attr.s
class Stream(object):

    name = attr.ib()
    account = attr.ib()
    stream_alias = attr.ib()
    annotated_schema = attr.ib()

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


class AdCreative(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    '''

    field_class = adcreative.AdCreative.Field
    key_properties = ['id']

    def __iter__(self):
        ad_creative = self.account.get_ad_creatives(fields=self.fields(), # pylint: disable=no-member
                                                    params={'limit': RESULT_RETURN_LIMIT})
        for a in ad_creative: # pylint: disable=invalid-name
            yield {'record': a.export_all_data()}


class Ads(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
    '''
    field_class = fb_ad.Ad.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        ads = self.account.get_ads(fields=self.fields(), params={'limit': RESULT_RETURN_LIMIT}) # pylint: disable=no-member
        for ad in ads: # pylint: disable=invalid-name
            yield {'record': ad.export_all_data()}


class AdSets(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign
    '''
    field_class = adset.AdSet.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        ad_sets = self.account.get_ad_sets(fields=self.fields(), # pylint: disable=no-member
                                           params={'limit': RESULT_RETURN_LIMIT})
        for ad_set in ad_sets:
            yield {'record': ad_set.export_all_data()}


class Campaigns(Stream):
    field_class = fb_campaign.Campaign.Field
    key_properties = ['id']

    def __iter__(self):
        props = self.fields()
        fields = [k for k in props if k != 'ads']
        pull_ads = 'ads' in props
        campaigns = self.account.get_campaigns(fields=fields, params={'limit': RESULT_RETURN_LIMIT}) # pylint: disable=no-member
        for campaign in campaigns:
            campaign_out = {}
            for k in campaign:
                campaign_out[k] = campaign[k]

            if pull_ads:
                campaign_out['ads'] = {'data': []}
                ids = [ad['id'] for ad in campaign.get_ads()]
                for ad_id in ids:
                    campaign_out['ads']['data'].append({'id': ad_id})

            yield {'record': campaign_out}


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

def get_start(state, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
    LOGGER.info("found current bookmark %s", current_bookmark)
    if current_bookmark is None:
        LOGGER.info("using start_date instead...%s", CONFIG['start_date'])
        return CONFIG['start_date']
    return current_bookmark

def advance_bookmark(state, tap_stream_id, bookmark_key, date):
    LOGGER.info('advance(%s, %s)', tap_stream_id, date)
    date = pendulum.parse(date) if date else None
    current_bookmark = pendulum.parse(get_start(state, tap_stream_id, bookmark_key))

    if date is None:
        LOGGER.info('Did not get a date for stream %s '+
                    ' not advancing bookmark',
                    tap_stream_id)
    elif date > current_bookmark:
        LOGGER.info('Bookmark for stream %s is currently %s, ' +
                    'advancing to %s',
                    tap_stream_id, current_bookmark, date)
        state = singer.write_bookmark(state, tap_stream_id, bookmark_key, date.to_date_string())
    else:
        LOGGER.info('Bookmark for stream %s is currently %s ' +
                    'not changing to to %s',
                    tap_stream_id, current_bookmark, date)
    return state

class InsightsJobTimeout(Exception):
    pass

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

    bookmark_key = "date_start"

    invalid_insights_fields = ['impression_device', 'publisher_platform', 'platform_position',
                               'age', 'gender', 'country', 'placement']

    # pylint: disable=no-member,unsubscriptable-object,attribute-defined-outside-init
    def __attrs_post_init__(self):
        self.breakdowns = self.options.get('breakdowns') or []
        self.key_properties = self.base_properties[:]
        if self.options.get('primary-keys'):
            self.key_properties.extend(self.options['primary-keys'])

    @backoff.on_exception(
        backoff.expo,
        (InsightsJobTimeout),
        max_tries=3,
        factor=2)
    def job_params(self):
        start_date = pendulum.parse(get_start(self.state, self.name, self.bookmark_key))

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


    def run_job(self, params):
        LOGGER.info('Starting adsinsights job with params %s', params)
        job = self.account.get_insights( # pylint: disable=no-member
            params=params,
            async=True)
        status = None
        time_start = time.time()
        sleep_time = 10
        while status != "Job Completed":
            duration = time.time() - time_start
            job = job.remote_read()
            status = job[adsinsights.AdsInsights.Summary.async_status]
            percent_complete = job[adsinsights.AdsInsights.Summary.
                                   async_percent_completion]
            job_id = job[adsinsights.AdsInsights.Summary.id]
            LOGGER.info('%s, %d%% done', status, percent_complete)

            if status == "Job Completed":
                return job

            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                raise Exception(
                    'Insights job {} did not start after {} seconds'.format(
                        job_id, INSIGHTS_MAX_WAIT_TO_START_SECONDS))
            elif duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS and status != "Job Completed":
                pretty_error_message = ('Insights job {} did not complete after {} minutes. ' +
                    'This is an intermittent error and may resolve itself on subsequent queries to the Facebook API. ' +
                    'You should deselect fields from the schema that are not necessary, ' +
                    'as that may help improve the reliability of the Facebook API.')
                raise Exception(pretty_error_message.format(job_id, INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS//60))
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
            yield {'state': advance_bookmark(self.state, self.name,
                                             self.bookmark_key, min_date_start_for_job)} # pylint: disable=no-member


INSIGHTS_BREAKDOWNS_OPTIONS = {
    'ads_insights': {"breakdowns": []},
    'ads_insights_age_and_gender': {"breakdowns": ['age', 'gender'],
                                    "primary-keys": ['age', 'gender']},
    'ads_insights_country': {"breakdowns": ['country']},
    'ads_insights_platform_and_device': {"breakdowns": ['publisher_platform',
                                                        'platform_position', 'impression_device'],
                                         "primary-keys": ['publisher_platform',
                                                          'platform_position', 'impression_device']}
}


def initialize_stream(name, account, stream_alias, annotated_schema, state): # pylint: disable=too-many-return-statements

    if name in INSIGHTS_BREAKDOWNS_OPTIONS:
        return AdsInsights(name, account, stream_alias, annotated_schema,
                           state=state,
                           options=INSIGHTS_BREAKDOWNS_OPTIONS[name])
    elif name == 'campaigns':
        return Campaigns(name, account, stream_alias, annotated_schema)
    elif name == 'adsets':
        return AdSets(name, account, stream_alias, annotated_schema)
    elif name == 'ads':
        return Ads(name, account, stream_alias, annotated_schema)
    elif name == 'adcreative':
        return AdCreative(name, account, stream_alias, annotated_schema)
    else:
        raise Exception('Unknown stream {}'.format(name))


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
        singer.write_schema(stream.name, schema, stream.key_properties, stream.stream_alias)

        with Transformer(pre_hook=transform_date_hook) as transformer:
            with metrics.record_counter(stream.name) as counter:
                for message in stream:
                    if 'record' in message:
                        counter.increment()
                        record = transformer.transform(message['record'], schema)
                        singer.write_record(stream.name, record, stream.stream_alias)
                    elif 'state' in message:
                        singer.write_state(message['state'])
                    else:
                        raise Exception('Unrecognized message {}'.format(message))


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream.name))
    field_class = stream.field_class
    schema = utils.load_json(path)
    for k in schema['properties']:
        if k in set(stream.key_properties):
            schema['properties'][k]['inclusion'] = 'automatic'
        elif k in field_class.__dict__:
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


def main():
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
        raise Exception("Couldn't find account with id {}".format(account_id))

    if args.discover:
        do_discover()
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        do_sync(account, catalog, args.state)
    else:
        LOGGER.info("No properties were selected")
