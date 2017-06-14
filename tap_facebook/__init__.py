#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time

import attr
import pendulum
import requests
import backoff

import singer
import singer.metrics as metrics
from singer import utils

from facebookads import FacebookAdsApi
import facebookads.objects as objects

TODAY = pendulum.today()

INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60

STREAMS = set([
    'adcreative',
    'ads',
    'adsets',
    'campaigns',
    'ads_insights',
    'ads_insights_age_and_gender',
    'ads_insights_country',
    'ads_insights_placement_and_device'])

REQUIRED_CONFIG_KEYS = ['start_date', 'account_id', 'access_token']
LOGGER = singer.get_logger()


@attr.s
class Stream(object):

    name = attr.ib()
    account = attr.ib()
    annotated_schema = attr.ib()

    def fields(self):
        fields = set()
        if self.annotated_schema:
            props = self.annotated_schema['properties'] # pylint: disable=unsubscriptable-object
            for k, val in props.items():
                inclusion = val.get('inclusion')
                selected = val.get('selected')
                if selected or inclusion == 'automatic':
                    fields.add(k)
        return fields


class AdCreative(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    '''

    field_class = objects.adcreative.AdCreative.Field
    key_properties = ['id']

    def __iter__(self):
        ad_creative = self.account.get_ad_creatives() # pylint: disable=no-member
        for a in ad_creative: # pylint: disable=invalid-name
            a.remote_read(fields=self.fields())
            yield {'record': a.export_all_data()}


class Ads(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
    '''
    field_class = objects.ad.Ad.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        ads = self.account.get_ads() # pylint: disable=no-member
        for ad in ads: # pylint: disable=invalid-name
            ad.remote_read(fields=self.fields())
            yield {'record': ad.export_all_data()}


class AdSets(Stream):
    field_class = objects.adset.AdSet.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        ad_sets = self.account.get_ad_sets() # pylint: disable=no-member
        for ad_set in ad_sets:
            ad_set.remote_read(fields=self.fields())
            yield {'record': ad_set.export_all_data()}


class Campaigns(Stream):
    field_class = objects.campaign.Campaign.Field
    key_properties = ['id']

    def __iter__(self):
        campaigns = self.account.get_campaigns() # pylint: disable=no-member
        props = self.fields()
        fields = [k for k in props if k != 'ads']
        pull_ads = 'ads' in props
        for campaign in campaigns:
            campaign.remote_read(fields=fields)
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


class State(object):
    def __init__(self, start_date, state):
        self.start_date = pendulum.parse(start_date)
        if state is None:
            self.state = {}
        else:
            self.state = {k: pendulum.parse(v) for k, v in state.items()}

    def _get(self, stream_name):
        if stream_name in self.state:
            return self.state[stream_name]
        return self.start_date

    def get(self, stream_name):
        return self._get(stream_name).to_date_string()

    def advance(self, stream_name, date):
        LOGGER.info('advance(%s, %s)', stream_name, date)
        date = pendulum.parse(date) if date else None
        old_date = self._get(stream_name)

        if date is None:
            LOGGER.info('Did not get a date for stream %s '+
                        ' not advancing bookmark',
                        stream_name)
        elif date > old_date:
            LOGGER.info('Bookmark for stream %s is currently %s, ' +
                        'advancing to %s',
                        stream_name, old_date, date)
            self.state[stream_name] = date
        else:
            LOGGER.info('Bookmark for stream %s is currently %s ' +
                        'not changing to to %s',
                        stream_name, old_date, date)
        return {k: v.to_date_string() for k, v in self.state.items()}

class InsightsJobTimeout(Exception):
    pass

@attr.s
class AdsInsights(Stream):
    field_class = objects.adsinsights.AdsInsights.Field
    key_properties = ['campaign_id', 'adset_id', 'ad_id', 'date_start']

    state = attr.ib()
    breakdowns = attr.ib()
    action_breakdowns = attr.ib(default=ALL_ACTION_BREAKDOWNS)
    level = attr.ib(default='ad')
    action_attribution_windows = attr.ib(
        default=ALL_ACTION_ATTRIBUTION_WINDOWS)
    time_increment = attr.ib(default=1)
    limit = attr.ib(default=100)

    @backoff.on_exception(
        backoff.expo,
        (InsightsJobTimeout),
        max_tries=3,
        factor=2)
    def job_params(self):
        until = pendulum.parse(self.state.get(self.name)) # pylint: disable=no-member
        since = until.subtract(days=28)
        while until <= pendulum.now():
            yield {
                'level': self.level,
                'action_breakdowns': list(self.action_breakdowns),
                'breakdowns': list(self.breakdowns),
                'limit': self.limit,
                'fields': list(self.fields()),
                'time_increment': self.time_increment,
                'action_attribution_windows': list(self.action_attribution_windows),
                'time_ranges': [{'since': since.to_date_string(),
                                 'until': until.to_date_string()}]
            }
            since = since.add(days=1)
            until = until.add(days=1)


    def run_job(self, params):
        LOGGER.info('Starting adsinsights job with params %s', params)
        job = self.account.get_insights( # pylint: disable=no-member
            params=params,
            async=True)
        status = None
        time_start = time.time()
        while status != "Job Completed":
            duration = time.time() - time_start
            job = job.remote_read()
            status = job[objects.AsyncJob.Field.async_status]
            percent_complete = job[objects.AsyncJob.Field.async_percent_completion]
            job_id = job[objects.AsyncJob.Field.id]
            LOGGER.info('%s, %d%% done', status, percent_complete)

            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                raise Exception(
                    'Insights job {} did not start after {} seconds'.format(
                        job_id, INSIGHTS_MAX_WAIT_TO_START_SECONDS))

            elif duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS and status != "Job Completed":
                raise Exception(
                    'Insights job {} did not complete after {} seconds'.format(
                        job_id, INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS))
            time.sleep(5)
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
                if not min_date_start_for_job or rec['date_start'] < min_date_start_for_job:
                    min_date_start_for_job = rec['date_start']
                yield {'record': rec}
            LOGGER.info('Got %d results for insights job', count)
            yield {'state': self.state.advance(self.name, min_date_start_for_job)} # pylint: disable=no-member


INSIGHTS_BREAKDOWNS = {
    'ads_insights': [],
    'ads_insights_age_and_gender': ['age', 'gender'],
    'ads_insights_country': ['country'],
    'ads_insights_placement_and_device': ['placement', 'impression_device'],
}


def initialize_stream(name, account, annotated_schema, state): # pylint: disable=too-many-return-statements

    if name in INSIGHTS_BREAKDOWNS:
        return AdsInsights(name, account, annotated_schema,
                           state=state,
                           breakdowns=INSIGHTS_BREAKDOWNS[name])
    elif name == 'campaigns':
        return Campaigns(name, account, annotated_schema)
    elif name == 'adsets':
        return AdSets(name, account, annotated_schema)
    elif name == 'ads':
        return Ads(name, account, annotated_schema)
    elif name == 'adcreative':
        return AdCreative(name, account, annotated_schema)
    else:
        raise Exception('Unknown stream {}'.format(name))


def get_streams_to_sync(account, annotated_schemas, state):
    streams = []
    for stream in annotated_schemas['streams']:
        schema = stream.get('schema')
        name = stream.get('stream')
        if schema.get('selected'):
            streams.append(initialize_stream(name, account, schema, state))
    return streams


def do_sync(account, annotated_schemas, state):

    for stream in get_streams_to_sync(account, annotated_schemas, state):
        LOGGER.info('Syncing %s, fields %s', stream.name, stream.fields())
        schema = load_schema(stream)
        singer.write_schema(stream.name, schema, stream.key_properties)

        with metrics.record_counter(stream.name) as counter:
            for message in stream:
                if 'record' in message:
                    counter.increment()
                    record = singer.transform.transform(message['record'], schema)
                    singer.write_record(stream.name, record)
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
    return [initialize_stream(name, None, None, None)
            for name in STREAMS]

def discover_schemas():
    result = {'streams': []}
    streams = initialize_streams_for_discovery()
    for stream in streams:
        LOGGER.info('Loading schema for %s', stream.name)
        result['streams'].append({'stream': stream.name,
                                  'tap_stream_id': stream.name,
                                  'schema': load_schema(stream)})
    return result

def do_discover():
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)


def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    start_date = args.config['start_date']
    account_id = args.config['account_id']
    access_token = args.config['access_token']
    state = State(start_date, args.state)

    FacebookAdsApi.init(access_token=access_token)
    user = objects.AdUser(fbid='me')
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
        do_sync(account, args.properties, state)
    else:
        LOGGER.info("No properties were selected")
