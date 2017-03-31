#!/usr/bin/env python3

import datetime
import json
import os
import sys
import time

import requests
import singer
from singer import utils

from facebookads import FacebookAdsApi
import facebookads.objects as objects

INSIGHTS_MAX_WAIT_TO_START_SECONDS = 5 * 60
INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS = 30 * 60

STREAMS = ['adcreative', 'ads', 'adsets', 'campaigns', 'adsinsights']

REQUIRED_CONFIG_KEYS = ["start_date", "account_id", "access_token"]

CONFIG = {}
STATE = {}

LOGGER = singer.get_logger()


def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def transform_field(value, field_type, field_format=None):
    if field_format == "date-time":
        # TODO
        return value

    if field_type == "boolean":
        return bool(value)

    if field_type == "integer":
        return int(value)

    if field_type == "number":
        return float(value)

    if field_type == "string":
        return value

    else:
        raise ValueError("Unsuppported type {}".format(field_type))


def transform_fields(row, schema):
    rtn = {}
    for field_name, field_schema in schema['schema']['properties'].items():
        if "type" not in field_schema:
            raise ValueError("Field {} schema missing type".format(field_name))

        field_types = field_schema["type"]
        if not isinstance(field_type, list):
            field_types = [field_types]

        if "null" in field_types:
            field_types.remove("null")
        else:
            if field_name not in row:
                raise ValueError("{} not in row and not null".format(field_name))

        errors = []
        for field_type in field_types:
            try:
                rtn[field_name] = transform_field(row[field_name], field_type, field_schema.get("format"))
                break
            except Exception as e:
                errors.append(e)
        else:
            err_msg = "\n\t".join(e.message for e in errors)
            raise ValueError("Field {} does not match schema {}\nErrors:\n\t{}"
                             .format(field_name, field_schema, err_msg))

    return rtn


class Stream(object):

    key_properties = ['id', 'date']
    
    def __init__(self, account=None, annotated_schema=None):
        self.account = account
        self.annotated_schema = annotated_schema
    
    def fields(self):
        if self.annotated_schema:
            props = self.annotated_schema['properties']
            return set([k for k in props if props[k].get('selected')])
        return set()


class AdCreative(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    '''
    
    name = 'adcreative'
    field_class = objects.adcreative.AdCreative.Field 
    key_properties = ['id']
   
    def __iter__(self):
        ad_creative = self.account.get_ad_creatives()

        LOGGER.info('Getting adcreative fields {}'.format(self.fields()))
        
        for a in ad_creative:                    
            a.remote_read(fields=self.fields())
            yield a.export_all_data()

    
class Ads(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
    '''
    name = 'ads'
    field_class = objects.ad.Ad.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        ads = self.account.get_ads()
        for a in ads:
            a.remote_read(fields=self.fields())
            yield a.export_all_data()


class AdSets(Stream):
    name = 'adsets'
    field_class = objects.adset.AdSet.Field
    key_properties = ['id', 'updated_time']

    def __iter__(self):
        ad_sets = self.account.get_ad_sets()
        for a in ad_sets:
            a.remote_read(fields=self.fields())
            yield a.export_all_data()


class Campaigns(Stream):
    name = 'campaigns'
    field_class = objects.campaign.Campaign.Field
    key_properties = ['id']
    
    def __iter__(self):

        campaigns = self.account.get_campaigns()
        props = self.fields()
        fields = [k for k in props if k != 'ads']
        pull_ads = 'ads' in props

        for c in campaigns:
            c.remote_read(fields=fields)
            c_out = {'ads': {'data': []}}
            for k in fields:
                c_out[k] = c[k]

            if pull_ads:
                for ad in c.get_ads():
                    c_out['ads']['data'].append({'id': ad['id']})

            yield c_out


class AdsInsights(Stream):
    name = 'adsinsights'
    field_class = objects.adsinsights.AdsInsights.Field
    key_properties = ['id', 'updated_time']    
    limit = 100
    time_increment = 1

    def __init__(self,
                 account=None,
                 annotated_schema=None,
                 breakdowns=None,
                 action_breakdowns=None,
                 level=None,
                 action_attribution_windows=None):
        self.breakdowns = breakdowns
        self.action_breakdowns = action_breakdowns
        self.level = level
        self.action_attribution_windows = action_attribution_windows
        self.annotated_schema = annotated_schema
        self.account = account

    def __iter__(self):
        fields = list(self.fields())
        params={
            'level': self.level,
            'action_breakdowns': self.action_breakdowns,
            'breakdowns': self.breakdowns,
            'limit': 100,
            'fields': fields,
            'time_increment': 1,
            'action_attribution_windows': self.action_attribution_windows,
            'time_ranges': [{'since':'2017-02-15', 'until':'2017-02-16'}]
        }
        LOGGER.info('Starting adsinsights job with params {}'.format(params))
        i_async_job = self.account.get_insights(params=params, async=True)

        status = None
        time_start = time.time()
        while status != "Job Completed":
            duration = time.time() - time_start
            job = i_async_job.remote_read()
            status = job[objects.AsyncJob.Field.async_status]
            percent_complete = job[objects.AsyncJob.Field.async_percent_completion]
            job_id = job[objects.AsyncJob.Field.id]
            LOGGER.info('Job status: {}; {}% done'
                        .format(status,
                                percent_complete))

            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                raise Exception('Insights job {} did not start after {} seconds'.format(job_id, INSIGHTS_MAX_WAIT_TO_START_SECONDS))

            elif duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS and status != "Job Completed":
                raise Exception('Insights job {} did not complete after {} seconds'.format(job_id, INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS))
            time.sleep(5)

        LOGGER.info('results are {}'.format(type(i_async_job.get_result())))
        for o in i_async_job.get_result():        
            yield o.export_all_data()


def initialize_stream(stream_name, account, config, annotated_schema):
    if stream_name == 'adsinsights':
        
        return AdsInsights(
            account, annotated_schema,
            breakdowns=config['insights_tables'][0]['breakdowns'],
            action_breakdowns=config['insights_tables'][0]['action_breakdowns'],
            level=config['insights_tables'][0]['level'],
            action_attribution_windows=config['insights_tables'][0]['action_attribution_windows'])
    elif stream_name == 'campaigns':
        return Campaigns(account, annotated_schema)
    elif stream_name == 'adsets':
        return AdSets(account, annotated_schema)
    elif stream_name == 'ads':
        return Ads(account, annotated_schema)
    elif stream_name == 'adcreative':
        return AdCreative(account, annotated_schema)


def do_sync(account, config, annotated_schemas):
    streams = []
    for stream_name in STREAMS:
        annotated_schema = {}
        if stream_name in annotated_schemas['streams']:
            annotated_schema = annotated_schemas['streams'][stream_name]
            if annotated_schema.get('selected'):
                streams.append(initialize_stream(stream_name, account, config, annotated_schema))
            
    for s in streams:
        LOGGER.info('Syncing {}'.format(s.name))
        schema = load_schema(s)
        singer.write_schema(s.name, schema, s.key_properties)

        num_records = 0
        for record in s:
            num_records += 1
            singer.write_record(s.name, record)
            if num_records % 1000 == 0:
                LOGGER.info('Got {} {} records'.format(num_records, s))

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)    

def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream.name))
    field_class = stream.field_class
    schema = utils.load_json(path)
    for k in schema['properties']:
        if k in field_class.__dict__:
            schema['properties'][k]['inclusion'] = 'available'
    return schema


def do_discover():
    LOGGER.info('Loading schemas')
    result = {'streams': {}}
    for stream in [Ads(),
                   AdSets(),
                   Campaigns(),
                   AdCreative(),
                   AdsInsights()]:
        LOGGER.info('Loading schema for {}'.format(stream.name))
        result['streams'][stream.name] = load_schema(stream)
    json.dump(result, sys.stdout, indent=4)

    
def main():
    # singer.write_record('adsinsights', {'a': 1})
    # return
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    if args.state:
        STATE.update(args.state)

    api = FacebookAdsApi.init(access_token=CONFIG['access_token'])
    user = objects.AdUser(fbid='me')
    accounts = user.get_ad_accounts()
    account = None
    for a in accounts:
        if a['account_id'] == CONFIG['account_id']:
            account = a
    if not account:
        raise Exception("Couldn't find account with id {}".format(CONFIG['account_id']))

    if args.discover:
        do_discover()
    elif args.properties:
        do_sync(account, CONFIG, args.properties)
    else:
        LOGGER.info("No properties were selected")


if __name__ == '__main__':
    main()
