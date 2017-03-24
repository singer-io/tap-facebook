
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

STREAMS = ['adcreative', 'ads', 'adsets', 'campaigns', 'insights']

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

    def __init__(self, account, selections):
        self.account = account
        self.selections = selections
    
    def is_selected(self):
        return self.name in self.selections


    def sync(self):
        if self.is_selected():
            LOGGER.info('Syncing {}'.format(self.name))
            self.sync_impl()
        else:
            LOGGER.info('Skipping {}'.format(self.name))


class CampaignsStream(Stream):
    name = 'campaigns'

    def sync_impl(self):
    
        campaigns = self.account.get_campaigns()
        props = self.selections[self.name]['properties']
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

            singer.write_record(self.name, c_out)


class AdSetsStream(Stream):
    name = 'adsets'
    
    def sync_impl(self):
        ad_sets = self.account.get_ad_sets()
        for a in ad_sets:
            fields = self.selections[self.name]['properties'].keys()
            a.remote_read(fields=fields)
            singer.write_record(self.name, a.export_all_data())

class AdsStream(Stream):

    name = 'ads'
            
    def sync_impl(self):
        #doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
        ads = self.account.get_ads()

        for a in ads:
            fields = self.selections[self.name]['properties'].keys()
            a.remote_read(fields=fields)
            singer.write_record(self.name, a.export_all_data())

class AdCreativeStream(Stream):
    name = 'adcreative'
    
    def sync_impl(self):
        #doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
        ad_creative = self.account.get_ad_creatives()
        fields = self.selections[self.name]['properties'].keys()

        for a in ad_creative:
            a.remote_read(fields=fields)
            singer.write_record(self.name, a.export_all_data())

class AdsInsights(Stream):
    name = 'ads_insights'
    action_breakdowns = [] # ["action_type",
                         # "action_target_id",
                         # "action_destination"]
    limit = 100
    time_increment = 1
    action_attribution_windows = [] #["1d_click",
                                    #"7d_click",
                                    #               "28d_click",
                                    #               "1d_view",
                                    #               "7d_view",
                                    #               "28d_view"]
    
    def sync_impl(self):
        fields = list(self.selections[self.name]['properties'].keys())
        LOGGER.info("fields are: {}".format(fields))
        params={
            'level': 'ad',
            'action_breakdowns': self.action_breakdowns,
            'limit': 100,
            'fields': fields,
            'time_increment': 1,
            'action_attribution_windows': self.action_attribution_windows,
            'time_ranges': [{'since':'2017-02-15', 'until':'2017-03-01'}]
        }      
        i_async_job = self.account.get_insights(params=params, \
                                                async=True)
        
        # Insights
        while True:
            job = i_async_job.remote_read()
            LOGGER.info('Job status: {}; {}% done'
                        .format(job[objects.AsyncJob.Field.async_status],
                                job[objects.AsyncJob.Field.async_percent_completion]))
            time.sleep(5)
            if job[objects.AsyncJob.Field.async_status] == "Job Completed":
                LOGGER.info("Done!")
                break

        LOGGER.info('results are {}'.format(type(i_async_job.get_result())))
        for o in i_async_job.get_result():        
            singer.write_record(self.name, o.export_all_data())

def do_sync(account, selections):

    streams = [
        AdsInsights(account, selections),        
        CampaignsStream(account, selections),
        AdSetsStream(account, selections),
        AdsStream(account, selections),
        AdCreativeStream(account, selections),
    ]
    
    for s in streams:
        s.sync()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)    

def load_schema(stream):
    path = get_abs_path('schemas/{}.json'.format(stream))
    return utils.load_json(path)


def do_discover():
    res = {s: load_schema(s) for s in STREAMS}
    json.dump(res, sys.stdout, indent=4)

    
def main():

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
    else:
        do_sync(account, args.properties)


if __name__ == '__main__':
    main()
