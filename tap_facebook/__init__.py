#!/usr/bin/env python3

import datetime
import json
import os

import requests
import singer
from singer import utils

from facebookads import FacebookAdsApi
import facebookads.objects as objects
from facebookads.objects import (
    AdAccount,
    Campaign,
)



BASE_URL = "https://graph.facebook.com"
API_VERSION = "v2.8"
REQUIRED_CONFIG_KEYS = ["start_date", "account_id", "access_token"]

PAGE_SIZE = 100
ASYNC_SLEEP_SECONDS = 5 * 60
ASYNC_WAIT_SECONDS = 30 * 60

AUTH_ERROR_CODES = [100, 190]
RATE_LIMIT_CODES = [4, 17, 613, 1487225]

CONFIG = {}
STATE = {}
SCHEMAS = {}

LOGGER = singer.get_logger()
SESSION = requests.Session()

INSIGHTS_FIELD_LIST = [
    "account_id",
    "account_name",
    "action_values",
    "actions",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "app_store_clicks",
    "buying_type",
    "call_to_action_clicks",
    "campaign_id",
    "campaign_name",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "clicks",
    "cost_per_10_sec_video_view",
    "cost_per_action_type",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_total_action",
    "cost_per_unique_action_type",
    "cost_per_unique_click",
    "cost_per_unique_inline_link_click",
    "cpc",
    "cpm",
    "cpp",
    "ctr",
    "date_start",
    "date_stop",
    "deeplink_clicks",
    "frequency",
    "impressions",
    "inline_link_click_ctr",
    "inline_link_clicks",
    "inline_post_engagement",
    "newsfeed_avg_position",
    "newsfeed_clicks",
    "newsfeed_impressions",
    "objective",
    "place_page_name",
    "reach",
    "relevance_score",
    "social_clicks",
    "social_impressions",
    "social_reach",
    "social_spend",
    "spend",
    "total_action_value",
    "total_actions",
    "total_unique_actions",
    "unique_actions",
    "unique_clicks",
    "unique_ctr",
    "unique_impressions",
    "unique_inline_link_click_ctr",
    "unique_inline_link_clicks",
    "unique_link_clicks_ctr",
    "unique_social_clicks",
    "unique_social_impressions",
    "video_10_sec_watched_actions",
    "video_15_sec_watched_actions",
    "video_30_sec_watched_actions",
    "video_avg_pct_watched_actions",
    "video_avg_sec_watched_actions",
    "video_complete_watched_actions",
    "video_p100_watched_actions",
    "video_p25_watched_actions",
    "video_p50_watched_actions",
    "video_p75_watched_actions",
    "video_p95_watched_actions",
    "website_clicks",
    "website_ctr",
]

INSIGHTS_INT_FIELDS = [
    "app_store_clicks",
    "call_to_action_clicks",
    "clicks",
    "deeplink_clicks",
    "impressions",
    "inline_link_clicks",
    "inline_post_engagement",
    "newsfeed_clicks",
    "newsfeed_impressions",
    "reach",
    "social_clicks",
    "social_impressions",
    "social_reach",
    "total_actions",
    "total_unique_actions",
    "unique_clicks",
    "unique_impressions",
    "unique_inline_link_clicks",
    "unique_social_clicks",
    "unique_social_impressions",
    "website_clicks",
]


ENTITIES = {
    "ads_insights": {
        "fields": INSIGHTS_FIELD_LIST,
    },
    "ads_insights_country": {
        "fields": INSIGHTS_FIELD_LIST,
    },
    "ads_insights_age_and_gender": {
        "fields": INSIGHTS_FIELD_LIST,
    },
    "ads_insights_placement_and_device": {
        "fields": INSIGHTS_FIELD_LIST,
    },
}

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


campaign_schema = {
    'key_properties': ['id'],
    'stream' : 'junk',
    'schema': {}
}


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

def sync_ads_insights(schema):
    pass


def do_sync(account, selections):

    streams = [
        CampaignsStream(account, selections),
        AdSetsStream(account, selections),
        AdsStream(account, selections),
        AdCreativeStream(account, selections),
    ]
    
    sync_funcs = {
        # "adcreative": sync_adcreative,
        # "ads_insights": sync_ads_insights,
        # "ads_insights_country": sync_ads_insights_country,
        # "ads_insights_age_and_gender": sync_ads_insights_age_and_gender,
        # "ads_insights_placement_and_device": sync_ads_insights_placement_and_device,
    }

    for s in streams:
        s.sync()


def main():

    config, state, properties = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)

    # SCHEMAS.update(schemas)

    api = FacebookAdsApi.init(access_token=CONFIG['access_token'])
    user = objects.AdUser(fbid='me')
    accounts = user.get_ad_accounts()
    account = None
    for a in accounts:
        if a['account_id'] == CONFIG['account_id']:
            account = a
    if not account:
        raise Exception("Couldn't find account with id {}".format(CONFIG['account_id']))

    do_sync(account, properties)


if __name__ == '__main__':
    main()
