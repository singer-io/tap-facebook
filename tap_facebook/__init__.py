#!/usr/bin/env python3

import datetime
import json
import os

import requests
import singer

from facebookads import FacebookAdsApi
import facebookads.objects as objects
from facebookads.objects import (
    AdAccount,
    Campaign,
)

from tap_facebook import utils

api = None
account = None

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
    "campaigns": {
        "fields": ["id", "account_id", "name", "objective", "ads", "effective_status", "buying_type"],
    },
    "adsets": {
        "fields": ["id", "name", "account_id", "bid_info", "campaign_id", "effective_status", "start_time",
                   "end_time", "updated_time", "created_time", "daily_budget", "lifetime_budget",
                   "budget_remaining", "targeting", "promoted_object"],
    },
    "ads": {
        "fields": ["id", "account_id", "effective_status", "bid_type", "bid_info",
                   "campaign_id", "adset_id", "conversion_specs", "created_time", "creative", "name",
                   "targeting", "updated_time"],
    },
    "adcreative": {
        "fields": ["id", "body", "image_hash", "image_url", "name", "object_id", "object_story_id", "object_story_spec", "title", "url_tags"],
    },
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


def get_url(endpoint):
    return "{}/{}/{}".format(BASE_URL, API_VERSION, endpoint)


def request(url, params=None):
    params = params or {}
    params["access_token"] = CONFIG['access_token']
    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('get', url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()
    return resp


def paged_request(url, params=None):
    print("In paged_request, url is {}, params are {}".format(url, params))
    while True:
        data = request(url, params).json()
        for row in data.get("data"):
            yield row

        if "paging" in data and "next" in data['paging']:
            url = data['paging']['next']
        else:
            break


"""
Example Schema:
{
  "stream": "orders",
  "location": {
    "schema": "public",
    "table": "orders"
  },
  "key_properties": [
     "id"
  ],
  "schema": {
    "type": "object",
    "properties": {
      "id": {
        "type": "integer"
      },
      "user_id": {
        "type": "integer"
      },
      "amount": {
        "type": "number"
      },
    }
  }
}
"""

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


def sync_campaigns(schema):
    campaigns = account.get_campaigns()

    for c in campaigns:
        fields = ["id", "account_id", "name", "objective", "effective_status", "buying_type"]
        c.remote_read(fields=fields)
        c_out = {'ads': {'data': []}}
        for k in fields:
            c_out[k] = c[k]

        for ad in c.get_ads():
            c_out['ads']['data'].append({'id': ad['id']})
 
        singer.write_record('campaigns', c_out)

def sync_adsets(schema):
    ad_sets = account.get_ad_sets()

    #TODO check on publisher_platforms and device_platforms sub-tables
    for a in ad_sets:
        fields = ENTITIES['adsets']['fields']
        a.remote_read(fields=fields)

        singer.write_record('ad_sets', a.export_all_data())

def sync_ads(schema):
    #doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
    ads = account.get_ads()

    #TODO check that adgroup_review_feedback and targeting_specs is ok to delete
    for a in ads:
        fields = ENTITIES['ads']['fields']
        a.remote_read(fields=fields)
        
        singer.write_record('ads', a.export_all_data())

def sync_adcreative(schema):
    #doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    ad_creative = account.get_ad_creatives()

    #TODO follow_redirect, image_crops, image_file,
    for a in ad_creative:
        fields = ENTITIES['adcreative']['fields']
        a.remote_read(fields=fields)
 
        singer.write_record('adcreative', a.export_all_data())

def sync_ads_insights(schema):
    pass


def do_sync():
    sync_funcs = {
        "campaigns": sync_campaigns,
        # "adsets": sync_adsets,
        # "ads": sync_ads,
        # "adcreative": sync_adcreative,
        # "ads_insights": sync_ads_insights,
        # "ads_insights_country": sync_ads_insights_country,
        # "ads_insights_age_and_gender": sync_ads_insights_age_and_gender,
        # "ads_insights_placement_and_device": sync_ads_insights_placement_and_device,
    }

    for k in sync_funcs:
        sync_funcs[k](campaign_schema)


def main():
    global api
    global account
    config, state, schemas = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    SCHEMAS.update(schemas)

    api = FacebookAdsApi.init(access_token=CONFIG['access_token'])
    #account = AdAccount(account_id=CONFIG['account_id'])
    user = objects.AdUser(fbid='me')
    
    accounts = user.get_ad_accounts()
    account = None
    for a in accounts:
        if a['account_id'] == CONFIG['account_id']:
            account = a
    if not account:
        raise("Couldn't find account with id {}".format(CONFIG['account_id']))

    do_sync()


if __name__ == '__main__':
    main()
