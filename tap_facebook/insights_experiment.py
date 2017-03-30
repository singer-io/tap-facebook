#!/usr/bin/env python3

import random
import singer
from singer import utils
import tap_facebook
import tempfile


ALL_FIELDS = [
    "account_id",
    "account_name",
    "action_values",
    "actions",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "app_store_clicks",
    "call_to_action_clicks",
    "campaign_id",
    "campaign_name",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "clicks",
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
    "website_clicks",
    "website_ctr",
]

NO_ACTIONS = [
    "account_id",
    "account_name",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "app_store_clicks",
    "call_to_action_clicks",
    "campaign_id",
    "campaign_name",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "clicks",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_total_action",
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
    "unique_clicks",
    "unique_ctr",
    "unique_impressions",
    "unique_inline_link_click_ctr",
    "unique_inline_link_clicks",
    "unique_link_clicks_ctr",
    "unique_social_clicks",
    "unique_social_impressions",
    "website_clicks",
]

COMMON_FIELDS = [
    "account_id",
    "account_name",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "call_to_action_clicks",    
    "campaign_id",
    "campaign_name",
    "canvas_avg_view_percent",
    "canvas_avg_view_time",
    "clicks",
    "cost_per_inline_link_click",
    "cost_per_inline_post_engagement",
    "cost_per_total_action",
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
    "objective",
    "reach",
    "social_clicks",
    "social_impressions",
    "social_reach",
    "spend",
    "total_action_value",
    "total_actions",
    "unique_clicks",
    "website_clicks",
]

def random_subset(values):
    res = []
    for value in values:
        if random.random() > 0.5:
            res.append(value)
    return res
        
def gen_level():
    # return random.choice(['ad', 'campaign'])
    return 'ad'

def gen_action_breakdowns():
    # default is action_type
    return random_subset([
        'action_type',
        'action_target_id',
        'action_destination'])

def gen_breakdowns():
    return random.choice([None,
                          'age_and_gender',
                          'country',
                          'placement_and_device'])

def gen_action_attribution_windows():
    # default is 1d_view, 28d_click
    # None if no action breakdown
    return random_subset([
        '1d_click',
        '7d_click',
        '28d_click',
        '1d_view',
        '7d_view',
        '28d_view'])

def main():
    args = utils.parse_args(tap_facebook.REQUIRED_CONFIG_KEYS)


    
    with tempfile.TemporaryDirectory(prefix='insights-experiment-') as config_dir:
        props_path = os.path.join(config_dir, 'properties.json')
        config_path = os.path.join(config_dir, 'config.json')
        with open(props_path) as fp:
            json.dump(args.properties, fp)
        while True:
            level = gen_level()
            breakdowns = gen_breakdowns()
            action_breakdowns = gen_action_breakdowns()
            if len(action_breakdowns) == 0:
                action_attribution_windows = []
            else:
                action_attribution_windows = gen_action_attribution_windows()
            table = {
                'level': level,
                'action_breakdowns': action_breakdowns,
                'breakdowns': breakdowns,
                'action_attribution_windows': action_attribution_windows
            }
            with open(config_path) as fp:
                config = copy.deep_copy(config)
                config['insight_tables'] = [table]
                json.dump(config, fp)
                
        
        print('{}'.format(table))
        

if __name__ == '__main__':
    main()
