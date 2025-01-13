"""
Test that with no fields selected for a stream all fields are still replicated
"""

from tap_tester import LOGGER
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base_new_frmwrk import FacebookBaseTest
import base


class FacebookAllFieldsTest(AllFieldsTest, FacebookBaseTest):
    """Test that with no fields selected for a stream all fields are still replicated"""

    is_done = None

    # https://jira.talendforge.org/browse/TDL-24424
    MISSING_FIELDS = {
        "ads_insights" : {
            'outbound_clicks',
            'conversions',
            'cost_per_action_type',
            'video_p100_watched_actions',
            'video_p75_watched_actions',
            'action_values',
            'video_30_sec_watched_actions',
            'canvas_avg_view_percent',
            'video_p50_watched_actions',
            'video_p25_watched_actions',
            'conversion_values',
            'unique_outbound_clicks',
            'canvas_avg_view_time',
            'cost_per_unique_action_type'
        },
        "campaigns" : {
            'adlabels'
        },
        "adsets" : {
            'adlabels',
            },
        "adcreative" : {
            'image_crops',
            'product_set_id',
            'url_tags',
            'applink_treatment',
            'object_id',
            'link_og_id',
            'template_url',
            'template_url_spec',
            'object_url',
            'link_url',
            'adlabels',
            'instagram_story_id'
        },
        "ads_insights_country": {
            'video_p75_watched_actions',
            'conversions',
            'conversion_values',
            'canvas_avg_view_percent',
            'action_values',
            'unique_outbound_clicks',
            'cost_per_unique_action_type',
            'outbound_clicks',
            'social_spend',
            'video_p50_watched_actions',
            'engagement_rate_ranking',
            'video_p25_watched_actions',
            'quality_ranking',
            'video_play_curve_actions',
            'video_30_sec_watched_actions',
            'canvas_avg_view_time',
            'cost_per_action_type',
            'video_p100_watched_actions',
            'conversion_rate_ranking'
        },
        "ads_insights_age_and_gender": {
            'video_p75_watched_actions',
            'conversions',
            'conversion_values',
            'canvas_avg_view_percent',
            'action_values',
            'unique_outbound_clicks',
            'cost_per_unique_action_type',
            'outbound_clicks',
            'social_spend',
            'video_p50_watched_actions',
            'engagement_rate_ranking',
            'video_p25_watched_actions',
            'quality_ranking',
            'video_play_curve_actions',
            'video_30_sec_watched_actions',
            'canvas_avg_view_time',
            'cost_per_action_type',
            'video_p100_watched_actions',
            'conversion_rate_ranking'
        },
        "ads_insights_dma": {
            'video_p75_watched_actions',
            'conversions',
            'cost_per_unique_click',
            'inline_link_click_ctr',
            'conversion_values',
            'canvas_avg_view_percent',
            'action_values',
            'unique_ctr',
            'unique_outbound_clicks',
            'unique_inline_link_clicks',
            'cost_per_unique_action_type',
            'outbound_clicks',
            'social_spend',
            'cost_per_unique_inline_link_click',
            'unique_link_clicks_ctr',
            'video_p50_watched_actions',
            'engagement_rate_ranking',
            'unique_inline_link_click_ctr',
            'video_p25_watched_actions',
            'quality_ranking',
            'cpp',
            'video_play_curve_actions',
            'canvas_avg_view_time',
            'video_30_sec_watched_actions',
            'cost_per_action_type',
            'video_p100_watched_actions',
            'conversion_rate_ranking'
        },
        "ads_insights_region": {
            'video_p75_watched_actions',
            'conversions',
            'conversion_values',
            'canvas_avg_view_percent',
            'action_values',
            'unique_outbound_clicks',
            'cost_per_unique_action_type',
            'outbound_clicks',
            'social_spend',
            'video_p50_watched_actions',
            'engagement_rate_ranking',
            'video_p25_watched_actions',
            'quality_ranking',
            'video_play_curve_actions',
            'video_30_sec_watched_actions',
            'canvas_avg_view_time',
            'cost_per_action_type',
            'video_p100_watched_actions',
            'conversion_rate_ranking'
        },
        "ads_insights_hourly_advertiser": {
            'conversions',
            'cost_per_estimated_ad_recallers',
            'cost_per_unique_click',
            'cost_per_unique_outbound_click',
            'frequency',
            'conversion_values',
            'canvas_avg_view_percent',
            'cost_per_conversion',
            'cost_per_thruplay',
            'action_values',
            'full_view_impressions',
            'place_page_name',
            'instant_experience_outbound_clicks',
            'cost_per_unique_action_type',
            'estimated_ad_recallers',
            'outbound_clicks',
            'social_spend',
            'cost_per_unique_inline_link_click',
            'instant_experience_clicks_to_start',
            'attribution_setting',
            'engagement_rate_ranking',
            'purchase_roas',
            'reach',
            'cost_per_outbound_click',
            'estimated_ad_recall_rate',
            'quality_ranking',
            'cpp',
            'catalog_segment_value',
            'canvas_avg_view_time',
            'cost_per_action_type',
            'outbound_clicks_ctr',
            'qualifying_question_qualify_answer_rate',
            'converted_product_quantity',
            'converted_product_value',
            'instant_experience_clicks_to_open',
            'conversion_rate_ranking'
        },
        "ads": {
            'recommendations',
            'adlabels'
        },
        "ads_insights_platform_and_device": {
            'video_p75_watched_actions',
            'conversions',
            'conversion_values',
            'canvas_avg_view_percent',
            'action_values',
            'unique_outbound_clicks',
            'cost_per_unique_action_type',
            'outbound_clicks',
            'social_spend',
            'video_p50_watched_actions',
            'engagement_rate_ranking',
            'video_p25_watched_actions',
            'quality_ranking',
            'video_play_curve_actions',
            'conversion_rate_ranking',
            'video_30_sec_watched_actions',
            'canvas_avg_view_time',
            'cost_per_action_type',
            'video_p100_watched_actions',
            'placement'
        }
    }

    # TODO: https://jira.talendforge.org/browse/TDL-26640
    EXCLUDE_STREAMS = {
        'ads_insights_hourly_advertiser',   # TDL-24312, TDL-26640
        'ads_insights_platform_and_device', # TDL-26640
        'ads_insights',                     # TDL-26640
        'ads_insights_age_and_gender',      # TDL-26640
        'ads_insights_country',             # TDL-26640
        'ads_insights_dma',                 # TDL-26640
        'ads_insights_region'               # TDL-26640
    }

    @staticmethod
    def name():
        return "tt_facebook_all_fields_test"

    def streams_to_test(self):
        expected_streams = self.expected_metadata().keys()
        self.assert_message = f"JIRA ticket has moved to done, \
                                re-add the applicable stream to the test: {0}"
        assert base.JIRA_CLIENT.get_status_category("TDL-24312") != 'done',\
            self.assert_message.format('ads_insights_hourly_advertiser')
        expected_streams = self.expected_metadata().keys() - {'ads_insights_hourly_advertiser'}
        LOGGER.warn(f"Skipped streams: {'ads_insights_hourly_advertiser'}")

        assert base.JIRA_CLIENT.get_status_category("TDL-26640") != 'done',\
            self.assert_message.format(self.EXCLUDE_STREAMS)
        expected_streams = self.expected_metadata().keys() - self.EXCLUDE_STREAMS
        LOGGER.warn(f"Skipped streams: {self.EXCLUDE_STREAMS}")

        return expected_streams
