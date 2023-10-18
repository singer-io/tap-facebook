
import os
from datetime import timedelta
from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class FacebookBaseTest(BaseCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).

    Insights Test Data by Date Ranges
        "ads_insights":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_age_and_gender":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_country":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_platform_and_device":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_region":
          "2019-08-03T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_dma":
          "2019-08-03T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_hourly_advertiser":
          "2019-08-03T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"

    """
    FULL_TABLE = "FULL_TABLE"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT00:00:00+00:00"

    start_date = ""
    end_date = ""

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-facebook"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.facebook"

    def get_properties(self):
        """Configuration properties required for the tap."""
        return {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : '2021-04-07T00:00:00Z',
            'end_date': '2021-04-09T00:00:00Z',
            'insights_buffer_days': '1',
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {'access_token': os.getenv('TAP_FACEBOOK_ACCESS_TOKEN')}
    @staticmethod
    def expected_metadata():
        """The expected streams and metadata about the streams"""
        return {
            "ads": {
                BaseCase.PRIMARY_KEYS: {"id", "updated_time"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updated_time"}
            },
            "adcreative": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.FULL_TABLE,
            },
            "adsets": {
                BaseCase.PRIMARY_KEYS: {"id", "updated_time"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updated_time"}
            },
            "campaigns": {
                BaseCase.PRIMARY_KEYS: {"id", },
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updated_time"}
            },
            "ads_insights": {
                BaseCase.PRIMARY_KEYS: {"campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_age_and_gender": {
                BaseCase.PRIMARY_KEYS: {
                    "campaign_id", "adset_id", "ad_id", "date_start", "age", "gender"
                },
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_country": {
                BaseCase.PRIMARY_KEYS: {"campaign_id", "adset_id", "ad_id", "date_start", "country"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_platform_and_device": {
                BaseCase.PRIMARY_KEYS: {
                    "campaign_id", "adset_id", "ad_id", "date_start",
                    "publisher_platform", "platform_position", "impression_device"
                },
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_region": {
                BaseCase.PRIMARY_KEYS: {"region", "campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_dma": {
                BaseCase.PRIMARY_KEYS: {"dma", "campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_hourly_advertiser": {
                BaseCase.PRIMARY_KEYS: {"hourly_stats_aggregated_by_advertiser_time_zone", "campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            # "leads": {
            #     BaseCase.PRIMARY_KEYS: {"id"},
            #     BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
            #     BaseCase.REPLICATION_KEYS: {"created_time"}
            # },
        }

    def set_replication_methods(self, conn_id, catalogs, replication_methods):

        replication_keys = self.expected_replication_keys()
        for catalog in catalogs:
            replication_method = replication_methods.get(catalog['stream_name'])
            annt=menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            if replication_method == self.INCREMENTAL:
                replication_key = list(replication_keys.get(catalog['stream_name']))[0]
                replication_md = [{ "breadcrumb": [], "metadata":{ "selected" : True}}]
            else:
                replication_md = [{ "breadcrumb": [], "metadata": { "selected": None}}]
            connections.set_non_discoverable_metadata(
                conn_id, catalog, menagerie.get_annotated_schema(conn_id, catalog['stream_id']), replication_md)

    ### Method to return the fields that are upsert only -
    ### exclude the non-upsert fields from the all fields test as these non-upsert fields are not replicated
    def get_upsert_only_fields(self, selected_fields, stream=None):
        non_upsert_fields = {
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
                'video_play_curve_actions',
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
                'bid_info'
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
                    'bid_amount',
                    'bid_info',
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
        actual_expected = {}
        non_upsert_streams = non_upsert_fields.keys()

        if stream:
            if stream in non_upsert_streams:
                actual_expected[stream] = selected_fields.get(stream).difference(non_upsert_fields.get(stream))
            else:
                actual_expected[stream] = selected_fields.get(stream)
        else:
            for key, fields in selected_fields.items():
                if key in non_upsert_streams:
                    actual_expected[key] = fields.difference(non_upsert_fields.get(key))
                else:
                    actual_expected[key] = fields
        return actual_expected

    @classmethod
    def setUpClass(cls,logging="Ensuring environment variables are sourced."):
        super().setUpClass(logging=logging)
        missing_envs = [x for x in [os.getenv('TAP_FACEBOOK_ACCESS_TOKEN'),
                                    os.getenv('TAP_FACEBOOK_ACCOUNT_ID')] if x is None]
        if len(missing_envs) != 0:
            raise Exception("set environment variables")


    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    @staticmethod
    def is_insight(stream):
        return stream.startswith('ads_insights')
