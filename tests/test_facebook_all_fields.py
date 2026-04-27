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
            'source_instagram_media_id'
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

    EXCLUDE_STREAMS = {
        'ads_insights_hourly_advertiser',    # TDL-24312
        'ads_insights_platform_and_device',  # SAC-30725
        'ads_insights',                      # SAC-30725
        'ads_insights_age_and_gender',       # SAC-30725
        'ads_insights_country',              # SAC-30725
        'ads_insights_dma',                  # SAC-30725
        'ads_insights_region'                # SAC-30725
    }

    FICKLE_FIELDS = {
        "adcreative": {
            'video_id',
        }
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
        expected_streams = self.expected_metadata().keys() - self.EXCLUDE_STREAMS
        LOGGER.warn(f"Skipped streams: {self.EXCLUDE_STREAMS}")

        return expected_streams

    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.test_streams:
            with self.subTest(stream=stream):

                # gather expectations
                self.expected_all_keys = (
                    self.selected_fields.get(stream, set())
                    - set(self.MISSING_FIELDS.get(stream, {})) \
                    - set(self.KEYS_WITH_NO_DATA.get(stream,{})) \
                    | set(self.EXTRA_FIELDS.get(stream, {}))
                )

                # gather results
                self.fields_replicated = self.actual_fields.get(stream, set())
                self.remove_bad_keys(stream)

                fickle_fields = self.FICKLE_FIELDS.get(stream, set())

                # Top level check for fickle fields, strict assertion is maintained
                top_level_fickle = fickle_fields & self.fields_replicated

                # Nested check for fickle field, skipping strict assertion
                excluded_fickle = fickle_fields - top_level_fickle

                self.assertSetEqual(
                    self.fields_replicated - excluded_fickle,
                    self.expected_all_keys - excluded_fickle,
                    logging=f"verify all fields are replicated for stream {stream}"
                )

                if excluded_fickle:
                    LOGGER.warning(
                        f"Fickle fields missing for stream {stream} at top level,"
                        f"likely nested: {excluded_fickle}"
                    )
