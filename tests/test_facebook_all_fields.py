from tap_tester import runner, connections, menagerie
import os
from base import FacebookBaseTest

class FacebookAllFieldsTest(FacebookBaseTest):

    # common fields which are not available in ads_insights_dma, ads_insights, ads_insights_region, ads_insights_country, ads_insights_age_and_gender, ads_insights_platform_and_device, ads_insights_hourly_advertiser streams
    common_fields_to_remove = {'cost_per_unique_inline_link_click', 'cost_per_inline_link_click', 'cpc', 'cost_per_unique_action_type','canvas_avg_view_percent', 'inline_link_clicks', 'website_ctr', 'canvas_avg_view_time', 'cost_per_unique_click', 'outbound_clicks', 'inline_link_click_ctr', 'action_values', 'cost_per_action_type', 'canvas_avg_view_percent', 'cost_per_unique_click'}
    
    # common fields which are not available in ads_insights_dma, ads_insights_region, ads_insights_country, ads_insights_age_and_gender, ads_insights_platform_and_device streams
    common_fields_to_remove_2 = {'quality_ranking','video_30_sec_watched_actions', 'unique_inline_link_clicks', 'unique_link_clicks_ctr', 'video_p50_watched_actions', 'video_p25_watched_actions', 'unique_inline_link_click_ctr', 'unique_outbound_clicks', 'video_p75_watched_actions', 'engagement_rate_ranking', 'unique_clicks', 'video_play_curve_actions', 'social_spend', 'conversion_rate_ranking', 'video_p100_watched_actions'}

    # Removing below fields as data cannot be generated
    fields_to_remove = {
        'campaigns': {'adlabels'}, 
        'ads': {
            'adlabels',
            'recommendations',
            'bid_amount',
            'bid_info'}, 
        'adsets': {
            'adlabels',
            'bid_info'},
        'ads_insights_dma': {
            'unique_ctr', 
            'cpp'}| common_fields_to_remove| common_fields_to_remove_2, 
        'adcreative': {
            'object_id',
            'product_set_id',
            'url_tags',
            'object_url',
            'template_url_spec',
            'applink_treatment',
            'instagram_story_id',
            'template_url',
            'link_url',
            'adlabels',
            'link_og_id',
            'image_crops'},
        'ads_insights': {
            'unique_inline_link_clicks',
            'unique_inline_link_click_ctr',
            'unique_outbound_clicks',
            'video_30_sec_watched_actions',
            'video_p50_watched_actions',
            'video_p100_watched_actions',
            'video_play_curve_actions',
            'unique_link_clicks_ctr',
            'video_p75_watched_actions',
            'video_p25_watched_actions'} | common_fields_to_remove, 
        'ads_insights_region': 
            common_fields_to_remove| common_fields_to_remove_2,
        'ads_insights_country': 
            common_fields_to_remove| common_fields_to_remove_2,
        'ads_insights_age_and_gender': 
            common_fields_to_remove| common_fields_to_remove_2,
        'ads_insights_platform_and_device': {'placement'}|
            common_fields_to_remove| common_fields_to_remove_2,
        'ads_insights_hourly_advertiser': {
            'qualifying_question_qualify_answer_rate',
            'social_spend',
            'cost_per_thruplay',
            'quality_ranking',
            'full_view_impressions',
            'engagement_rate_ranking',
            'conversions',
            'cost_per_outbound_click',
            'catalog_segment_value',
            'converted_product_quantity',
            'purchase_roas',
            'cost_per_estimated_ad_recallers',
            'conversion_values',
            'converted_product_value',
            'instant_experience_clicks_to_open',
            'attribution_setting',
            'place_page_name',
            'mobile_app_purchase_roas',
            'frequency',
            'reach',
            'cost_per_unique_outbound_click',
            'instant_experience_outbound_clicks',
            'instant_experience_clicks_to_start',
            'estimated_ad_recallers',
            'conversion_rate_ranking',
            'cost_per_conversion',
            'cpp',
            'outbound_clicks_ctr',
            'website_purchase_roas',
            'estimated_ad_recall_rate'}|common_fields_to_remove,
    }

    @staticmethod
    def name():
        return "tap_tester_facebook_all_fields_test"
    
    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : '2019-07-22T00:00:00Z',
            'end_date' : '2019-07-23T00:00:00Z',
            'insights_buffer_days': '1'
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):
        """
        Testing that all fields mentioned in the catalog are synced from the tap
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream
        """
        expected_streams = self.expected_streams()
        
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs,select_all_fields=True)

        # # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in found_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1] for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_automatic_keys = self.expected_primary_keys()[stream] | self.expected_replication_keys()[stream]

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                fields = self.fields_to_remove.get(stream) or []
                for field in fields:
                    expected_all_keys.remove(field)
                
                self.assertSetEqual(expected_all_keys, actual_all_keys)