import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from functools import reduce

class FacebookBookmarks(unittest.TestCase):
    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_FACEBOOK_ACCESS_TOKEN'),
                                    os.getenv('TAP_FACEBOOK_ACCOUNT_ID')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_FACEBOOK_ACCESS_TOKEN, TAP_FACEBOOK_ACCOUNT_ID")

    def name(self):
        return "tap_tester_facebook_bookmarks"

    def get_type(self):
        return "platform.facebook"

    def get_credentials(self):
        return {'access_token': os.getenv('TAP_FACEBOOK_ACCESS_TOKEN')}

    def expected_check_streams(self):
        return {
            'ads',
            'adcreative',
            'adsets',
            'campaigns',
            'ads_insights',
            'ads_insights_age_and_gender',
            'ads_insights_country',
            'ads_insights_platform_and_device',
            'ads_insights_region',
            'ads_insights_dma',
        }

    def expected_sync_streams(self):
        return {
            "ads",
            "adcreative",
            "adsets",
            "campaigns",
            "ads_insights",
            "ads_insights_age_and_gender",
            "ads_insights_country",
            "ads_insights_platform_and_device",
            "ads_insights_region",
            "ads_insights_dma",
        }

    def tap_name(self):
        return "tap-facebook"

    def expected_pks(self):
        return {
            "ads" :                             {"id", "updated_time"},
            "adcreative" :                      {'id'},
            "adsets" :                          {"id", "updated_time"},
            "campaigns" :                       {"id"},
            "ads_insights" :                    {"campaign_id", "adset_id", "ad_id", "date_start"},
            "ads_insights_age_and_gender" :     {"campaign_id", "adset_id", "ad_id", "date_start", "age", "gender"},
            "ads_insights_country" :            {"campaign_id", "adset_id", "ad_id", "date_start"},
            "ads_insights_platform_and_device": {"campaign_id", "adset_id", "ad_id", "date_start", "publisher_platform", "platform_position", "impression_device"},
            "ads_insights_region":              {"campaign_id", "adset_id", "ad_id", "date_start"},
            "ads_insights_dma":                 {"campaign_id", "adset_id", "ad_id", "date_start"},
        }

    def get_properties(self):
        return {'start_date' : '2015-03-15T00:00:00Z',
                'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
                'end_date': '2015-03-16T00:00:00+00:00',
                'insights_buffer_days': '1'
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        #select all catalogs
        #selected_catalogs = list(map(lambda catalog: self.perform_field_selection(conn_id, catalog), found_catalogs))
        #menagerie.post_annotated_catalogs(conn_id, selected_catalogs)

        for c in found_catalogs:
            connections.select_catalog_and_fields_via_metadata(conn_id, c,
                                                               menagerie.get_annotated_schema(conn_id, c['stream_id']))

        #clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # bookmarks for the 4 streams should be 2015-03-16
        states = menagerie.get_state(conn_id)["bookmarks"]
        end_date = self.get_properties()["end_date"].split()[0]
        for k, v in states.items():
            if "insights" in k:
                bm_date = v.get("date_start")
                self.assertEqual(end_date, bm_date)
        print("bookmarks match end_date of {}".format(end_date))
