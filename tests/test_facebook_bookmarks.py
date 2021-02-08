import os
from functools import reduce

from tap_tester import connections, menagerie, runner

from base import FacebookBaseTest


class FacebookBookmarks(FacebookBaseTest):  # TODO use base.py and update test

    @staticmethod
    def name():
        return "tap_tester_facebook_bookmarks"

    def get_properties(self):  # pylint: disable=arguments-differ
        return {'start_date' : '2015-03-15T00:00:00Z',
                'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
                'end_date': '2015-03-16T00:00:00+00:00',
                'insights_buffer_days': '1'
        }

    def test_run(self):
        """Test bookmarks"""
        expected_streams = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        #select all catalogs
        # for c in found_catalogs:
        #     connections.select_catalog_and_fields_via_metadata(conn_id, c,
        #                                                        menagerie.get_annotated_schema(conn_id, c['stream_id']))
        # clear state and run sync
        menagerie.set_state(conn_id, {})
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()


        # bookmarks for the 4 streams should be 2015-03-16
        states = menagerie.get_state(conn_id)["bookmarks"]
        end_date = self.get_properties()["end_date"].split()[0]
        for k, v in states.items():
            if "insights" in k:
                bm_date = v.get("date_start")
                self.assertEqual(end_date, bm_date)
        print("bookmarks match end_date of {}".format(end_date))
