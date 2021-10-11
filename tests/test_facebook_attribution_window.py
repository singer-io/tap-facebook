import os

from tap_tester import runner, connections

from base import FacebookBaseTest

class FacebookAttributionWindow(FacebookBaseTest):

    # set attribution window
    attrubution_window = 7

    @staticmethod
    def name():
        return "tap_tester_facebook_attribution_window"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : '2019-07-24T00:00:00Z',
            'end_date' : '2019-07-26T00:00:00Z',
            'insights_buffer_days': str(self.attrubution_window)
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):
        self.run_test(self.attrubution_window) # attribution window: 7

        self.attrubution_window = 28
        self.run_test(self.attrubution_window) # attribution window: 28

    def run_test(self, attr_window):
        """
            Test to check the attribution window
        """

        conn_id = connections.ensure_connection(self)

        # get start date
        start_date = self.get_properties()['start_date']
        # calculate start date with attribution window
        start_date_with_attribution_window = self.timedelta_formatted(start_date, days=-attr_window)

        # 'attribution window' is only supported for 'ads_insights' streams
        expected_streams = []
        for stream in self.expected_streams():
            if self.is_insight(stream):
                expected_streams.append(stream)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, select_all_fields=True)

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()

        expected_replication_keys = self.expected_replication_keys()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                replication_key = next(iter(expected_replication_keys[stream]))

                # get records
                records = [record.get('data') for record in sync_records.get(stream).get('messages')]

                # check for the record is between attribution date and start date
                is_between = False

                for record in records:
                    replication_key_value = record.get(replication_key)

                    # Verify the sync records respect the (simulated) start date value
                    self.assertGreaterEqual(self.parse_date(replication_key_value), self.parse_date(start_date_with_attribution_window),
                                            msg="The record does not respect the attribution window.")

                    # verify if the record's bookmark value is between start date and (simulated) start date value
                    if self.parse_date(start_date_with_attribution_window) <= self.parse_date(replication_key_value) < self.parse_date(start_date):
                        is_between = True

                    self.assertTrue(is_between, msg="No record found between start date and attribution date.")
