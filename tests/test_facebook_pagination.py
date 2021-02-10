import os

from tap_tester import connections, runner

from base import FacebookBaseTest


class FacebookPaginationTest(FacebookBaseTest):


    @staticmethod
    def name():
        return "tap_tester_facebook_pagination_test"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : '2019-07-22T00:00:00Z',
            'end_date' : '2019-07-26T00:00:00Z',
            'insights_buffer_days': '1',
            # 'result_return_limit': '1', # TODO causes 
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        expected_streams = self.expected_streams()
        import pdb; pdb.set_trace()
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_insights_buffer = -1 * int(self.get_properties()['insights_buffer_days'])
                expected_start_date = self.timedelta_formatted(self.start_date, days=expected_insights_buffer)

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records.get(stream).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_sync = set(primary_keys_list)

                if self.is_insight(stream):

                    # collect information specific to incremental streams from syncs 1 & 2
                    expected_replication_key = next(iter(self.expected_replication_keys().get(stream)))
                    replication_dates =[row.get('data').get(expected_replication_key) for row in
                                          synced_records.get(stream, {'messages': []}).get('messages', [])
                                          if row.get('data')]

                    # # Verify replication key is greater or equal to start_date for sync 1
                    for replication_date in replication_dates:
                        self.assertGreaterEqual(
                            self.parse_date(replication_date), self.parse_date(expected_start_date),
                                msg="Report pertains to a date prior to our start date.\n" +
                                "Sync start_date: {}\n".format(expected_start_date) +
                                "Record date: {} ".format(replication_date)
                        )
