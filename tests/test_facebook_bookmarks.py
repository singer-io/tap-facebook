import os
import datetime
import dateutil.parser
import pytz

from tap_tester import runner, menagerie, connections

from base import FacebookBaseTest


class FacebookBookmarks(FacebookBaseTest):
    @staticmethod
    def name():
        return "tap_tester_facebook_bookmarks"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : '2019-07-22T00:00:00Z',
            'end_date' : '2019-07-26T00:00:00Z',
            'insights_buffer_days': '1'
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def expected_sync_streams():
        """
        TODO ads, adsets, and campaigns only have 1 record,
             to get more testable data we need at least 2 records per stream.
        """
        return {
            # "ads",
            "adcreative",
            # "adsets",
            # "campaigns",
            "ads_insights",
            "ads_insights_age_and_gender",
            "ads_insights_country",
            "ads_insights_platform_and_device",
            "ads_insights_region",
            "ads_insights_dma",
        }

    @staticmethod
    def convert_state_to_utc(date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def calculated_states_by_stream(self, current_state):
        """
        Look at the bookmarks from a previous sync and set a new bookmark
        value that is 1 day prior. This ensures the subsequent sync will replicate
        at least 1 record but, fewer records than the previous sync.
        """

        stream_to_current_state = {stream : bookmark.get(self.expected_replication_keys()[stream].pop())
                                   for stream, bookmark in current_state['bookmarks'].items()}
        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}

        for stream, state in stream_to_current_state.items():
            # convert state from string to datetime object
            state_as_datetime = dateutil.parser.parse(state)
            # subtract n days from the state
            n = 2
            calculated_state_as_datetime = state_as_datetime - datetime.timedelta(days=n)
            # convert back to string and format
            calculated_state = datetime.datetime.strftime(calculated_state_as_datetime, '%Y-%m-%dT00:00:00+00:00')
            stream_to_calculated_state[stream] = calculated_state

        return stream_to_calculated_state


    def test_run(self):
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        expected_insights_buffer = -1 * int(self.get_properties()['insights_buffer_days'])  # lookback window

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_sync_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update State Between Syncs
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            replication_key = list(expected_replication_keys[stream])[0]
            new_states['bookmarks'][stream] = {replication_key: new_state}
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)


        ##########################################################################
        ### Test By Stream
        ##########################################################################

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)


                if expected_replication_method == self.INCREMENTAL:


                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = list(expected_replication_keys[stream])[0]
                    self.assertEqual(1, len(list(expected_replication_keys[stream]))) # catches unexpected compound replication key
                    first_bookmark_value = first_bookmark_key_value.get(replication_key)
                    second_bookmark_value = second_bookmark_key_value.get(replication_key)
                    first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                    simulated_bookmark_value = new_states['bookmarks'][stream][replication_key]
                    simulated_bookmark_minus_lookback = self.timedelta_formatted(
                        simulated_bookmark_value, days=expected_insights_buffer
                    )

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_key_value.get(replication_key))

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_key_value.get(replication_key))

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                    # TODO refactor assertions synctax below THIS POINT
                    #      - could do a list comprehension and compare all() to the expected/test value then just assertTrue
                    #      - see if possible to drop these msg's and have a failure that is still clear
                    #      - cleanup variable names
                    # NOTE: TIMEBOX ^ to like 20 minutes, the test is pretty clear as-is

                    # Verify the second sync records respect the previous (simulated) bookmark value
                    for record in second_sync_messages:
                        replication_key_value = record.get(replication_key)
                        if stream == 'ads_insights_age_and_gender': # BUG | https://stitchdata.atlassian.net/browse/SRCE-4873
                            replication_key_value = datetime.datetime.strftime(
                                dateutil.parser.parse(replication_key_value),
                                self.BOOKMARK_COMPARISON_FORMAT
                            )
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_minus_lookback,
                                                msg="Second sync records do not repect the previous bookmark.")

                    # Verify the first sync bookmark value is the max replication key value for a given stream
                    for record in first_sync_messages:
                        replication_key_value = record.get(replication_key)
                        self.assertLessEqual(
                            replication_key_value, first_bookmark_value_utc,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    # Verify the second sync bookmark value is the max replication key value for a given stream
                    for record in second_sync_messages:
                        replication_key_value = record.get(replication_key)
                        self.assertLessEqual(
                            replication_key_value, second_bookmark_value_utc,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    # TODO refactor assertions synctax above THIS POINT

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)


                elif expected_replication_method == self.FULL_TABLE:


                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)


                else:


                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method)
                    )


                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
