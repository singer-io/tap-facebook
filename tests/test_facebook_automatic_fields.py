"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
import os

from tap_tester import runner, connections, LOGGER
import base
from base import FacebookBaseTest


class FacebookAutomaticFields(FacebookBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    is_done = None

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
        return "tap_tester_facebook_automatic_fields"

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

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : '2021-04-08T00:00:00Z',
            'end_date' : '2021-04-08T00:00:00Z',
            'insights_buffer_days': '1'
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value


    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """

        expected_streams = self.streams_to_test()

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]


                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)
