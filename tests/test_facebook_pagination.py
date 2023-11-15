import test_client as tc
import time
import unittest

from datetime import datetime as dt

from tap_tester.base_suite_tests.pagination_test import PaginationTest
from tap_tester import connections, runner, menagerie, LOGGER

from base_new_frmwrk import FacebookBaseTest

fb_client = tc.TestClient()


class FacebookDiscoveryTest(PaginationTest, FacebookBaseTest):
    """Standard Pagination Test"""

    @staticmethod
    def name():
        return "tt_facebook_pagination"
    def streams_to_test(self):
        # TODO ads_insights empty for account, no post via API, spike on generating data
        return {'adcreative', 'ads', 'adsets', 'campaigns'}

    def setUp(self):  # pylint: disable=invalid-name
        """
        Setup for tests in this module.
        """
        if PaginationTest.synced_records and PaginationTest.record_count_by_stream:
            return

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in self.streams_to_test()]

        # non_selected_fields are none
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        # ensure there is enough data to paginate
        start_date_dt = self.parse_date(self.start_date)
        date_range = {'since': dt.strftime(start_date_dt, "%Y-%m-%d"),
                      'until': dt.strftime(dt.now(), "%Y-%m-%d")}

        for stream in self.streams_to_test():
            limit = self.expected_page_size(stream)
            response = fb_client.get_account_objects(stream, limit, date_range)

            number_of_records = len(response['data'])
            # TODO move "if" logic below to client method get_account_objects()
            if number_of_records >= limit and response.get('paging', {}).get('next'):
                continue  # stream is ready for test, no need for futher action

            LOGGER.info(f"Stream: {stream} - Record count is less than max page size: {limit}, "
                        "posting more records to setUp the PaginationTest")

            for i in range(limit - number_of_records + 1):
                post_response = fb_client.create_account_objects(stream)
                LOGGER.info(f"Posted {i + 1} new {stream}, new total: {number_of_records + i + 1}")

        # run initial sync
        PaginationTest.record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        PaginationTest.synced_records = runner.get_records_from_target_output()
