import test_client as tc
import time
import unittest

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
        # return self.expected_stream_names()
        return {'campaigns'}  # TODO WIP, expand to all core streams

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
        for stream in self.streams_to_test():
            response = fb_client.get_account_objects(stream)
            self.assertGreater(len(response['data']), 0,
                               msg='Failed HTTP get response for stream: {}'.format(stream))
            number_of_records = len(response['data'])
            if number_of_records > self.expected_page_size(stream):
                continue
            LOGGER.info(f"Stream: {stream} - Record count is less than max page size: "
                        f"{self.expected_page_size(stream)}. Posting more records to setUp "
                        "the PaginationTest")
            for i in range(self.expected_page_size(stream) - number_of_records + 1):
                post_response = fb_client.create_account_objects(stream)
                self.assertEqual(post_response.status_code, 200,
                                   msg='Failed HTTP post response for stream: {}'.format(stream))
                LOGGER.info(f"Posted {i + 1} new campaigns, new total: {number_of_records + i + 1}")
                time.sleep(1)

        # run initial sync
        PaginationTest.record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        PaginationTest.synced_records = runner.get_records_from_target_output()
