"""
Test that with no fields selected for a stream all fields are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base_new_frmwrk import FacebookBaseTest


class FacebookAllFieldsTest(AllFieldsTest, FacebookBaseTest):
    """Test that with no fields selected for a stream all fields are still replicated"""
    @staticmethod
    def name():
        return "tt_facebook_all_fields_test"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())

    def test_all_fields_for_streams_are_replicated(self):
        self.selected_fields = self.get_upsert_only_fields(AllFieldsTest.selected_fields)
        super().test_all_fields_for_streams_are_replicated()
