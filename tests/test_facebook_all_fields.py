"""
Test that with no fields selected for a stream all fields are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base_new_frmwrk import FacebookBaseTest
import base


class FacebookAllFieldsTest(AllFieldsTest, FacebookBaseTest):
    """Test that with no fields selected for a stream all fields are still replicated"""
    is_done = None

    @staticmethod
    def name():
        return "tt_facebook_all_fields_test"

    def streams_to_test(self):
        #return set(self.expected_metadata().keys())
        # Fail the test when the JIRA card is done to allow stream to be re-added and tested
        if self.is_done is None:
            self.is_done = base.JIRA_CLIENT.get_status_category("TDL-24312") == 'done'
            self.assert_message = ("JIRA ticket has moved to done, re-add the "
                                   "ads_insights_hourly_advertiser stream to the test.")
        assert self.is_done != True, self.assert_message

        return self.expected_metadata().keys() - {'ads_insights_hourly_advertiser', 'ads_insights'}

    def test_all_fields_for_streams_are_replicated(self):
        self.selected_fields = self.get_upsert_only_fields(AllFieldsTest.selected_fields)
        super().test_all_fields_for_streams_are_replicated()
