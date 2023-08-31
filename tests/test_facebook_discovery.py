import unittest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base_new_frmwrk import FacebookBaseTest


class FacebookDiscoveryTest(DiscoveryTest, FacebookBaseTest):
    """Standard Discovery Test"""

    @staticmethod
    def name():
        return "tt_facebook_discovery"
    def streams_to_test(self):
        return self.expected_stream_names()
