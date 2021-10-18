import unittest
from unittest.mock import Mock
from unittest import mock
from requests.exceptions import Timeout
from tap_facebook import AdCreative, Ads, AdSets, Campaigns, AdsInsights, Leads

@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):
    """A set of unit tests to ensure that requests are retrying properly for Timeout Error"""
    def test_get_adcreatives(self, mocked_sleep):
        """ 
            AdCreative.get_adcreatives calls a `facebook_business` method,`get_ad_creatives()`, to get a batch of ad creatives. 
            We mock this method to raise a `Timeout` and expect the tap to retry this that function up to 5 times,
            which is the current hard coded `max_tries` value.
        """

        # Mock get_ad_creatives function to throw Timeout exception
        mocked_account = Mock()
        mocked_account.get_ad_creatives = Mock()
        mocked_account.get_ad_creatives.side_effect = Timeout

        # Call get_adcreatives() function of AdCreatives and verify Timeout is raised
        ad_creative_object = AdCreative('', mocked_account, '', '')
        with self.assertRaises(Timeout):
            ad_creative_object.get_adcreatives()

        # verify get_ad_creatives() is called 5 times as max 5 reties provided for function
        self.assertEquals(mocked_account.get_ad_creatives.call_count, 5)

    def test__call_get_ads(self, mocked_sleep):
        """ 
            Ads._call_get_ads calls a `facebook_business` method,`get_ads()`, to get a batch of ads. 
            We mock this method to raise a `Timeout` and expect the tap to retry this that function up to 5 times,
            which is the current hard coded `max_tries` value.
        """

        # Mock get_ads function to throw Timeout exception
        mocked_account = Mock()
        mocked_account.get_ads = Mock()
        mocked_account.get_ads.side_effect = Timeout

        # Call _call_get_ads() function of Ads and verify Timeout is raised
        ad_object = Ads('', mocked_account, '', '', '')
        with self.assertRaises(Timeout):
            ad_object._call_get_ads('test')

        # verify get_ads() is called 5 times as max 5 reties provided for function
        self.assertEquals(mocked_account.get_ads.call_count, 5)

    def test__call_get_ad_sets(self, mocked_sleep):
        """ 
            AdSets._call_get_ad_sets calls a `facebook_business` method,`get_ad_sets()`, to get a batch of adsets. 
            We mock this method to raise a `Timeout` and expect the tap to retry this that function up to 5 times,
            which is the current hard coded `max_tries` value.
        """

        # Mock get_ad_sets function to throw Timeout exception
        mocked_account = Mock()
        mocked_account.get_ad_sets = Mock()
        mocked_account.get_ad_sets.side_effect = Timeout

        # Call _call_get_ad_sets() function of AdSets and verify Timeout is raised
        ad_set_object = AdSets('', mocked_account, '', '', '')
        with self.assertRaises(Timeout):
            ad_set_object._call_get_ad_sets('test')

        # verify get_ad_sets() is called 5 times as max 5 reties provided for function
        self.assertEquals(mocked_account.get_ad_sets.call_count, 5)

    def test__call_get_campaigns(self, mocked_sleep):
        """ 
            Campaigns._call_get_campaigns calls a `facebook_business` method,`get_campaigns()`, to get a batch of campaigns. 
            We mock this method to raise a `Timeout` and expect the tap to retry this that function up to 5 times,
            which is the current hard coded `max_tries` value.
        """

        # Mock get_campaigns function to throw Timeout exception
        mocked_account = Mock()
        mocked_account.get_campaigns = Mock()
        mocked_account.get_campaigns.side_effect = Timeout

        # Call _call_get_campaigns() function of Campaigns and verify Timeout is raised
        campaigns_object = Campaigns('', mocked_account, '', '', '')
        with self.assertRaises(Timeout):
            campaigns_object._call_get_campaigns('test')

        # verify get_campaigns() is called 5 times as max 5 reties provided for function
        self.assertEquals(mocked_account.get_campaigns.call_count, 5)

    def test_run_job(self, mocked_sleep):
        """ 
            AdsInsights.run_job calls a `facebook_business` method,`get_insights()`, to get a batch of insights. 
            We mock this method to raise a `Timeout` and expect the tap to retry this that function up to 5 times,
            which is the current hard coded `max_tries` value.
        """

        # Mock get_insights function to throw Timeout exception
        mocked_account = Mock()
        mocked_account.get_insights = Mock()
        mocked_account.get_insights.side_effect = Timeout

        # Call run_job() function of Campaigns and verify Timeout is raised
        ads_insights_object = AdsInsights('', mocked_account, '', '', '', {})
        with self.assertRaises(Timeout):
            ads_insights_object.run_job('test')

        # verify get_insights() is called 5 times as max 5 reties provided for function
        self.assertEquals(mocked_account.get_insights.call_count, 5)
