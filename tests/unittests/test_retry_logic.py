import unittest
from unittest.mock import Mock
# from facebook_business.exceptions import FacebookRequestError

from tap_facebook import FacebookRequestError
from tap_facebook import AdCreative

    
class TestAdCreatives(unittest.TestCase):
    """A set of unit tests to ensure that requests to get AdCreatives behave
    as expected"""
    def test_retries_on_500(self):
        """`AdCreative.sync.do_request()` calls a `facebook_business` method,
        `get_ad_creatives()`, to make a request to the API. We mock this
        method to raise a `FacebookRequestError` with an `http_status` of
        `500`.

        We expect the tap to retry this request up to 5 times, which is
        the current hard coded `max_tries` value.
        """

        # Create the mock and force the function to throw an error
        mocked_account = Mock()
        mocked_account.get_ad_creatives = Mock()
        mocked_account.get_ad_creatives.side_effect = FacebookRequestError(
            message='',
            request_context={"":Mock()},
            http_status=500,
            http_headers=Mock(),
            body={}
        )

        # Initialize the object and call `sync()`
        ad_creative_object = AdCreative('', mocked_account, '', '')
        with self.assertRaises(FacebookRequestError):
            ad_creative_object.sync()
        # 5 is the max tries specified in the tap    
        self.assertEquals(5, mocked_account.get_ad_creatives.call_count )    
        
        
