"""
Unit tests for `get_ad_accounts()` and the account-discovery portion of
`main_impl()` in `tap_facebook/__init__.py`.

Background: `facebook_business`'s Cursor-based pagination can intermittently
raise `TypeError: string indices must be integers` (Python <3.11) or
`TypeError: string indices must be integers, not 'str'` (Python 3.11+) when a
page of a list-edge response (e.g. `/me/adaccounts`) isn't in the shape its
`ObjectParser` expects. Every other Facebook API call in this tap already
retries around this known SDK bug via `retry_pattern`; these tests cover the
same protection added to the account-discovery call site in `main_impl()`.
"""
import unittest
from unittest import mock
from unittest.mock import Mock

import tap_facebook
from facebook_business.exceptions import FacebookRequestError
from requests.exceptions import ConnectionError, Timeout
from singer import SingerConfigurationError


@mock.patch("time.sleep")
class TestGetAdAccountsRetry(unittest.TestCase):
    """Unit tests for the `get_ad_accounts()` helper, which wraps
    `User.get_ad_accounts()` with the same retry_pattern protection used
    elsewhere in the tap."""

    def test_returns_list_of_accounts(self, mocked_sleep):
        """On success, `get_ad_accounts()` returns a fully-realized list built
        from the user's accounts, with no retries."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.return_value = [{'account_id': '123'}, {'account_id': '456'}]

        accounts = tap_facebook.get_ad_accounts(mocked_user)

        self.assertEqual(accounts, [{'account_id': '123'}, {'account_id': '456'}])
        self.assertEqual(1, mocked_user.get_ad_accounts.call_count)

    def test_retries_on_type_error_old_message_format(self, mocked_sleep):
        """A persistent `TypeError("string indices must be integers")` (the
        pre-3.11 message format) is retried up to the hard coded 5 max tries,
        then re-raised."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.side_effect = TypeError("string indices must be integers")

        with self.assertRaises(TypeError):
            tap_facebook.get_ad_accounts(mocked_user)
        self.assertEqual(5, mocked_user.get_ad_accounts.call_count)

    def test_retries_on_type_error_python311_message_format(self, mocked_sleep):
        """A persistent `TypeError("string indices must be integers, not 'str'")`
        (the Python 3.11+ message format) is retried up to the hard coded 5 max
        tries, then re-raised. This is the format actually raised in production
        and is the scenario that previously crashed discovery uncaught."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.side_effect = TypeError("string indices must be integers, not 'str'")

        with self.assertRaises(TypeError):
            tap_facebook.get_ad_accounts(mocked_user)
        self.assertEqual(5, mocked_user.get_ad_accounts.call_count)

    def test_does_not_retry_unrelated_type_error(self, mocked_sleep):
        """A `TypeError` with an unrelated message is not recognized as the
        known SDK bug, so it is not retried -- it propagates immediately."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.side_effect = TypeError("some unrelated failure")

        with self.assertRaises(TypeError):
            tap_facebook.get_ad_accounts(mocked_user)
        self.assertEqual(1, mocked_user.get_ad_accounts.call_count)

    def test_retries_on_facebook_request_error(self, mocked_sleep):
        """A transient `FacebookRequestError` (e.g. http_status=500) is retried
        up to the hard coded 5 max tries, then re-raised."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.side_effect = FacebookRequestError(
            message='',
            request_context={"": Mock()},
            http_status=500,
            http_headers=Mock(),
            body={}
        )

        with self.assertRaises(FacebookRequestError):
            tap_facebook.get_ad_accounts(mocked_user)
        self.assertEqual(5, mocked_user.get_ad_accounts.call_count)

    def test_retries_on_attribute_error(self, mocked_sleep):
        """A persistent `AttributeError` (another known transient SDK failure
        mode) is retried up to the hard coded 5 max tries, then re-raised."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.side_effect = AttributeError

        with self.assertRaises(AttributeError):
            tap_facebook.get_ad_accounts(mocked_user)
        self.assertEqual(5, mocked_user.get_ad_accounts.call_count)

    def test_retries_on_timeout(self, mocked_sleep):
        """A persistent `Timeout` is retried up to the hard coded 5 max tries
        by the outer retry_pattern decorator, then re-raised."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.side_effect = Timeout

        with self.assertRaises(Timeout):
            tap_facebook.get_ad_accounts(mocked_user)
        self.assertEqual(5, mocked_user.get_ad_accounts.call_count)

    def test_succeeds_after_transient_type_errors(self, mocked_sleep):
        """If the known SDK bug only occurs on the first couple of attempts,
        `get_ad_accounts()` should recover and return the eventual good
        response instead of raising."""
        mocked_user = Mock()
        mocked_user.get_ad_accounts = Mock()
        mocked_user.get_ad_accounts.side_effect = [
            TypeError("string indices must be integers, not 'str'"),
            TypeError("string indices must be integers, not 'str'"),
            [{'account_id': '123'}],
        ]

        accounts = tap_facebook.get_ad_accounts(mocked_user)

        self.assertEqual(accounts, [{'account_id': '123'}])
        self.assertEqual(3, mocked_user.get_ad_accounts.call_count)


# Mock args, mirroring the pattern used in test_request_timeout.py
class Args():
    def __init__(self, config):
        self.config = config
        self.discover = False
        self.properties = False
        self.catalog = False
        self.state = False


@mock.patch("time.sleep")
@mock.patch('tap_facebook.utils.parse_args')
@mock.patch("tap_facebook.FacebookAdsApi.init")
@mock.patch("tap_facebook.fb_user.User")
class TestMainImplAccountDiscoveryTypeErrorSafetyNet(unittest.TestCase):
    """Unit tests for the `TypeError` safety net in `main_impl()`'s account
    discovery, which surfaces a persistent instance of the known
    facebook_business SDK malformed-page bug as a clear
    `SingerConfigurationError` instead of an unhandled crash."""

    def test_persistent_type_error_old_format_raises_singer_configuration_error(
            self, mocked_user, mocked_facebook_api, mocked_args, mocked_sleep):
        tap_facebook.CONFIG = {}
        config = {'account_id': 'test', 'access_token': 'test'}
        mocked_args.return_value = Args(config)

        mocked_fb_user = Mock()
        mocked_fb_user.get_ad_accounts = Mock()
        mocked_fb_user.get_ad_accounts.side_effect = TypeError("string indices must be integers")
        mocked_user.return_value = mocked_fb_user

        with self.assertRaises(SingerConfigurationError):
            tap_facebook.main_impl()

        # get_ad_accounts() retries internally up to 5 times before giving up
        self.assertEqual(5, mocked_fb_user.get_ad_accounts.call_count)

    def test_persistent_type_error_python311_format_raises_singer_configuration_error(
            self, mocked_user, mocked_facebook_api, mocked_args, mocked_sleep):
        """This is the exact scenario from the reported crash: discovery
        previously died with an unhandled `TypeError` traceback instead of a
        clear configuration error."""
        tap_facebook.CONFIG = {}
        config = {'account_id': 'test', 'access_token': 'test'}
        mocked_args.return_value = Args(config)

        mocked_fb_user = Mock()
        mocked_fb_user.get_ad_accounts = Mock()
        mocked_fb_user.get_ad_accounts.side_effect = TypeError("string indices must be integers, not 'str'")
        mocked_user.return_value = mocked_fb_user

        with self.assertRaises(SingerConfigurationError):
            tap_facebook.main_impl()

        self.assertEqual(5, mocked_fb_user.get_ad_accounts.call_count)

    def test_unrelated_type_error_is_not_converted(
            self, mocked_user, mocked_facebook_api, mocked_args, mocked_sleep):
        """A `TypeError` unrelated to the known SDK bug must propagate as-is
        (not be silently converted to a `SingerConfigurationError`), since the
        safety net only recognizes this specific, known failure mode."""
        tap_facebook.CONFIG = {}
        config = {'account_id': 'test', 'access_token': 'test'}
        mocked_args.return_value = Args(config)

        mocked_fb_user = Mock()
        mocked_fb_user.get_ad_accounts = Mock()
        mocked_fb_user.get_ad_accounts.side_effect = TypeError("some unrelated failure")
        mocked_user.return_value = mocked_fb_user

        with self.assertRaises(TypeError) as ctx:
            tap_facebook.main_impl()

        self.assertNotIsInstance(ctx.exception, SingerConfigurationError)
        # Unrelated TypeErrors are not retried (not the known SDK bug)
        self.assertEqual(1, mocked_fb_user.get_ad_accounts.call_count)

    def test_succeeds_after_transient_type_error(
            self, mocked_user, mocked_facebook_api, mocked_args, mocked_sleep):
        """If the known SDK bug only occurs transiently, discovery should
        recover and complete normally instead of failing."""
        tap_facebook.CONFIG = {}
        config = {'account_id': 'test', 'access_token': 'test'}
        mocked_args.return_value = Args(config)

        mocked_fb_user = Mock()
        mocked_fb_user.get_ad_accounts = Mock()
        mocked_fb_user.get_ad_accounts.side_effect = [
            TypeError("string indices must be integers, not 'str'"),
            [{'account_id': 'test'}],
        ]
        mocked_user.return_value = mocked_fb_user

        # No properties/discover configured in Args, so a successful account
        # lookup falls through to "No properties were selected" with no error.
        tap_facebook.main_impl()

        self.assertEqual(2, mocked_fb_user.get_ad_accounts.call_count)


if __name__ == '__main__':
    unittest.main()
