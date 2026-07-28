import json
import unittest
from unittest.mock import Mock, patch
import tap_facebook
from tap_facebook import FacebookRequestError, TapFacebookException
from tap_facebook import facebook_business
from facebook_business.exceptions import FacebookBadObjectError
from facebook_business import FacebookAdsApi
from facebook_business.api import FacebookResponse
from tap_facebook import AdCreative, AdsInsights
from tap_facebook import call_with_retry
from facebook_business.adobjects.adaccount import AdAccount
import requests
from requests.models import Response
from requests.exceptions import ChunkedEncodingError, Timeout
from urllib3.exceptions import IncompleteRead, ProtocolError

@patch("time.sleep")
class TestAdCreative(unittest.TestCase):
    """A set of unit tests to ensure that requests to get AdCreatives behave
    as expected"""
    def test_retries_on_500(self, mocked_sleep):
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
        self.assertEqual(5, mocked_account.get_ad_creatives.call_count )

    def test_retries_on_503(self, mocked_sleep):
        """`AdCreative.sync.do_request()` calls a `facebook_business` method,
        `get_ad_creatives()`, to make a request to the API. We mock this
        method to raise a `FacebookRequestError` with an `http_status` of
        `503`.

        We expect the tap to retry this request up to 5 times, which is
        the current hard coded `max_tries` value.
        """

        # Create the mock and force the function to throw an error
        mocked_account = Mock()
        mocked_account.get_ad_creatives = Mock()
        mocked_account.get_ad_creatives.side_effect = FacebookRequestError(
            message='',
            request_context={"":Mock()},
            http_status=503,
            http_headers=Mock(),
            body="Service Uavailable"
        )

        # Initialize the object and call `sync()`
        ad_creative_object = AdCreative('', mocked_account, '', '')
        with self.assertRaises(FacebookRequestError):
            ad_creative_object.sync()
        # 5 is the max tries specified in the tap
        self.assertEqual(5, mocked_account.get_ad_creatives.call_count )

    def test_catch_a_type_error(self, mocked_sleep):
        """`AdCreative.sync.do_request()` calls a `facebook_business` method `get_ad_creatives()`.
        We want to mock this to throw a `TypeError("string indices must be integers")` and assert
        that we retry this specific error.
        """
        # Create the mock and force the function to throw an error
        mocked_account = Mock()
        mocked_account.get_ad_creatives = Mock()
        mocked_account.get_ad_creatives.side_effect = TypeError("string indices must be integers")

        # Initialize the object and call `sync()`
        ad_creative_object = AdCreative('', mocked_account, '', '')
        with self.assertRaises(TypeError):
            ad_creative_object.sync()
        # 5 is the max tries specified in the tap
        self.assertEqual(5, mocked_account.get_ad_creatives.call_count )

    def test_retries_and_good_response(self, mocked_sleep):
        """Facebook has a class called `FacebookResponse` and it is created from a `requests.Response`. Some
        `facebook_business` functions depend on calling `FacebookResponse.json()`, which sometimes returns a
        string instead of a dictionary. This leads to a `TypeError("string indices must be integers")` and
        we want to retry these.

        This test will return a "bad" API response the first time the function is called, then a
        "good" response that can be `json.loads()`. We check that the resulting object has our
        expected value in it.

        """
        FacebookAdsApi.init(access_token='access_token')

        expected_value = {"foo":"bar"}

        account = AdAccount('abc_123')
        patcher = patch('requests.Session.request')
        mocked_request = patcher.start()

        mocked_bad_response = Response()
        mocked_bad_response._content = b'images'

        mocked_good_response = Response()

        # Convert our expected value into a JSON string, and then into bytes
        byte_string = json.dumps(expected_value).encode()

        mocked_good_response._content = byte_string


        mocked_request.side_effect = [mocked_bad_response, mocked_good_response]

        ad_creative_object = AdCreative('', account, '', '')
        with self.assertRaises(TypeError):
            ad_creative_object.account.get_ad_creatives(params={})

        list_response = ad_creative_object.account.get_ad_creatives(params={})
        actual_response = list_response.get_one()
        self.assertDictEqual(expected_value, actual_response._json)

        # Clean up tests
        patcher.stop()



@patch("time.sleep")
class TestInsightJobs(unittest.TestCase):
    """A set of unit tests to ensure that requests to get AdsInsights behave
    as expected"""
    def test_retries_on_bad_data(self, mocked_sleep):
        """`AdInsights.run_job()` calls a `facebook_business` method,
        `get_insights()`, to make a request to the API. We mock this
        method to raise a `FacebookBadObjectError`

        We expect the tap to retry this request up to 5 times, which is
        the current hard coded `max_tries` value.
        """

        # Create the mock and force the function to throw an error
        mocked_account = Mock()
        mocked_account.get_insights = Mock()
        mocked_account.get_insights.side_effect = FacebookBadObjectError("Bad data to set object data")

        # Initialize the object and call `sync()`
        ad_creative_object = AdsInsights('', mocked_account, '', '', {}, {})
        with self.assertRaises(FacebookBadObjectError):
            ad_creative_object.run_job({})
        # 5 is the max tries specified in the tap
        self.assertEqual(5, mocked_account.get_insights.call_count )

    def test_retries_on_type_error(self, mocked_sleep):
        """`AdInsights.run_job()` calls a `facebook_business` method, `get_insights()`, to make a request to
        the API. We want to mock this to throw a `TypeError("string indices must be integers")` and
        assert that we retry this specific error.
        """

        # Create the mock and force the function to throw an error
        mocked_account = Mock()
        mocked_account.get_insights = Mock()
        mocked_account.get_insights.side_effect = TypeError("string indices must be integers")

        # Initialize the object and call `sync()`
        ad_creative_object = AdsInsights('', mocked_account, '', '', {}, {})
        with self.assertRaises(TypeError):
            ad_creative_object.run_job({})
        # 5 is the max tries specified in the tap
        self.assertEqual(5, mocked_account.get_insights.call_count )

    def test_retries_and_good_response(self, mocked_sleep):
        """Facebook has a class called `FacebookResponse` and it is created from a `requests.Response`. Some
        `facebook_business` functions depend on calling `FacebookResponse.json()`, which sometimes returns a
        string instead of a dictionary. This leads to a `TypeError("string indices must be integers")` and
        we want to retry these.

        This test will return a "bad" API response the first time the function is called, then a
        "good" response that can be `json.loads()`. We check that the resulting object has our
        expected value in it.
        """
        FacebookAdsApi.init(access_token='access_token')

        expected_value = {"foo":"bar"}

        account = AdAccount('abc_123')
        patcher = patch('requests.Session.request')
        mocked_request = patcher.start()

        mocked_bad_response = Response()
        mocked_bad_response._content = b'images'

        mocked_good_response = Response()

        # Convert our expected value into a JSON string, and then into bytes
        byte_string = json.dumps(expected_value).encode()

        mocked_good_response._content = byte_string

        mocked_request.side_effect = [mocked_bad_response, mocked_good_response]

        ad_creative_object = AdsInsights('', account, '', '', {}, {})
        with self.assertRaises(TypeError):
            ad_creative_object.account.get_insights(params={}, is_async=True)

        actual_response = ad_creative_object.account.get_insights(params={}, is_async=True)

        self.assertDictEqual(expected_value, actual_response._json)

        # Clean up tests
        patcher.stop()


    def test_job_polling_retry(self, mocked_sleep):
        """AdInsights.api_get() polls the job status of an insights job we've requested
        that Facebook generate. This test makes a request with a mock response to
        raise a 400 status error that should be retried.

        We expect the tap to retry this request up to 5 times for each insights job attempted.
        """

        mocked_api_get = Mock()
        mocked_api_get.side_effect = FacebookRequestError(
            message='Unsupported get request; Object does not exist',
            request_context={"":Mock()},
            http_status=400,
            http_headers=Mock(),
            body={"error": {"error_subcode": 33}}
        )
        # Create the mock and force the function to throw an error
        mocked_account = Mock()
        mocked_account.get_insights = Mock()
        mocked_account.get_insights.return_value.api_get = mocked_api_get


        # Initialize the object and call `sync()`
        ad_insights_object = AdsInsights('', mocked_account, '', '', {}, {})
        with self.assertRaises(FacebookRequestError):
            ad_insights_object.run_job({})
        # 5 is the max tries specified in the tap
        self.assertEqual(25, mocked_account.get_insights.return_value.api_get.call_count)
        self.assertEqual(5, mocked_account.get_insights.call_count )



    def test_job_polling_retry_succeeds_eventually(self, mocked_sleep):
        """AdInsights.api_get() polls the job status of an insights job we've requested
        that Facebook generate. This test makes a request with a mock response to
        raise a 400 status error that should be retried.

        We expect the tap to retry this request up to 5 times for each insights job attempted.
        """

        mocked_bad_response = FacebookRequestError(
                message='Unsupported get request; Object does not exist',
                request_context={"":Mock()},
                http_status=400,
                http_headers=Mock(),
                body={"error": {"error_subcode": 33}}
            )

        mocked_good_response = {
            "async_status": "Job Completed",
            "async_percent_completion": 100,
            "id": "2134"
        }

        mocked_api_get = Mock()
        mocked_api_get.side_effect = [
            mocked_bad_response,
            mocked_bad_response,
            mocked_good_response
        ]

        # Create the mock and force the function to throw an error
        mocked_account = Mock()
        mocked_account.get_insights = Mock()
        mocked_account.get_insights.return_value.api_get = mocked_api_get

        # Initialize the object and call `sync()`
        ad_insights_object = AdsInsights('', mocked_account, '', '', {}, {})
        ad_insights_object.run_job({})
        self.assertEqual(3, mocked_account.get_insights.return_value.api_get.call_count)
        self.assertEqual(1, mocked_account.get_insights.call_count)

    def test_job_failed_raises_tap_exception(self, mocked_sleep):
        """AdsInsights.run_job() polls the async job status. When api_get() returns
        async_status == "Job Failed", the tap should immediately raise a
        TapFacebookException containing all v25.0 error fields, without retrying.
        """
        failed_job_response = {
            "async_status": "Job Failed",
            "async_percent_completion": 0,
            "id": "9999",
            "error_code": 2601,
            "error_message": "There was an error running your report.",
            "error_subcode": 1487742,
            "error_user_title": "Report Unavailable",
            "error_user_msg": "Your report could not be run due to a temporary issue.",
        }

        mocked_api_get = Mock()
        mocked_api_get.return_value = failed_job_response

        mocked_account = Mock()
        mocked_account.get_insights = Mock()
        mocked_account.get_insights.return_value.api_get = mocked_api_get

        ad_insights_object = AdsInsights('', mocked_account, '', '', {}, {})

        with self.assertRaises(TapFacebookException) as ctx:
            ad_insights_object.run_job({})

        error_str = str(ctx.exception)
        self.assertIn("9999", error_str)
        self.assertIn("2601", error_str)
        self.assertIn("1487742", error_str)
        self.assertIn("Report Unavailable", error_str)
        self.assertIn("Your report could not be run due to a temporary issue.", error_str)
        self.assertIn("There was an error running your report.", error_str)
        # Should fail immediately — no retries on job failure
        self.assertEqual(1, mocked_api_get.call_count)


@patch("time.sleep")
class TestCallWithRetryRateLimit(unittest.TestCase):
    """`call_with_retry` is the function patched onto `FacebookAdsApi.call`, the single
    choke point every Facebook SDK call passes through -- including page 2+ of every
    paginated stream, since `Cursor.load_next_page()` calls `self._api.call(...)`
    directly and bypasses the per-stream `retry_pattern` decorator entirely.

    Previously `call_with_retry` only retried a narrow summary-param regex match, so a
    rate limit error (Facebook error code 17) hit while paginating would crash instead
    of retrying, even though the exact same error is retried when it happens on page 1
    (via `retry_pattern` on the outer method). These tests assert `call_with_retry`
    itself now retries on error code 17.
    """

    def _rate_limit_error(self):
        return FacebookRequestError(
            message='User request limit reached',
            request_context={"": Mock()},
            http_status=400,
            http_headers=Mock(),
            body={"error": {"code": 17, "message": "User request limit reached"}},
        )

    @patch("tap_facebook.original_call")
    def test_retries_on_rate_limit_code_17_then_succeeds(self, mocked_original_call, mocked_sleep):
        """Two consecutive code 17 rate limit errors followed by a successful response
        should be retried and eventually return the successful result.
        """
        mocked_original_call.side_effect = [
            self._rate_limit_error(),
            self._rate_limit_error(),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        self.assertEqual(3, mocked_original_call.call_count)

    @patch("tap_facebook.original_call")
    def test_gives_up_after_max_tries_on_persistent_rate_limit(self, mocked_original_call, mocked_sleep):
        """If the rate limit error never clears, `call_with_retry` should give up
        after the max_tries configured for the decorator (5), matching the other
        retry-until-giveup tests in this module.
        """
        mocked_original_call.side_effect = self._rate_limit_error()

        with self.assertRaises(FacebookRequestError):
            call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual(5, mocked_original_call.call_count)

    def _chunked_encoding_error(self):
        # Matches the shape of the real production error: a ChunkedEncodingError
        # wrapping an IncompleteRead, raised when the connection is truncated
        # mid-response (e.g. AdCreative.sync_batches's api_batch.execute()).
        return ChunkedEncodingError(
            (
                "Connection broken: IncompleteRead(1316 bytes read, 33197 more expected)",
                IncompleteRead(1316, 33197),
            )
        )

    @patch("tap_facebook.original_call")
    def test_retries_on_chunked_encoding_error_then_succeeds(self, mocked_original_call, mocked_sleep):
        """`ChunkedEncodingError` is a sibling of `ConnectionError` under
        `requests.exceptions.RequestException` (not a subclass of it), and is not a
        `FacebookRequestError` either -- so before this fix, `backoff` on
        `call_with_retry` never even caught it, and it crashed the tap uncaught
        (which is what happened in production on the `adcreative` stream).

        Two transient ChunkedEncodingErrors followed by a successful response
        should be retried and eventually return the successful result.
        """
        mocked_original_call.side_effect = [
            self._chunked_encoding_error(),
            self._chunked_encoding_error(),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        self.assertEqual(3, mocked_original_call.call_count)

    @patch("tap_facebook.original_call")
    def test_gives_up_after_max_tries_on_persistent_chunked_encoding_error(self, mocked_original_call, mocked_sleep):
        """If the connection truncation never clears, `call_with_retry` should give
        up after max_tries (5), re-raising the original `ChunkedEncodingError`
        rather than retrying forever.
        """
        mocked_original_call.side_effect = self._chunked_encoding_error()

        with self.assertRaises(ChunkedEncodingError):
            call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual(5, mocked_original_call.call_count)

    @patch("tap_facebook.original_call")
    def test_retries_on_protocol_error_then_succeeds(self, mocked_original_call, mocked_sleep):
        """`urllib3.exceptions.ProtocolError` is the lower-level equivalent of the
        same kind of connection truncation and should also be retried directly by
        `call_with_retry`.
        """
        mocked_original_call.side_effect = [
            ProtocolError("Connection aborted."),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        self.assertEqual(2, mocked_original_call.call_count)

    @patch("tap_facebook.original_call")
    def test_retries_on_timeout_then_succeeds(self, mocked_original_call, mocked_sleep):
        """`requests.exceptions.Timeout` (and its `ReadTimeout` subclass) is NOT a
        subclass of `ConnectionError` (only `ConnectTimeout` is, via multi-inheritance),
        so a plain read-timeout during SDK-internal pagination bypassed both this
        decorator's exception tuple and crashed uncaught -- even though
        `is_transient_facebook_error` already treated `Timeout` as retryable for the
        per-stream `retry_pattern` decorators. This asserts `call_with_retry` itself
        now retries on `Timeout` too.
        """
        mocked_original_call.side_effect = [
            Timeout("Read timed out."),
            Timeout("Read timed out."),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        self.assertEqual(3, mocked_original_call.call_count)

    @patch("tap_facebook.original_call")
    def test_gives_up_after_max_tries_on_persistent_timeout(self, mocked_original_call, mocked_sleep):
        """If the read-timeout never clears, `call_with_retry` should give up
        after max_tries (5), re-raising the original `Timeout` rather than
        retrying forever.
        """
        mocked_original_call.side_effect = Timeout("Read timed out.")

        with self.assertRaises(Timeout):
            call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual(5, mocked_original_call.call_count)


@patch("time.sleep")
class TestCallWithRetryRateLimitPacing(unittest.TestCase):
    """Tests for the calculated, bounded rate-limit pacing in
    `_rate_limit_aware_expo` / `_reset_wait_seconds_from_facebook_error`.

    Rather than guessing with blind exponential backoff, Meta's own
    `X-Ad-Account-Usage` / `X-Business-Use-Case-Usage` response headers tell us
    exactly how long a rate-limit window has left. These tests assert the wait
    passed to `time.sleep` is derived from that header when present and
    parseable, capped at `RATE_LIMIT_MAX_WAIT_SECONDS`, and that the tap falls
    back cleanly to the pre-existing exponential sequence otherwise.
    """

    def _rate_limit_error(self, http_headers):
        return FacebookRequestError(
            message='User request limit reached',
            request_context={"": Mock()},
            http_status=400,
            http_headers=http_headers,
            body={"error": {"code": 17, "message": "User request limit reached"}},
        )

    @patch("tap_facebook.original_call")
    def test_wait_derived_from_reset_time_duration_header(self, mocked_original_call, mocked_sleep):
        """`X-Ad-Account-Usage`'s `reset_time_duration` (seconds) should drive the
        actual sleep duration directly, instead of the blind exponential sequence.
        """
        usage_header = json.dumps({"acc_id_util_pct": 91.2, "reset_time_duration": 12})
        mocked_original_call.side_effect = [
            self._rate_limit_error({"x-ad-account-usage": usage_header}),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        mocked_sleep.assert_called_once_with(12)

    @patch("tap_facebook.original_call")
    def test_wait_capped_when_reset_time_duration_is_absurd(self, mocked_original_call, mocked_sleep):
        """An absurd or malformed-large `reset_time_duration` (e.g. a bug on Meta's
        side, or a header parsed incorrectly) must never translate into an
        unbounded sleep -- it should be clamped to `RATE_LIMIT_MAX_WAIT_SECONDS`.
        """
        usage_header = json.dumps({"acc_id_util_pct": 100, "reset_time_duration": 999999})
        mocked_original_call.side_effect = [
            self._rate_limit_error({"x-ad-account-usage": usage_header}),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        mocked_sleep.assert_called_once_with(tap_facebook.RATE_LIMIT_MAX_WAIT_SECONDS)
        self.assertEqual(120, tap_facebook.RATE_LIMIT_MAX_WAIT_SECONDS)

    @patch("tap_facebook.original_call")
    def test_wait_derived_from_business_use_case_usage_minutes(self, mocked_original_call, mocked_sleep):
        """`X-Business-Use-Case-Usage`'s `estimated_time_to_regain_access` is
        documented by Meta in MINUTES, so it must be converted to seconds before
        being used as the sleep duration.
        """
        buc_header = json.dumps(
            {
                "123456": [
                    {
                        "type": "ads_insights",
                        "call_count": 95,
                        "estimated_time_to_regain_access": 1,
                    }
                ]
            }
        )
        mocked_original_call.side_effect = [
            self._rate_limit_error({"x-business-use-case-usage": buc_header}),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        # 1 minute -> 60 seconds
        mocked_sleep.assert_called_once_with(60)

    @patch("tap_facebook.original_call")
    def test_falls_back_to_exponential_backoff_when_header_missing(
        self, mocked_original_call, mocked_sleep
    ):
        """When `http_headers()` carries nothing usable, `call_with_retry` must
        not crash trying to parse it, and must fall back to the pre-existing
        exponential sequence (5/10/20/40s) exactly as before this feature.
        """
        mocked_original_call.side_effect = [
            self._rate_limit_error(Mock()),
            self._rate_limit_error(Mock()),
            self._rate_limit_error(Mock()),
            self._rate_limit_error(Mock()),
            self._rate_limit_error(Mock()),
        ]

        with self.assertRaises(FacebookRequestError):
            call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual(5, mocked_original_call.call_count)
        self.assertEqual(
            [5, 10, 20, 40],
            [call_args.args[0] for call_args in mocked_sleep.call_args_list],
        )

    @patch("tap_facebook.original_call")
    def test_falls_back_when_header_present_but_unparseable(
        self, mocked_original_call, mocked_sleep
    ):
        """A header that exists but isn't valid JSON (or has no usable numeric
        field) must degrade to the exponential fallback rather than raising.
        """
        mocked_original_call.side_effect = [
            self._rate_limit_error({"x-ad-account-usage": "not-json"}),
            "success",
        ]

        result = call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual("success", result)
        mocked_sleep.assert_called_once_with(5)

    @patch("tap_facebook.original_call")
    def test_max_tries_still_hard_stop_even_with_valid_header(
        self, mocked_original_call, mocked_sleep
    ):
        """Even when Meta keeps reporting a short, valid reset window on every
        try, `max_tries=5` remains a hard stop -- the pacing feature only
        changes how long each wait is, never how many tries are allowed.
        """
        usage_header = json.dumps({"acc_id_util_pct": 95, "reset_time_duration": 3})
        mocked_original_call.side_effect = self._rate_limit_error(
            {"x-ad-account-usage": usage_header}
        )

        with self.assertRaises(FacebookRequestError):
            call_with_retry(Mock(), "GET", "/some/path")

        self.assertEqual(5, mocked_original_call.call_count)
        self.assertEqual([3, 3, 3, 3], [c.args[0] for c in mocked_sleep.call_args_list])


class TestThrottleIfNearRateLimit(unittest.TestCase):
    """Tests for the optional proactive success-path throttle (`_throttle_if_near_rate_limit`),
    the point-3 nice-to-have: peeking at `acc_id_util_pct` on a SUCCESSFUL response to ease
    off before Meta's rate limit actually trips, rather than only reacting after a code 17.
    """

    def _response_with_usage(self, usage_header):
        response = Mock()
        response.headers.return_value = {"x-ad-account-usage": usage_header}
        return response

    @patch("time.sleep")
    def test_pauses_when_utilization_at_or_above_threshold(self, mocked_sleep):
        usage_header = json.dumps({"acc_id_util_pct": 85, "reset_time_duration": 0})
        response = self._response_with_usage(usage_header)

        tap_facebook._throttle_if_near_rate_limit(response)

        mocked_sleep.assert_called_once_with(
            tap_facebook.RATE_LIMIT_PROACTIVE_SLEEP_SECONDS
        )

    @patch("time.sleep")
    def test_no_pause_when_utilization_below_threshold(self, mocked_sleep):
        usage_header = json.dumps({"acc_id_util_pct": 42, "reset_time_duration": 0})
        response = self._response_with_usage(usage_header)

        tap_facebook._throttle_if_near_rate_limit(response)

        mocked_sleep.assert_not_called()

    @patch("time.sleep")
    def test_no_crash_and_no_pause_when_response_has_no_headers_method(self, mocked_sleep):
        """`call_with_retry`'s success path may receive a bare value in tests
        (e.g. the string "success" used throughout this module) -- this must
        never raise or sleep.
        """
        tap_facebook._throttle_if_near_rate_limit("success")

        mocked_sleep.assert_not_called()

    def _response_with_buc_usage(self, call_count):
        response = Mock()
        buc_header = json.dumps(
            {"123456": [{"call_count": call_count, "estimated_time_to_regain_access": 0}]}
        )
        response.headers.return_value = {"x-business-use-case-usage": buc_header}
        return response

    @patch("time.sleep")
    def test_pauses_based_on_business_use_case_usage_when_ad_account_header_absent(
        self, mocked_sleep
    ):
        """Most of this tap's streams are ads_insights variants, governed by
        the Business Use Case formula rather than the general ad-account one
        -- the proactive throttle must react to X-Business-Use-Case-Usage
        too, not only X-Ad-Account-Usage.
        """
        response = self._response_with_buc_usage(call_count=85)

        tap_facebook._throttle_if_near_rate_limit(response)

        mocked_sleep.assert_called_once_with(
            tap_facebook.RATE_LIMIT_PROACTIVE_SLEEP_SECONDS
        )

    @patch("time.sleep")
    def test_no_pause_when_business_use_case_usage_below_threshold(self, mocked_sleep):
        response = self._response_with_buc_usage(call_count=10)

        tap_facebook._throttle_if_near_rate_limit(response)

        mocked_sleep.assert_not_called()

    @patch("time.sleep")
    def test_longer_pause_when_utilization_crosses_high_threshold(self, mocked_sleep):
        """Escalating tiers: >=95% gets the longer pause, not the short one --
        a large first full sync should back off more the closer it gets to
        the ceiling instead of applying the same tiny pause all the way up
        to 100%.
        """
        usage_header = json.dumps({"acc_id_util_pct": 97, "reset_time_duration": 0})
        response = self._response_with_usage(usage_header)

        tap_facebook._throttle_if_near_rate_limit(response)

        mocked_sleep.assert_called_once_with(
            tap_facebook.RATE_LIMIT_HIGH_PROACTIVE_SLEEP_SECONDS
        )

    @patch("time.sleep")
    def test_uses_whichever_header_reports_higher_utilization(self, mocked_sleep):
        """When both headers are present, the higher of the two utilization
        signals should govern the pause tier -- an insights-heavy call
        pushing Business Use Case usage past 95% must not be masked by a
        comfortably low Ad Account usage number.
        """
        response = Mock()
        response.headers.return_value = {
            "x-ad-account-usage": json.dumps(
                {"acc_id_util_pct": 30, "reset_time_duration": 0}
            ),
            "x-business-use-case-usage": json.dumps(
                {"123456": [{"call_count": 96, "estimated_time_to_regain_access": 0}]}
            ),
        }

        tap_facebook._throttle_if_near_rate_limit(response)

        mocked_sleep.assert_called_once_with(
            tap_facebook.RATE_LIMIT_HIGH_PROACTIVE_SLEEP_SECONDS
        )
