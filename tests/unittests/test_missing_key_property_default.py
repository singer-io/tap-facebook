"""Regression tests for Facebook Insights records that omit a declared key
property (e.g. campaign_id/adset_id, or the hourly breakdown dimension),
which previously caused the destination to reject the record with
"Record for table ads_insights_hourly_advertiser is missing key property
campaign_id".
"""

import unittest
from unittest.mock import MagicMock, patch

from facebook_business.exceptions import FacebookRequestError
from singer import Transformer
from singer.catalog import CatalogEntry

import tap_facebook
from tap_facebook import (
    AdsInsights,
    DEFAULT_PK_VALUE,
    MISSING_KEY_PROPERTY_DEFAULT,
    transform_date_hook,
)


def _unresolvable_ad_error():
    """A FacebookRequestError shaped like what _fetch_ad_parent_ids would raise for an ad_id
    that can no longer be resolved (e.g. genuinely deleted), so the caller falls back to
    defaulting the missing propert(y/ies) instead."""
    call = {'method': 'GET', 'relative_url': 'fields=campaign_id,adset_id'}
    return FacebookRequestError(
        'msg', call, 400, {}, '{"error": {"code": 100, "error_subcode": 33}}'
    )


class FakeInsightResult:
    """Stands in for the facebook_business AdsInsights result object, whose
    `export_all_data()` returns the record dict."""

    def __init__(self, data):
        self._data = data

    def export_all_data(self):
        return dict(self._data)


class FakeJob:
    def __init__(self, records):
        self._records = records

    def get_result(self):
        return [FakeInsightResult(rec) for rec in self._records]


@patch("time.sleep")
class TestMissingKeyPropertyDefault(unittest.TestCase):
    fake_catalog_entry = CatalogEntry(
        schema={"properties": {"something": {"type": "object"}}},
        metadata=[{"breadcrumb": ("properties", "something"), "metadata": {"selected": True}}],
    )

    def setUp(self):
        tap_facebook._RESOLVED_AD_PARENT_IDS.clear()
        self.addCleanup(tap_facebook._RESOLVED_AD_PARENT_IDS.clear)
        patcher = patch("tap_facebook._fetch_ad_parent_ids")
        self.mock_fetch_ad_parent_ids = patcher.start()
        self.addCleanup(patcher.stop)
        # Default: simulate the ad itself also being unresolvable, so every pre-existing test
        # below continues to exercise (and verify) the sentinel-defaulting fallback behavior
        # unless a test explicitly overrides this to exercise the recovery path instead.
        self.mock_fetch_ad_parent_ids.side_effect = _unresolvable_ad_error()

    def _make_stream(self, name, options):
        return AdsInsights(
            name=name,
            account=None,
            stream_alias=name,
            options=options,
            catalog_entry=self.fake_catalog_entry,
            state={"bookmarks": {name: {"date_start": "2026-10-01"}}},
        )

    def _iterate(self, stream, mock_job_params, mock_run_job, records):
        mock_job_params.return_value = iter(
            [{"time_ranges": [{"since": "2026-10-02", "until": "2026-10-02"}]}]
        )
        mock_run_job.return_value = FakeJob(records)
        return [msg for msg in stream if "record" in msg]

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_missing_campaign_id_defaults_instead_of_dropping_record(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """campaign_id missing (e.g. owning campaign since deleted/archived) is
        defaulted, not dropped, and every other field is preserved untouched."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            "adset_id": "456",
            # campaign_id intentionally omitted
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
            "impressions": "10",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        self.assertEqual(1, len(messages))
        written = messages[0]["record"]
        self.assertEqual(MISSING_KEY_PROPERTY_DEFAULT, written["campaign_id"])
        self.assertEqual("123", written["ad_id"])
        self.assertEqual("456", written["adset_id"])
        self.assertEqual("10", written["impressions"])

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_missing_hourly_breakdown_still_uses_its_specific_default(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """The pre-existing hourly-breakdown-dimension workaround keeps using
        its own specific default value (not the generic fallback)."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            "adset_id": "456",
            "campaign_id": "789",
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            # hourly breakdown field intentionally omitted (zero impressions all day)
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        written = messages[0]["record"]
        self.assertEqual(
            DEFAULT_PK_VALUE, written["hourly_stats_aggregated_by_advertiser_time_zone"]
        )

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_no_missing_key_properties_leaves_record_untouched(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """A fully-populated record is passed through unmodified."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            "adset_id": "456",
            "campaign_id": "789",
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "01:00:00 - 01:59:59",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        self.assertEqual(record, messages[0]["record"])

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_missing_key_property_on_non_hourly_stream_uses_generic_default(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """The fix isn't hard-coded to ads_insights_hourly_advertiser; any
        AdsInsights-derived stream missing a key property gets it defaulted."""
        stream = self._make_stream("ads_insights_country", {"primary-keys": ["country"]})
        record = {
            "ad_id": "123",
            "adset_id": "456",
            # campaign_id intentionally omitted
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "country": "US",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        written = messages[0]["record"]
        self.assertEqual(MISSING_KEY_PROPERTY_DEFAULT, written["campaign_id"])
        self.assertEqual("US", written["country"])

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_missing_date_start_defaults_to_requests_since_date(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """date_start is a special case: unlike every other key property, which falls back to
        MISSING_KEY_PROPERTY_DEFAULT, a missing date_start is defaulted to the request's own
        `since` date (set up by _iterate as "2026-10-02"). Verify that value is used verbatim,
        and that it survives schema transformation (format: date-time) as a valid, correctly
        dated timestamp - not a sentinel string dateutil can't parse."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            "adset_id": "456",
            "campaign_id": "789",
            # date_start intentionally omitted
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        written = messages[0]["record"]
        self.assertEqual("2026-10-02", written["date_start"])
        self.assertNotEqual(MISSING_KEY_PROPERTY_DEFAULT, written["date_start"])

        schema = {
            "type": "object",
            "properties": {"date_start": {"type": ["null", "string"], "format": "date-time"}},
        }
        with Transformer(pre_hook=transform_date_hook) as transformer:
            transformed = transformer.transform(written, schema)
        self.assertEqual("2026-10-02T00:00:00.000000Z", transformed["date_start"])

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_colliding_composite_keys_are_disambiguated_not_overwritten(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """Two distinct records that are both missing campaign_id, with otherwise-identical key
        fields (same ad_id/date_start/hourly bucket), would resolve to an identical composite key
        once both default campaign_id to the same shared sentinel - letting a destination upsert
        silently overwrite one of them despite their different impressions. Verify both are instead
        emitted with distinct composite keys, with the first-seen record keeping the plain sentinel
        (so existing single-record behavior/tests are unaffected)."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record_a = {
            "ad_id": "123",
            "adset_id": "456",
            # campaign_id intentionally omitted on both records
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
            "impressions": "0",
        }
        record_b = dict(record_a, impressions="37")

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record_a, record_b])

        self.assertEqual(2, len(messages))
        written_a, written_b = messages[0]["record"], messages[1]["record"]

        # The first record seen at a given resolved key keeps the plain shared sentinel.
        self.assertEqual(MISSING_KEY_PROPERTY_DEFAULT, written_a["campaign_id"])
        # The second, colliding record is disambiguated so it no longer shares a composite key.
        self.assertNotEqual(written_a["campaign_id"], written_b["campaign_id"])
        self.assertTrue(written_b["campaign_id"].startswith(MISSING_KEY_PROPERTY_DEFAULT))

        key_properties = stream.key_properties
        composite_key_a = tuple(written_a[kp] for kp in key_properties)
        composite_key_b = tuple(written_b[kp] for kp in key_properties)
        self.assertNotEqual(composite_key_a, composite_key_b)

        # Non-key data must still be preserved untouched on both records.
        self.assertEqual("0", written_a["impressions"])
        self.assertEqual("37", written_b["impressions"])

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_three_colliding_records_all_get_distinct_composite_keys(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """More than two records colliding on the same resolved key are all pairwise disambiguated,
        not just the second one relative to the first."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        records = [
            {
                "ad_id": "123",
                "adset_id": "456",
                # campaign_id intentionally omitted on every record
                "date_start": "2026-10-02",
                "date_stop": "2026-10-02",
                "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
                "impressions": str(impressions),
            }
            for impressions in (0, 5, 9)
        ]

        messages = self._iterate(stream, mock_job_params, mock_run_job, records)

        key_properties = stream.key_properties
        composite_keys = [tuple(msg["record"][kp] for kp in key_properties) for msg in messages]
        self.assertEqual(3, len(composite_keys))
        self.assertEqual(len(composite_keys), len(set(composite_keys)))

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_genuinely_duplicate_records_are_not_artificially_split(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """Two records that are truly full duplicates (identical content, not just identical
        declared key fields) have no distinguishing data to derive a stable identity from, so
        they're allowed to keep sharing a composite key - they're legitimately mergeable."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            "adset_id": "456",
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
            "impressions": "0",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [dict(record), dict(record)])

        self.assertEqual(messages[0]["record"], messages[1]["record"])

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_missing_campaign_id_and_adset_id_recovered_via_ad_lookup(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """When Insights omits campaign_id/adset_id but still returns ad_id, a direct (mocked)
        Ad lookup that successfully resolves both is used instead of the "unknown" sentinel."""
        self.mock_fetch_ad_parent_ids.side_effect = None
        self.mock_fetch_ad_parent_ids.return_value = {"campaign_id": "999", "adset_id": "888"}
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            # campaign_id and adset_id intentionally omitted
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
            "impressions": "10",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        written = messages[0]["record"]
        self.assertEqual("999", written["campaign_id"])
        self.assertEqual("888", written["adset_id"])
        self.assertNotEqual(MISSING_KEY_PROPERTY_DEFAULT, written["campaign_id"])
        self.assertNotEqual(MISSING_KEY_PROPERTY_DEFAULT, written["adset_id"])
        self.mock_fetch_ad_parent_ids.assert_called_once_with("123")

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_ad_lookup_result_is_cached_per_ad_id(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """The same ad_id recurring across multiple rows (as it does across every day in a real
        sync window) triggers only one ad lookup, not one per row."""
        self.mock_fetch_ad_parent_ids.side_effect = None
        self.mock_fetch_ad_parent_ids.return_value = {"campaign_id": "999", "adset_id": "888"}
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            # campaign_id and adset_id intentionally omitted
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [dict(record), dict(record)])

        for message in messages:
            self.assertEqual("999", message["record"]["campaign_id"])
            self.assertEqual("888", message["record"]["adset_id"])
        self.mock_fetch_ad_parent_ids.assert_called_once_with("123")

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_ad_lookup_failure_falls_back_to_default(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """If the owning ad is itself unresolvable (setUp's default mock behavior), the existing
        sentinel-defaulting fallback still applies - no behavior regression for genuinely gone
        ads."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            # campaign_id and adset_id intentionally omitted
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        written = messages[0]["record"]
        self.assertEqual(MISSING_KEY_PROPERTY_DEFAULT, written["campaign_id"])
        self.assertEqual(MISSING_KEY_PROPERTY_DEFAULT, written["adset_id"])
        self.mock_fetch_ad_parent_ids.assert_called_once_with("123")

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_ad_lookup_skipped_when_ad_id_also_missing(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """There's nothing to key the ad lookup off of if ad_id is also missing from the row, so
        it must not be attempted - defaulting applies directly."""
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            # ad_id and campaign_id both intentionally omitted
            "adset_id": "456",
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        written = messages[0]["record"]
        self.assertEqual(MISSING_KEY_PROPERTY_DEFAULT, written["campaign_id"])
        self.mock_fetch_ad_parent_ids.assert_not_called()

    @patch.object(AdsInsights, "run_job")
    @patch.object(AdsInsights, "job_params")
    def test_ad_lookup_only_fills_the_properties_actually_missing(
        self, mock_job_params, mock_run_job, _mock_sleep
    ):
        """If only campaign_id is missing (adset_id already present), a resolved adset_id from the
        ad lookup must not overwrite the value Insights already returned."""
        self.mock_fetch_ad_parent_ids.side_effect = None
        self.mock_fetch_ad_parent_ids.return_value = {"campaign_id": "999", "adset_id": "OTHER"}
        stream = self._make_stream(
            "ads_insights_hourly_advertiser",
            {"primary-keys": ["hourly_stats_aggregated_by_advertiser_time_zone"]},
        )
        record = {
            "ad_id": "123",
            "adset_id": "456",
            # campaign_id intentionally omitted
            "date_start": "2026-10-02",
            "date_stop": "2026-10-02",
            "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
        }

        messages = self._iterate(stream, mock_job_params, mock_run_job, [record])

        written = messages[0]["record"]
        self.assertEqual("999", written["campaign_id"])
        self.assertEqual("456", written["adset_id"])


if __name__ == "__main__":
    unittest.main()


