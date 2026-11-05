"""Regression tests for Facebook Insights records that omit a declared key
property (e.g. campaign_id/adset_id, or the hourly breakdown dimension),
which previously caused the destination to reject the record with
"Record for table ads_insights_hourly_advertiser is missing key property
campaign_id".
"""

import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import CatalogEntry

from tap_facebook import AdsInsights, DEFAULT_PK_VALUE, MISSING_KEY_PROPERTY_DEFAULT


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


if __name__ == "__main__":
    unittest.main()
