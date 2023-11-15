import os
from datetime import datetime as dt
from datetime import timezone as tz
from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class FacebookBaseTest(BaseCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).

    Insights Test Data by Date Ranges
        "ads_insights":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_age_and_gender":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_country":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_platform_and_device":
          "2019-08-02T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_region":
          "2019-08-03T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_dma":
          "2019-08-03T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"
        "ads_insights_hourly_advertiser":
          "2019-08-03T00:00:00.000000Z" -> "2019-10-30T00:00:00.000000Z"
          "2021-04-07T00:00:00.000000Z" -> "2021-04-08T00:00:00.000000Z"

    """
    FULL_TABLE = "FULL_TABLE"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT00:00:00+00:00"

    start_date = "2021-04-07T00:00:00Z"
    end_date = ""

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-facebook"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.facebook"

    def get_properties(self):
        """Configuration properties required for the tap."""
        return {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : self.start_date,
            'end_date': '2021-04-09T00:00:00Z',
            'insights_buffer_days': '1',
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {'access_token': os.getenv('TAP_FACEBOOK_ACCESS_TOKEN')}
    @staticmethod
    def expected_metadata():
        """The expected streams and metadata about the streams"""
        return {
            "ads": {
                BaseCase.PRIMARY_KEYS: {"id", "updated_time"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updated_time"},
                BaseCase.API_LIMIT: 100
            },
            "adcreative": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.FULL_TABLE,
                BaseCase.API_LIMIT: 100
            },
            "adsets": {
                BaseCase.PRIMARY_KEYS: {"id", "updated_time"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updated_time"},
                BaseCase.API_LIMIT: 100
            },
            "campaigns": {
                BaseCase.PRIMARY_KEYS: {"id", },
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updated_time"},
                BaseCase.API_LIMIT: 100
            },
            "ads_insights": {
                BaseCase.PRIMARY_KEYS: {"campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"},
                BaseCase.API_LIMIT: 100
            },
            "ads_insights_age_and_gender": {
                BaseCase.PRIMARY_KEYS: {
                    "campaign_id", "adset_id", "ad_id", "date_start", "age", "gender"
                },
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_country": {
                BaseCase.PRIMARY_KEYS: {"campaign_id", "adset_id", "ad_id", "date_start",
                                        "country"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_platform_and_device": {
                BaseCase.PRIMARY_KEYS: {"campaign_id", "adset_id", "ad_id", "date_start",
                                        "publisher_platform", "platform_position",
                                        "impression_device"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_region": {
                BaseCase.PRIMARY_KEYS: {"region", "campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_dma": {
                BaseCase.PRIMARY_KEYS: {"dma", "campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            "ads_insights_hourly_advertiser": {
                BaseCase.PRIMARY_KEYS: {"hourly_stats_aggregated_by_advertiser_time_zone",
                                        "campaign_id", "adset_id", "ad_id", "date_start"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_start"}
            },
            # "leads": {
            #     BaseCase.PRIMARY_KEYS: {"id"},
            #     BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
            #     BaseCase.REPLICATION_KEYS: {"created_time"}
            # },
        }

    def set_replication_methods(self, conn_id, catalogs, replication_methods):

        replication_keys = self.expected_replication_keys()
        for catalog in catalogs:
            replication_method = replication_methods.get(catalog['stream_name'])
            annt=menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            if replication_method == self.INCREMENTAL:
                replication_key = list(replication_keys.get(catalog['stream_name']))[0]
                replication_md = [{ "breadcrumb": [], "metadata":{ "selected" : True}}]
            else:
                replication_md = [{ "breadcrumb": [], "metadata": { "selected": None}}]
            connections.set_non_discoverable_metadata(conn_id,
                                                      catalog,
                                                      menagerie.get_annotated_schema(
                                                          conn_id,
                                                          catalog['stream_id']),
                                                      replication_md)

    @classmethod
    def setUpClass(cls,logging="Ensuring environment variables are sourced."):
        super().setUpClass(logging=logging)
        missing_envs = [x for x in [os.getenv('TAP_FACEBOOK_ACCESS_TOKEN'),
                                    os.getenv('TAP_FACEBOOK_ACCOUNT_ID')] if x is None]
        if len(missing_envs) != 0:
            raise Exception("set environment variables")


    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    @staticmethod
    def is_insight(stream):
        return stream.startswith('ads_insights')
