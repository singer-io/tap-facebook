import itertools
import unittest
import pendulum
import tap_facebook

from tap_facebook import AdsInsights
from singer.catalog import Catalog
from singer.schema import Schema
from singer.utils import strftime

class TestAdsInsights(unittest.TestCase):

    def test_insights_start_dates(self):
        insights = AdsInsights(
            name='insights',
            account=None,
            stream_alias="insights",
            options={},
            annotated_schema=Schema.from_dict({'selected': True,
                              'properties': {'something': {'type': 'object'}}}),
            state={'bookmarks':{'insights': {'date_start': '2017-01-31'}}})
        params = list(itertools.islice(insights.job_params(), 5))
        self.assertEqual(params[0]['time_ranges'],
                         [{'since': '2017-01-03',
                           'until': '2017-01-03'}])

        self.assertEqual(params[4]['time_ranges'],
                         [{'since': '2017-01-07',
                           'until': '2017-01-07'}])

    def test_insights_job_params_stops(self):
        start_date = tap_facebook.TODAY.subtract(days=2)
        insights = AdsInsights(
            name='insights',
            account=None,
            stream_alias="insights",
            options={},
            annotated_schema=Schema.from_dict({'selected': True,
                              'properties': {'something': {'type': 'object'}}}),
            state={'bookmarks':{'insights': {'date_start': start_date.to_date_string()}}})

        self.assertEqual(31, len(list(insights.job_params())))


class TestPrimaryKeyInclusion(unittest.TestCase):

    def test_primary_keys_automatically_included(self):
        streams = tap_facebook.initialize_streams_for_discovery()
        for stream in streams:
            schema = tap_facebook.load_schema(stream)
            for prop in stream.key_properties:
                if prop not in schema['properties']:
                    self.fail('Stream {} key property {} is not defined'.format(
                              stream.name, prop))
                self.assertEqual(
                    schema['properties'][prop]['inclusion'],
                    'automatic',
                    'Stream {} key property {} should be included automatically'.format(
                        stream.name, prop))

            # Get the schema
            # Find the primary key property defs
            # Assert that all of their "inclusion" attrs are "automatic"


class TestGetStreamsToSync(unittest.TestCase):


    def test_getting_streams_to_sync(self):
        annotated_schemas = {
            'streams': [
                {
                    'stream': 'adcreative',
                    'tap_stream_id': 'adcreative',
                    'schema': {'selected': True}
                },
                {
                    'stream': 'ads',
                    'tap_stream_id': 'ads',
                    'schema': {'selected': False}
                }
            ]
        }

        catalog = Catalog.from_dict(annotated_schemas)

        streams_to_sync = tap_facebook.get_streams_to_sync(None, catalog, None)
        names_to_sync = [stream.name for stream in streams_to_sync]
        self.assertEqual(['adcreative'], names_to_sync)

class TestDateTimeParsing(unittest.TestCase):

    def test(self):
        dt       = '2016-07-07T15:46:48-0400'
        expected = '2016-07-07T19:46:48.000000Z'
        self.assertEqual(
            tap_facebook.transform_datetime_string(dt),
            expected)


if __name__ == '__main__':
    unittest.main()
