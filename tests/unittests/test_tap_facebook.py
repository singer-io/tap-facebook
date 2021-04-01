import itertools
import unittest
import pendulum
import tap_facebook

from tap_facebook import AdsInsights
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.utils import strftime

class TestAdsInsights(unittest.TestCase):

    def test_insights_start_dates(self):
        insights = AdsInsights(
            name='insights',
            account=None,
            stream_alias="insights",
            options={},
            catalog_entry=CatalogEntry(schema={'properties': {'something': {'type': 'object'}}},
                                       metadata=[{'breadcrumb': ('properties', 'something'),
                                                  'metadata': {'selected' : True}}]),
            state={'bookmarks':{'insights': {'date_start': '2017-01-31'}}})
        params = list(itertools.islice(insights.job_params(), 5))
        self.assertEqual(params[0]['time_ranges'],
                         [{'since': '2017-01-03',
                           'until': '2017-01-03'}])

        self.assertEqual(params[4]['time_ranges'],
                         [{'since': '2017-01-07',
                           'until': '2017-01-07'}])

    def test_insights_job_params_stops(self):
        start_date = pendulum.today().subtract(days=2)
        insights = AdsInsights(
            name='insights',
            account=None,
            stream_alias="insights",
            options={},
            catalog_entry=CatalogEntry(schema={'properties': {'something': {'type': 'object'}}},
                                       metadata=[{'breadcrumb': ('properties', 'something'),
                                                  'metadata': {'selected' : True}}]),
            state={'bookmarks':{'insights': {'date_start': start_date.to_date_string()}}})

        self.assertEqual(31, len(list(insights.job_params())))


class TestPrimaryKeyInclusion(unittest.TestCase):

    def test_primary_keys_automatically_included(self):
        streams = tap_facebook.initialize_streams_for_discovery() # Make this list for the key_properties
        catalog = tap_facebook.discover_schemas()['streams']
        for catalog_entry in catalog:
            streamObject = [stream for stream in streams if stream.name == catalog_entry['stream']][0]
            key_prop_breadcrumbs = {('properties', x) for x in streamObject.key_properties} # Enumerate the breadcrumbs for key properties
            for field in catalog_entry['metadata']: # Check that all key properties are automatic inclusion
                if field['breadcrumb'] in key_prop_breadcrumbs:
                    self.assertEqual(field['metadata']['inclusion'], 'automatic')

class TestGetStreamsToSync(unittest.TestCase):


    def test_getting_streams_to_sync(self):
        catalog_entry= {
            'streams': [
                {
                    'stream': 'adcreative',
                    'tap_stream_id': 'adcreative',
                    'schema': {},
                    'metadata': [{'breadcrumb': (),
                                  'metadata': {'selected': True}}]
                },
                {
                    'stream': 'ads',
                    'tap_stream_id': 'ads',
                    'schema': {},
                    'metadata': [{'breadcrumb': (),
                                  'metadata': {'selected': False}}]
                }
            ]
        }

        catalog = Catalog.from_dict(catalog_entry)

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
