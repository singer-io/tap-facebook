import itertools
import unittest
import pendulum
import tap_facebook
from tap_facebook import AdsInsights, State

class TestState(unittest.TestCase):

    def test_get_with_no_initial_state(self):
        state = State('2017-03-01', None)
        self.assertEqual(state.get('foo'), '2017-03-01')

    def test_advance_cant_move_backwards(self):
        state = State('2017-03-01', None)
        state.advance('foo', '2017-02-01')
        self.assertEqual(state.get('foo'), '2017-03-01')

    def test_advance_can_move_forward(self):
        state = State('2017-03-01', None)
        state.advance('foo', '2017-04-01')
        self.assertEqual(state.get('foo'), '2017-04-01')

    def test_advance_returns_message(self):
        state = State('2017-03-01', None)
        self.assertEqual(state.advance('foo', '2017-04-01')['foo'],
                         '2017-04-01')

class TestAdsInsights(unittest.TestCase):

    def test_insights_start_dates(self):
        insights = AdsInsights(
            name='insights',
            account=None,
            breakdowns=[],
            annotated_schema={'selected': True,
                              'properties': {'something': {'type': 'object'}}},
            state=tap_facebook.State('2017-01-31', None))
        params = list(itertools.islice(insights.job_params(), 5))
        self.assertEqual(params[0]['time_ranges'],
                         [{'since': '2017-01-03',
                           'until': '2017-01-31'}])

        self.assertEqual(params[4]['time_ranges'],
                         [{'since': '2017-01-07',
                           'until': '2017-02-04'}])

    def test_insights_job_params_stops(self):
        start_date = tap_facebook.TODAY.subtract(days=2)
        insights = AdsInsights(
            name='insights',
            account=None,
            breakdowns=[],
            annotated_schema={'selected': True,
                              'properties': {'something': {'type': 'object'}}},
            state=tap_facebook.State(start_date.to_date_string(), None))

        self.assertEqual(3, len(list(insights.job_params())))


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


    def test_foo(self):
        annotated_schemas = {
            'streams': {
                'adcreative': {'selected': True},
                'ads': {'selected': False}
            }
        }

        streams_to_sync = tap_facebook.get_streams_to_sync(None, annotated_schemas, None)
        names_to_sync = [stream.name for stream in streams_to_sync]
        self.assertEqual(['adcreative'], names_to_sync)


if __name__ == '__main__':
    unittest.main()
