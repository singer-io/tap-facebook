import itertools
import unittest

import tap_facebook
from tap_facebook import AdsInsights

class TestTapFacebook(unittest.TestCase):

    def test_insights_start_dates(self):
        tap_facebook.CONFIG['start_date'] = '2017-01-31'
        insights = AdsInsights(
            name='insights',
            account=None,
            breakdowns=[],
            annotated_schema={'selected': True,
                              'properties': {'something': {'type': 'object'}}})
        params = list(itertools.islice(insights.job_params(), 5))
        self.assertEqual(params[0]['time_ranges'],
                         [{'since': '2017-01-03',
                           'until': '2017-01-31'}])

        self.assertEqual(params[4]['time_ranges'],
                         [{'since': '2017-01-07',
                           'until': '2017-02-04'}])



if __name__ == '__main__':
    unittest.main()
