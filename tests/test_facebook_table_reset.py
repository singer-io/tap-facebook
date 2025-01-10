import os
import dateutil.parser
import datetime
import base
from base_new_frmwrk import FacebookBaseTest
from tap_tester.base_suite_tests.table_reset_test import TableResetTest
from tap_tester import LOGGER


class FacebookTableResetTest(TableResetTest, FacebookBaseTest):
    """tap-salesforce Table reset test implementation
    Currently tests only the stream with Incremental replication method"""

    is_done = None

    @staticmethod
    def name():
        return "tt_facebook_table_reset"

    # TODO: https://jira.talendforge.org/browse/TDL-26640
    EXCLUDE_STREAMS = {
        'ads_insights_hourly_advertiser',   # TDL-24312, TDL-26640
        'ads_insights_platform_and_device', # TDL-26640
        'ads_insights',                     # TDL-26640
        'ads_insights_age_and_gender',      # TDL-26640
        'ads_insights_country',             # TDL-26640
        'ads_insights_dma',                 # TDL-26640
        'ads_insights_region'               # TDL-26640
    }

    def streams_to_test(self):
        expected_streams = self.expected_metadata().keys()
        self.assert_message = f"JIRA ticket has moved to done, \
                                re-add the applicable stream to the test: {0}"
        assert base.JIRA_CLIENT.get_status_category("TDL-24312") != 'done',\
            self.assert_message.format('ads_insights_hourly_advertiser')
        expected_streams = self.expected_metadata().keys() - {'ads_insights_hourly_advertiser'}
        LOGGER.warn(f"Skipped streams: {'ads_insights_hourly_advertiser'}")

        assert base.JIRA_CLIENT.get_status_category("TDL-26640") != 'done',\
            self.assert_message.format(self.EXCLUDE_STREAMS)
        expected_streams = self.expected_metadata().keys() - self.EXCLUDE_STREAMS
        LOGGER.warn(f"Skipped streams: {self.EXCLUDE_STREAMS}")

        return expected_streams

    @property
    def reset_stream(self):
        return ('ads')


    def calculated_states_by_stream(self, current_state):

        """        The following streams barely make the cut:

        campaigns "2021-02-09T18:17:30.000000Z"
                  "2021-02-09T16:24:58.000000Z"

        adsets    "2021-02-09T18:17:41.000000Z"
                  "2021-02-09T17:10:09.000000Z"

        leads     '2021-04-07T20:09:39+0000',
                  '2021-04-07T20:08:27+0000',
        """
        timedelta_by_stream = {stream: [0,0,0]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_stream_names()}
        timedelta_by_stream['campaigns'] = [0, 1, 0]
        timedelta_by_stream['adsets'] = [0, 1, 0]
        timedelta_by_stream['leads'] = [0, 0 , 1]

        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():
            state_key, state_value = next(iter(state.keys())), next(iter(state.values()))
            state_as_datetime = dateutil.parser.parse(state_value)
            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - datetime.timedelta(days=days, hours=hours, minutes=minutes)

            state_format = '%Y-%m-%dT00:00:00+00:00' if self.is_insight(stream) else '%Y-%m-%dT%H:%M:%S-00:00'
            calculated_state_formatted = datetime.datetime.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = {state_key: calculated_state_formatted}

        return stream_to_calculated_state

    def manipulate_state(self,current_state):
        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(current_state)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        return new_states
