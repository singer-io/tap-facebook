import os

from tap_tester import runner, connections, menagerie

from base import FacebookBaseTest

class FacebookInvalidAttributionWindow(FacebookBaseTest):

    @staticmethod
    def name():
        return "tap_tester_facebook_invalid_attribution_window"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'account_id': os.getenv('TAP_FACEBOOK_ACCOUNT_ID'),
            'start_date' : '2019-07-24T00:00:00Z',
            'end_date' : '2019-07-26T00:00:00Z',
            'insights_buffer_days': '10'
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):
        """
            Test to verify that the error is raise when passing attribution window other than 1, 7 or 28
        """
        conn_id = connections.ensure_connection(self)
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        discovery_error_message = exit_status.get('discovery_error_message')
        self.assertEquals(discovery_error_message, "The attribution window must be 1, 7 or 28.")
