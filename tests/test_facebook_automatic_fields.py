import os
import unittest
from functools import reduce

from tap_tester import connections, menagerie, runner

from base import FacebookBaseTest


class FacebookAutomaticFields(FacebookBaseTest): # TODO Fix assertions
    def name(self):
        return "tap_tester_facebook_automatic_fields"

    def expected_check_streams(self):
        return {
            'ads',
            'adcreative',
            'adsets',
            'campaigns',
            'ads_insights',
            'ads_insights_age_and_gender',
            'ads_insights_country',
            'ads_insights_platform_and_device',
            'ads_insights_region',
            'ads_insights_dma',
        }

    def expected_sync_streams(self):
        return {
            "ads",
            "adcreative",
            "adsets",
            "campaigns",
            "ads_insights",
            "ads_insights_age_and_gender",
            "ads_insights_country",
            "ads_insights_platform_and_device",
            "ads_insights_region",
            "ads_insights_dma",
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # select all catalogs
        for c in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            for k in self.expected_primary_keys()[c['stream_name']]:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)
                print("Validating inclusion on {}: {}".format(c['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')
            connections.select_catalog_via_metadata(conn_id, c, catalog_entry)

        # clear state
        menagerie.set_state(conn_id, {})

        # run a sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()
        for stream_name, data in synced_records.items():
            record_messages = [set(row['data'].keys()) for row in data['messages']]
            for record_keys in record_messages:
                # The symmetric difference should be empty
                self.assertEqual(record_keys, self.expected_automatic_fields().get(stream_name, set()))
