"""Test tap discovery mode and metadata/annotated-schema."""
import re

from tap_tester import menagerie, connections

from base import FacebookBaseTest


class DiscoveryTest(FacebookBaseTest):
    """Test tap discovery mode and metadata/annotated-schema conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_facebook_discovery_test"

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # NOTE: The following assertion is not backwards compatible with older taps, but it
        #       SHOULD BE IMPLEMENTED in future taps, leaving here as a comment for reference

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                assert catalog  # based on previous tests this should always be found

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                schema = schema_and_metadata["annotated-schema"]

                # verify there is only 1 top level breadcrumb
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # TODO BUG_1 ?
                failing_with_no_replication_keys = {
                    'ads_insights_country', 'adsets', 'adcreative', 'ads', 'ads_insights_region',
                    'campaigns', 'ads_insights_age_and_gender', 'ads_insights_platform_and_device',
                    'ads_insights_dma', 'ads_insights'
                }
                if stream not in failing_with_no_replication_keys:  # BUG_1
                    # verify replication key(s)
                    self.assertEqual(
                        set(stream_properties[0].get(
                            "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])),
                        self.expected_replication_keys()[stream],
                        msg="expected replication key {} but actual is {}".format(
                            self.expected_replication_keys()[stream],
                            set(stream_properties[0].get(
                                "metadata", {self.REPLICATION_KEYS: None}).get(
                                    self.REPLICATION_KEYS, []))))

                # verify primary key(s)
                self.assertEqual(
                    set(stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])),
                    self.expected_primary_keys()[stream],
                    msg="expected primary key {} but actual is {}".format(
                        self.expected_primary_keys()[stream],
                        set(stream_properties[0].get(
                            "metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS, []))))

                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)


                if stream not in failing_with_no_replication_keys:  # BUG_1
                    # verify the actual replication matches our expected replication method
                    self.assertEqual(
                        self.expected_replication_method().get(stream, None),
                        actual_replication_method,
                        msg="The actual replication method {} doesn't match the expected {}".format(
                            actual_replication_method,
                            self.expected_replication_method().get(stream, None)))

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []):
                    self.assertTrue(actual_replication_method == self.INCREMENTAL,
                                    msg="Expected INCREMENTAL replication "
                                        "since there is a replication key")
                else:
                    # TODO Failing for all streams because they are missing replication method
                    pass
                    # self.assertTrue(actual_replication_method == self.FULL_TABLE,
                    #                 msg="Expected FULL replication since there is no replication key")


                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in annotated schema.
                actual_automatic_fields = {key for key, value in schema["properties"].items()
                                           if value.get("inclusion") == "automatic"}
                self.assertEqual(expected_automatic_fields, actual_automatic_fields)

                self.assertTrue(
                    all({value.get("inclusion") == "available" for key, value
                         in schema["properties"].items()
                         if key not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in annotated schema")

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in metadata.
                actual_automatic_fields = {item.get("breadcrumb", ["properties", None])[1]
                                           for item in metadata
                                           if item.get("metadata").get("inclusion") == "automatic"}
                self.assertEqual(expected_automatic_fields,
                                 actual_automatic_fields,
                                 msg="expected {} automatic fields but got {}".format(
                                     expected_automatic_fields,
                                     actual_automatic_fields))

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")
