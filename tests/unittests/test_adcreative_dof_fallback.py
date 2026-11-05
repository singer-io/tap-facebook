import unittest
from unittest import mock

from facebook_business.exceptions import FacebookRequestError

from tap_facebook import (AdCreative, adcreative_batch_record_failure,
                          creative_id_from_response, is_transient_batch_error,
                          ADCREATIVE_BATCH_ERROR_PAUSE_SECONDS)


def make_response(creative_id, code, subcode):
    call = {'method': 'GET', 'relative_url': '{}?fields=id,object_story_spec'.format(creative_id)}
    error = FacebookRequestError('msg', call, 400, {}, '{"error": {"code": %d, "error_subcode": %d}}' % (code, subcode))
    response = mock.Mock(_call=call)
    response.error.return_value = error
    return response


class TestAdCreativeDofFallback(unittest.TestCase):
    def test_creative_id_from_response(self):
        self.assertEqual(creative_id_from_response(make_response('123', 100, 2446211)), '123')

    def test_dof_error_is_collected_once(self):
        ids = []
        adcreative_batch_record_failure(make_response('123', 100, 2446211), ids)
        adcreative_batch_record_failure(make_response('123', 100, 2446211), ids)
        self.assertEqual(ids, ['123'])

    def test_other_error_raises(self):
        with self.assertRaises(FacebookRequestError):
            adcreative_batch_record_failure(make_response('123', 100, 1), [])

    @mock.patch('tap_facebook.singer.resolve_schema_references')
    @mock.patch('tap_facebook.API')
    def test_retry_without_object_story_spec(self, mock_api, _):
        stream = AdCreative('adcreative', None, mock.Mock(schema=mock.Mock(to_dict=lambda: {})), None) \
            if False else AdCreative.__new__(AdCreative)
        stream.catalog_entry = mock.Mock()
        stream.fields = lambda: {'id', 'object_story_spec', 'name'}
        with mock.patch('tap_facebook.adcreative.AdCreative') as creative_cls:
            stream.sync_dof_creatives(['1', '2'])
        fields = creative_cls.return_value.api_get.call_args.kwargs['fields']
        self.assertNotIn('object_story_spec', fields)
        self.assertEqual(creative_cls.return_value.api_get.call_count, 2)
        mock_api.new_batch.return_value.execute.assert_called_once()


class TestAdCreativePerBatchRetry(unittest.TestCase):
    @mock.patch('time.sleep')
    @mock.patch('tap_facebook.singer.resolve_schema_references')
    @mock.patch('tap_facebook.API')
    def test_only_failing_batch_is_retried(self, mock_api, _, __):
        stream = AdCreative.__new__(AdCreative)
        stream.catalog_entry = mock.Mock()
        stream.fields = lambda: {'id'}
        batch_ok, batch_fail = mock.Mock(), mock.Mock()
        batch_fail.execute.side_effect = [AttributeError('x'), None]
        # batch 1 ok, batch 2 fails once then succeeds
        mock_api.new_batch.side_effect = [batch_ok, batch_fail, batch_fail]
        stream.sync_batches([mock.Mock() for _ in range(51 + 49)])
        self.assertEqual(batch_ok.execute.call_count, 1)
        self.assertEqual(batch_fail.execute.call_count, 2)


class TestAdCreativeBatchErrorCooldown(unittest.TestCase):
    def test_transient_500_error_is_transient(self):
        error = FacebookRequestError('msg', {'relative_url': 'batch'}, 500, {}, '{}')
        self.assertTrue(is_transient_batch_error(error))

    def test_transient_503_error_is_transient(self):
        error = FacebookRequestError('msg', {'relative_url': 'batch'}, 503, {}, '{}')
        self.assertTrue(is_transient_batch_error(error))

    def test_non_transient_4xx_error_is_not_transient(self):
        error = FacebookRequestError('msg', {'relative_url': 'batch'}, 400,
                                      {}, '{"error": {"code": 190, "error_subcode": 0}}')
        self.assertFalse(is_transient_batch_error(error))

    def test_non_facebook_error_is_not_transient(self):
        self.assertFalse(is_transient_batch_error(AttributeError('boom')))

    @mock.patch('time.sleep')
    @mock.patch('tap_facebook.singer.resolve_schema_references')
    @mock.patch('tap_facebook.API')
    def test_execute_batch_pauses_before_retrying_a_transient_error(self, mock_api, _, mock_sleep):
        """A transient (500) batch failure should sleep for the dedicated cooldown ahead of each
        retry, so a multi-minute Facebook-side blip doesn't exhaust the retry budget and fail the
        whole stream. The cooldown must NOT fire on the final (5th) attempt that gives up, since no
        further request follows it - that would just add 5 minutes to failure reporting for nothing."""
        stream = AdCreative.__new__(AdCreative)
        stream.catalog_entry = mock.Mock()
        stream.fields = lambda: {'id'}

        error = FacebookRequestError('msg', {'relative_url': 'batch'}, 500, {}, '{}')
        batch = mock.Mock()
        batch.execute.side_effect = error
        mock_api.new_batch.return_value = batch

        obj = mock.Mock()
        obj.get.return_value = None
        with self.assertRaises(FacebookRequestError):
            stream.execute_batch([obj], [], {}, mock.Mock())

        # Exhausted all 5 attempts (as configured on execute_batch), but only the 4 that are
        # followed by a retry pause for the cooldown - not the 5th, which gives up.
        self.assertEqual(batch.execute.call_count, 5)
        self.assertEqual(
            mock_sleep.call_args_list.count(mock.call(ADCREATIVE_BATCH_ERROR_PAUSE_SECONDS)), 4)

    @mock.patch('time.sleep')
    @mock.patch('tap_facebook.singer.resolve_schema_references')
    @mock.patch('tap_facebook.API')
    def test_execute_batch_does_not_pause_for_non_transient_error(self, mock_api, _, mock_sleep):
        """A non-transient (permanent) batch-level error should not trigger the extra cooldown."""
        stream = AdCreative.__new__(AdCreative)
        stream.catalog_entry = mock.Mock()
        stream.fields = lambda: {'id'}

        error = FacebookRequestError('msg', {'relative_url': 'batch'}, 400,
                                      {}, '{"error": {"code": 190, "error_subcode": 0}}')
        batch = mock.Mock()
        batch.execute.side_effect = error
        mock_api.new_batch.return_value = batch

        obj = mock.Mock()
        obj.get.return_value = None
        with self.assertRaises(FacebookRequestError):
            stream.execute_batch([obj], [], {}, mock.Mock())

        self.assertEqual(batch.execute.call_count, 1)
        self.assertNotIn(mock.call(ADCREATIVE_BATCH_ERROR_PAUSE_SECONDS), mock_sleep.call_args_list)

    @mock.patch('time.sleep')
    @mock.patch('tap_facebook.singer.resolve_schema_references')
    @mock.patch('tap_facebook.API')
    def test_execute_dof_batch_pauses_before_retrying_a_transient_error(self, mock_api, _, mock_sleep):
        """Same cooldown behavior as execute_batch applies to the DOF fallback batch: 4 cooldowns
        for 5 attempts, none on the final attempt that gives up."""
        stream = AdCreative.__new__(AdCreative)
        stream.catalog_entry = mock.Mock()
        stream.fields = lambda: {'id'}

        error = FacebookRequestError('msg', {'relative_url': 'batch'}, 500, {}, '{}')
        batch = mock.Mock()
        batch.execute.side_effect = error
        mock_api.new_batch.return_value = batch

        with mock.patch('tap_facebook.adcreative.AdCreative'):
            with self.assertRaises(FacebookRequestError):
                stream.execute_dof_batch(['1'], ['id'], {}, mock.Mock())

        self.assertEqual(batch.execute.call_count, 5)
        self.assertEqual(
            mock_sleep.call_args_list.count(mock.call(ADCREATIVE_BATCH_ERROR_PAUSE_SECONDS)), 4)
