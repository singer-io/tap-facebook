import unittest
from unittest import mock

from facebook_business.exceptions import FacebookRequestError

from tap_facebook import (AdCreative, adcreative_batch_record_failure,
                          creative_id_from_response)


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
