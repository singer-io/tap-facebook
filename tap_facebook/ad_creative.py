from tap_facebook.stream import Stream

class AdCreative(Stream):
    '''
    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup/adcreatives/
    '''

    field_class = adcreative.AdCreative.Field
    key_properties = ['id']


    def sync(self):
        @retry_pattern(backoff.expo, FacebookRequestError, max_tries=5, factor=5)
        def do_request():
            return self.account.get_ad_creatives(params={'limit': RESULT_RETURN_LIMIT})

        ad_creative = do_request()

        # Create the initial batch
        api_batch = API.new_batch()
        batch_count = 0

        # This loop syncs minimal AdCreative objects
        for a in ad_creative:
            # Excecute and create a new batch for every 50 added
            if batch_count % 50 == 0:
                api_batch.execute()
                api_batch = API.new_batch()

            # Add a call to the batch with the full object
            a.api_get(fields=self.fields(),
                      batch=api_batch,
                      success=partial(ad_creative_success, stream=self),
                      failure=ad_creative_failure)
            batch_count += 1

        # Ensure the final batch is executed
        api_batch.execute()