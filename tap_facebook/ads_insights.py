import sys
import json

import attr

import pendulum
import requests
import backoff
import singer
import singer.metrics as metrics


from tap_facebook.stream import Stream
from tap_facebook.config import *

from facebook_business.exceptions import FacebookRequestError
import time

import facebook_business.adobjects.adsinsights as adsinsights


LOGGER = singer.get_logger()

    
@attr.s
class AdsInsights(Stream):
    field_class = adsinsights.AdsInsights.Field
    base_properties = ['campaign_id', 'adset_id', 'ad_id', 'date_start']

    options = attr.ib()
    action_breakdowns = attr.ib(default=ALL_ACTION_BREAKDOWNS)
    level = attr.ib(default='ad')
    action_attribution_windows = attr.ib(
        default=ALL_ACTION_ATTRIBUTION_WINDOWS)
    time_increment = attr.ib(default=1)
    limit = attr.ib(default=RESULT_RETURN_LIMIT)

    bookmark_key = "date_start"

    invalid_insights_fields = ['impression_device', 'publisher_platform', 'platform_position','age', 'gender', 'country', 'placement', 'region', 'dma']



    # pylint: disable=no-member,unsubscriptable-object,attribute-defined-outside-init
    def __attrs_post_init__(self):
        self.breakdowns = self.options.get('breakdowns') or []
        self.key_properties = self.base_properties[:]
        if self.options.get('primary-keys'):
            self.key_properties.extend(self.options['primary-keys'])

    def job_params(self):
        start_date = self.get_start(self.bookmark_key)

        buffer_days = self.config.get('insights_buffer_days', 5)

        buffered_start_date = start_date.subtract(days=buffer_days)

        end_date = pendulum.now()
        config_end_date = self.config.get("end_date")
        if config_end_date:
            end_date = pendulum.parse(config_end_date)

        # Some automatic fields (primary-keys) cannot be used as 'fields' query params.
        while buffered_start_date <= end_date:
            yield {
                'level': self.level,
                'action_breakdowns': list(self.action_breakdowns),
                'breakdowns': list(self.breakdowns),
                'limit': self.limit,
                'fields': list(self.fields().difference(self.invalid_insights_fields)),
                'time_increment': self.time_increment,
                'action_attribution_windows': list(self.action_attribution_windows),
                'time_ranges': [{'since': buffered_start_date.to_date_string(),
                                 'until': buffered_start_date.to_date_string()}]
            }
            buffered_start_date = buffered_start_date.add(days=1)

    def run_job(self, params):
        # do not perform request immediately due to potential throttling exception
        request = self.account.get_insights(params=params, pending=True)
        yield from self.paginate(request)

    # truly disgusting workaround for self-throttling:
    # using the len(cursor) to ensure that new api calls are not made
    # without us knowing and also enable us to control the re-tries.
    # 
    # both the request.execute and cursor.load_next_page 
    # can be executed multiple times and will only mutate
    # itself when it succeeds, enabling us to blindly retry it.
    # 
    # this does not mean you should not have a shower after looking at this.
    def paginate(self, request):
        while True:
            try:
                cursor = request.execute()
                break
            except FacebookRequestError as err:
                self.wait_on_throttle(err)

        while True:
            for i in range(len(cursor)):
                yield cursor.__getitem__(i)

            try:
                if not cursor.load_next_page():
                    break
            except FacebookRequestError as err:
                self.wait_on_throttle(err)

    def wait_on_throttle(self, err):
        if not isinstance(err, FacebookRequestError):
            raise err

        headers = err.http_headers()

        if err.api_transient_error():
            LOGGER.warn(f"error is transient - sleeping for 5 seconds just to be sure")
            return
        
        # BUC Rate Limit Type
        # https://developers.facebook.com/docs/graph-api/overview/rate-limiting#error-codes-2
        
        RATE_LIMIT_SUBCODE = 2446079
        
        # Ads Inisights => 80000
        # Custom Audience => 80003
        # Ads management => 80004
        if err.api_error_code() in (80000, 80003, 80004) and err.api_error_subcode() == RATE_LIMIT_SUBCODE:
            headers = err.http_headers()
            usage_object_raw = headers.get('x-business-use-case-usage')

            if not usage_object_raw:
                raise err

            usage_obj = json.loads(usage_object_raw)
            for accnt, metadata in usage_obj.items():
                wait_time_minutes = metadata[0].get("estimated_time_to_regain_access")
                if wait_time_minutes:
                    LOGGER.info(f"waiting {wait_time_minutes} minutes based on 'estimated_time_to_regain_access' header for accnt {accnt}")
                    time.sleep(60 * int(wait_time_minutes))
            
            return

    def __iter__(self):
        for params in self.job_params():
            with metrics.job_timer('insights'):
                count = 0
                min_date_start_for_job = None
                for obj in self.run_job(params):
                    count += 1
                    rec = obj.export_all_data()
                    if not min_date_start_for_job or rec['date_stop'] < min_date_start_for_job:
                        min_date_start_for_job = rec['date_stop']
                    yield {'record': rec}

            # when min_date_start_for_job stays None, we should
            # still update the bookmark using 'until' in time_ranges
            if min_date_start_for_job is None:
                for time_range in params['time_ranges']:
                    if time_range['until']:
                        min_date_start_for_job = time_range['until']
            yield {'state': self.advance_bookmark(self.bookmark_key,min_date_start_for_job)} # pylint: disable=no-member
