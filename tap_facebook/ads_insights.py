import sys

import attr

import pendulum
import requests
import backoff
import ratelimit
import singer
import singer.metrics as metrics


from tap_facebook.stream import Stream
from tap_facebook.exceptions import *

import time

import facebook_business.adobjects.adsinsights as adsinsights

from tap_facebook.config import *

LOGGER = singer.get_logger()

def retry_pattern(backoff_type, exception, **wait_gen_kwargs):
    # HACK: Workaround added due to bug with Facebook prematurely deprecating 'relevance_score'
    # Issue being tracked here: https://developers.facebook.com/support/bugs/2489592517771422
    def is_relevance_score(exception):
        if getattr(exception, "body", None):
            return exception.body().get("error", {}).get("message") == '(#100) relevance_score is not valid for fields param. please check https://developers.facebook.com/docs/marketing-api/reference/ads-insights/ for all valid values'
        else:
            return False

    def log_retry_attempt(details):
        _, exception, _ = sys.exc_info()
        if is_relevance_score(exception):
            raise Exception("Due to a bug with Facebook prematurely deprecating 'relevance_score' that is "
                            "not affecting all tap-facebook users in the same way, you need to "
                            "deselect `relevance_score` from your Insights export. For further "
                            "information, please see this Facebook bug report thread: "
                            "https://developers.facebook.com/support/bugs/2489592517771422") from exception
        LOGGER.info(exception)
        LOGGER.info('Caught retryable error after %s tries. Waiting %s more seconds then retrying...',
                    details["tries"],
                    details["wait"])

    def should_retry_api_error(exception):
        if isinstance(exception, FacebookRequestError):
            return exception.api_transient_error() or exception.api_error_subcode() == 99 or is_relevance_score(exception)
        elif isinstance(exception, InsightsJobTimeout):
            return True
        return False

    return backoff.on_exception(
        backoff_type,
        exception,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=lambda exc: not should_retry_api_error(exc),
        **wait_gen_kwargs
    )

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

    bookmark_key = START_DATE_KEY

    invalid_insights_fields = ['impression_device', 'publisher_platform', 'platform_position',
                               'age', 'gender', 'country', 'placement', 'region', 'dma']



    # pylint: disable=no-member,unsubscriptable-object,attribute-defined-outside-init
    def __attrs_post_init__(self):
        self.breakdowns = self.options.get('breakdowns') or []
        self.key_properties = self.base_properties[:]
        if self.options.get('primary-keys'):
            self.key_properties.extend(self.options['primary-keys'])

    def job_params(self):
        start_date = self.get_start(self.bookmark_key)

        buffer_days = 28
        # if CONFIG.get('insights_buffer_days'):
        #     buffer_days = int(CONFIG.get('insights_buffer_days'))

        buffered_start_date = start_date.subtract(days=buffer_days)

        end_date = pendulum.now()
        # if CONFIG.get('end_date'):
        #     end_date = pendulum.parse(CONFIG.get('end_date'))

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

    @retry_pattern(backoff.expo, (FacebookRequestError, InsightsJobTimeout), max_tries=5, factor=5)
    # @ratelimit.limits(calls=60 + 400 * )
    def run_job(self, params):
        LOGGER.info('Starting adsinsights job with params %s', params)
        job = self.account.get_insights( # pylint: disable=no-member
            params=params,
            is_async=True)
        status = None
        time_start = time.time()
        sleep_time = 10
        while status != "Job Completed":
            duration = time.time() - time_start
            job = job.api_get()
            status = job['async_status']
            percent_complete = job['async_percent_completion']

            job_id = job['id']
            LOGGER.info('%s, %d%% done', status, percent_complete)

            if status == "Job Completed":
                return job

            if duration > INSIGHTS_MAX_WAIT_TO_START_SECONDS and percent_complete == 0:
                pretty_error_message = ('Insights job {} did not start after {} seconds. ' +
                                        'This is an intermittent error and may resolve itself on subsequent queries to the Facebook API. ' +
                                        'You should deselect fields from the schema that are not necessary, ' +
                                        'as that may help improve the reliability of the Facebook API.')
                raise InsightsJobTimeout(pretty_error_message.format(job_id, INSIGHTS_MAX_WAIT_TO_START_SECONDS))
            elif duration > INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS and status != "Job Completed":
                pretty_error_message = ('Insights job {} did not complete after {} seconds. ' +
                                        'This is an intermittent error and may resolve itself on subsequent queries to the Facebook API. ' +
                                        'You should deselect fields from the schema that are not necessary, ' +
                                        'as that may help improve the reliability of the Facebook API.')
                raise InsightsJobTimeout(pretty_error_message.format(job_id,
                                                                     INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS//60))

            LOGGER.info("sleeping for %d seconds until job is done", sleep_time)
            time.sleep(sleep_time)
            if sleep_time < INSIGHTS_MAX_ASYNC_SLEEP_SECONDS:
                sleep_time = 2 * sleep_time
        return job

    def __iter__(self):
        for params in self.job_params():
            with metrics.job_timer('insights'):
                job = self.run_job(params)

            min_date_start_for_job = None
            count = 0
            for obj in job.get_result():
                count += 1
                rec = obj.export_all_data()
                if not min_date_start_for_job or rec['date_stop'] < min_date_start_for_job:
                    min_date_start_for_job = rec['date_stop']
                yield {'record': rec}
            LOGGER.info('Got %d results for insights job', count)

            # when min_date_start_for_job stays None, we should
            # still update the bookmark using 'until' in time_ranges
            if min_date_start_for_job is None:
                for time_range in params['time_ranges']:
                    if time_range['until']:
                        min_date_start_for_job = time_range['until']
            yield {'state': self.advance_bookmark(self, self.bookmark_key,
                                             min_date_start_for_job)} # pylint: disable=no-member