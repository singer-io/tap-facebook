import sys

import singer
import backoff

from tap_facebook.exceptions import *

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