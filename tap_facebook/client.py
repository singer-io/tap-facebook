import json
import requests
import backoff
import ratelimit
import time
from typing import Union, Sequence

import singer

from datetime import datetime, timedelta, date
from dateutil import parser

RATE_LIMIT_SUBCODE = 2446079
ADS_INSIGHTS_CODE = 80000
CUSTOM_AUDIENCE_CODE = 80003
ADS_MANAGEMENT = 80004

logger = singer.get_logger()


def should_give_up(err):
    if isinstance(err, ratelimit.exception.RateLimitException):
        return False

    if not isinstance(
        err, (requests.exceptions.HTTPError, requests.exceptions.RequestException)
    ):
        return True

    logger.error(str(err))
    headers = err.response.headers
    status_code = err.response.status_code
    data = err.response.json()

    if "error" not in data:
        return True

    error = data["error"]
    code = error.get("code", 0)
    message = error.get("message")
    error_subcode = error.get("error_subcode", 0)
    is_transient = error.get("is_transient", False)
    type = error.get("type", None)

    logger.error(f"Facebook client error: {message}")

    if not (
        code in [ADS_INSIGHTS_CODE, ADS_MANAGEMENT, CUSTOM_AUDIENCE_CODE]
        and error_subcode == RATE_LIMIT_SUBCODE
    ):
        if is_transient:
            return False

        return True

    use_case_usage_str = headers.get("x-business-use-case-usage", None)
    if not use_case_usage_str:
        return False

    use_case_usage = json.loads(use_case_usage_str)
    for account_id, account_metadata in use_case_usage.items():
        for metadata in account_metadata:
            if "estimated_time_to_regain_access" not in metadata:
                continue

            wait_time_in_minutes = metadata["estimated_time_to_regain_access"]
            if not wait_time_in_minutes:
                return False

            resume_datetime = datetime.utcnow() + timedelta(
                seconds=wait_time_in_minutes * 60
            )

            logger.warn(
                f"waiting for {wait_time_in_minutes} minutes based on 'estimated_time_to_regain_access' for account_id {account_id}"
            )
            logger.warn(f"will resume at {resume_datetime.isoformat()} UTC")
            time.sleep(wait_time_in_minutes * 60)
            return False

    return True


class Facebook(object):
    def __init__(self, access_token, version="v5.0"):
        self.access_token = access_token
        self.version = version
        self.base_url = f"https://graph.facebook.com/{version}"
        self.__session = requests.Session()

    def list_ad_accounts(self):
        yield from self.__do("GET", f"{self.base_url}/me/adaccounts")

    def list_insights(
        self,
        account_id,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime] = None,
        fields: list = None,
        limit: int = 100,
        action_breakdowns: list = [],
        breakdowns: list = [],
        time_increments: int = 1,
        action_attribution_windows: list = [],
    ):
        if not start_date:
            raise ValueError("client: start_date is required")

        since = self.__parse_date(start_date)

        if end_date:
            until = self.__parse_date(end_date)
        else:
            until = date.today() + timedelta(days=-1)

        increment = 1

        if until - since > timedelta(days=increment):
            # for large intervals, the API returns 500
            # handle this by chunking the dates instead
            time_ranges = []

            total_days = (until - since).days
            from_date = since
            while True:
                to_date = from_date + timedelta(days=increment)

                if to_date > until:
                    break

                time_ranges.append((from_date, to_date))

                # add one to to_date to make intervals non-overlapping
                from_date = to_date + timedelta(days=1)

            if from_date <= until:
                time_ranges.append((from_date, until))
        else:
            time_ranges = [(since, until)]

        for (start, stop) in time_ranges:

            timerange = {"since": str(start), "until": str(stop)}

            if not fields:
                fields = [
                    "account_id",
                    "account_name",
                    "account_currency",
                    "ad_id",
                    "ad_name",
                    "adset_id",
                    "adset_name",
                    "campaign_id",
                    "campaign_name",
                    "clicks",
                    "ctr",
                    "date_start",
                    "date_stop",
                    "frequency",
                    "impressions",
                    "reach",
                    "social_spend",
                    "spend",
                    "unique_clicks",
                    "unique_ctr",
                ]

            params = {
                "level": "ad",
                "limit": limit,
                "action_breakdowns": action_breakdowns,
                "breakdowns": breakdowns,
                "fields": fields,
                "time_increment": 1,
                "action_attribution_windows": action_attribution_windows,
                "time_range": timerange,
            }

            yield from self.__paginate(
                "GET", f"{self.base_url}/{account_id}/insights", params=params
            )

    def __parse_date(self, dateobj: Union[str, datetime]):
        if isinstance(dateobj, datetime):
            return dateobj.date()
        elif isinstance(dateobj, str):
            return parser.isoparse(dateobj).date()

        raise ValueError(f"invalid date: {dateobj}")

    def __paginate(self, method, url, **kwargs):
        while True:
            data, next = self.__do(method, url, paginate=True, **kwargs)
            if data:
                yield from data

            if not next:
                return

            url = next

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
        giveup=should_give_up,
    )
    @ratelimit.limits(calls=20 * 60, period=60, raise_on_limit=False)
    def __do(self, method, url, paginate=False, **kwargs):
        params = kwargs.pop("params", {})
        params["access_token"] = self.access_token
        encoded_params = self.__encode_params(params)

        resp = self.__session.request(method, url, params=encoded_params, **kwargs)

        resp.raise_for_status()

        response = resp.json()
        if "error" in response:
            message = response["error"].get("message")
            raise RuntimeError(f"Facebook Api Error: {message}")

        data = response.get("data", {})
        if not paginate:
            return data

        paginate = response.get("paging", {}).get("next")

        return data, paginate

    def __encode_params(self, params: dict):
        """encode all parameters in a way that is not native to the requests library.
        See url: 
        """
        encoded_params = {}
        for k, v in params.items():
            if isinstance(v, dict):
                encoded_params[k] = json.dumps(v)
            elif isinstance(v, (list, tuple, set)):
                encoded_params[k] = ",".join(list(v))
            else:
                encoded_params[k] = v
        return encoded_params


if __name__ == "__main__":
    import os
    import json

    access_token = os.environ.get("FACEBOOK_ACCESS_TOKEN")
    if not access_token:
        raise ValueError(f"missing 'FACEBOOK_ACCESS_TOKEN' in environment")

    # fb_client = Facebook(access_token)
    # for ad_account in fb_client.list_ad_accounts():
    #     insights = list(fb_client.list_insights(ad_account["id"]),)
    #     for insight in insights:
    #         print(json.dumps(insights))
