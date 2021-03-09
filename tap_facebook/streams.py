import singer
from typing import Sequence, Union, Optional, Dict, cast, List
from datetime import timedelta, datetime, date
from dateutil import parser

from tap_facebook import utils
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adsinsights import AdsInsights as FacebookAdsInsights

logger = singer.get_logger()


class AdsInsights:
    def __init__(self, config):

        self.config = config
        self.bookmark_key = "date_start"

    def stream(self, account_ids: Sequence[str], state: dict, tap_stream_id: str):

        for account_id in account_ids:
            state = self.process_account(
                account_id, tap_stream_id=tap_stream_id, state=state
            )
        return state

    def process_account(
        self,
        account_id: str,
        tap_stream_id: str,
        state: Dict,
        fields: Optional[Sequence[str]] = None,
    ) -> dict:
        logger.info(f"account_id: {account_id}")
        start_date = self.__get_start(account_id, state, tap_stream_id)
        today = datetime.utcnow()

        if start_date.date() >= today.date() - timedelta(days=1):
            logger.info(
                f"start_date {start_date} is yesterday - aborting run to not accidentally skip a day that has not yet received data yet."
            )
            return self.__advance_bookmark(account_id, state, None, tap_stream_id)

        prev_bookmark = None
        fields = fields or [
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
            "unique_link_clicks_ctr",
            "inline_link_clicks",
            "unique_inline_link_clicks",
        ]
        with singer.record_counter(tap_stream_id) as counter:

            if not start_date:
                raise ValueError("client: start_date is required")

            since = utils.parse_date(start_date)

            until = date.today() + timedelta(days=-1)

            if until - since > timedelta(days=1):
                # for large intervals, the API returns 500
                # handle this by chunking the dates instead
                time_ranges = []

                from_date = since
                while True:
                    to_date = from_date + timedelta(days=1)

                    if to_date > until:
                        break

                    time_ranges.append((from_date, to_date))

                    # add one to to_date to make intervals non-overlapping
                    from_date = to_date + timedelta(days=1)

                if from_date <= until:
                    time_ranges.append((from_date, until))
            else:
                time_ranges = [(since, until)]
            try:
                for (start, stop) in time_ranges:
                    timerange = {"since": str(start), "until": str(stop)}
                    params = {
                        "level": "ad",
                        "limit": 100,
                        "fields": fields,
                        "time_increment": 1,
                        "time_range": timerange,
                    }
                    insights_resp = AdSet(account_id).get_insights(
                        fields=fields, params=params
                    )
                    facebook_insights = cast(List[FacebookAdsInsights], insights_resp)
                    for facebook_insight in facebook_insights:
                        insight = dict(facebook_insight)
                        singer.write_record(tap_stream_id, insight)
                        counter.increment(1)

                        new_bookmark = insight[self.bookmark_key]
                        if not prev_bookmark:
                            prev_bookmark = new_bookmark

                        if prev_bookmark < new_bookmark:
                            state = self.__advance_bookmark(
                                account_id, state, prev_bookmark, tap_stream_id
                            )
                            prev_bookmark = new_bookmark

            except Exception:
                self.__advance_bookmark(account_id, state, prev_bookmark, tap_stream_id)
                raise
        return self.__advance_bookmark(account_id, state, prev_bookmark, tap_stream_id)

    def __get_start(self, account_id, state: dict, tap_stream_id: str) -> datetime:
        default_date = datetime.utcnow() + timedelta(weeks=4)

        config_start_date = self.config.get("start_date")
        if config_start_date:
            default_date = parser.isoparse(config_start_date)

        # the facebook api does not allow us to go more than 37 weeks backwards.
        # we'll lock it for 36 weeks just to be sure
        if datetime.utcnow() - default_date > timedelta(days=37 * 30):
            default_date = datetime.utcnow() - timedelta(days=36 * 30)

        if not state:
            logger.info(f"using 'start_date' from config: {default_date}")
            return default_date

        account_record = singer.get_bookmark(state, tap_stream_id, account_id)
        if not account_record:
            logger.info(f"using 'start_date' from config: {default_date}")
            return default_date

        current_bookmark = account_record.get(self.bookmark_key, None)
        if not current_bookmark:
            logger.info(f"using 'start_date' from config: {default_date}")
            return default_date

        state_date = parser.isoparse(current_bookmark)

        # increment by one to not reprocess the previous date
        new_date = state_date + timedelta(days=1)

        logger.info(f"using 'start_date' from previous state: {current_bookmark}")
        return new_date

    def __advance_bookmark(
        self,
        account_id: str,
        state: dict,
        bookmark: Union[str, datetime, None],
        tap_stream_id: str,
    ):
        if not bookmark:
            singer.write_state(state)
            return state

        if isinstance(bookmark, datetime):
            bookmark_datetime = bookmark
        elif isinstance(bookmark, str):
            bookmark_datetime = parser.isoparse(bookmark)
        else:
            raise ValueError(
                f"bookmark is of type {type(bookmark)} but must be either string or datetime"
            )

        state = singer.write_bookmark(
            state,
            tap_stream_id,
            account_id,
            {self.bookmark_key: bookmark_datetime.isoformat()},
        )

        singer.write_state(state)
        return state
