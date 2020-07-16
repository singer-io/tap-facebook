import singer
from singer import metrics
from typing import Sequence, Union, Optional, Dict
from datetime import timedelta, datetime
from dateutil import parser

from tap_facebook.client import Facebook

logger = singer.get_logger()


class AdsInsights:
    def __init__(self, client, config):

        self.config = config
        self.client = client
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

        if start_date.date() >= today.date() + timedelta(days=-1):
            logger.info(
                f"start_date {start_date} is yesterday - aborting run to not accidentally skip a day that has not yet received data yet."
            )
            return self.__advance_bookmark(account_id, state, None, tap_stream_id)

        prev_bookmark = None
        with singer.metrics.record_counter(tap_stream_id) as counter:
            try:
                for insight in self.client.list_insights(
                    account_id, fields=fields, start_date=start_date
                ):
                    new_bookmark = insight[self.bookmark_key]

                    if not prev_bookmark:
                        prev_bookmark = new_bookmark

                    if prev_bookmark < new_bookmark:
                        state = self.__advance_bookmark(
                            account_id, state, prev_bookmark, tap_stream_id
                        )
                        prev_bookmark = new_bookmark

                    singer.write_record(tap_stream_id, insight)
                    counter.increment(1)
            except Exception:
                self.__advance_bookmark(account_id, state, prev_bookmark, tap_stream_id)
                raise
        return self.__advance_bookmark(account_id, state, prev_bookmark, tap_stream_id)

    def __get_start(self, account_id, state: dict, tap_stream_id: str):
        default_date = datetime.utcnow() + timedelta(weeks=4)

        config_start_date = self.config.get("start_date")
        if config_start_date:
            default_date = parser.isoparse(config_start_date)

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
