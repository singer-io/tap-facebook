import singer
from singer import utils, metadata, CatalogEntry, Transformer, metrics
from typing import Sequence, Union
from datetime import timedelta, datetime
from dateutil import parser

from tap_facebook.client import Facebook

logger = singer.get_logger()


class AdsInsights:
    def __init__(self, client, catalog: CatalogEntry, config):
        self.catalog = catalog
        self.tap_stream_id = catalog.tap_stream_id
        self.schema = catalog.schema.to_dict()
        self.key_properties = catalog.key_properties
        self.mdata = metadata.to_map(catalog.metadata)
        self.config = config
        self.client = client
        self.bookmark_key = "date_start"

    def stream(self, account_ids: Sequence[str], state: dict):

        # write schema
        singer.write_schema(
            self.tap_stream_id,
            self.schema,
            key_properties=self.key_properties,
            bookmark_properties=self.bookmark_key,
        )

        fields = self.__fields_from_catalog(self.catalog)
        for account_id in account_ids:
            state = self.process_account(account_id, fields, state)
        return state

    def process_account(self, account_id: str, fields: Sequence[str], state) -> dict:
        logger.info(f"account_id: {account_id}")
        start_date = self.__get_start(account_id, state)
        today = datetime.utcnow()

        if start_date.date() >= today.date() + timedelta(days=-1):
            logger.info(
                f"start_date {start_date} is yesterday - aborting run to not accidentally skip a day that has not yet received data yet."
            )
            return self.__advance_bookmark(account_id, state, None)

        prev_bookmark = None
        with Transformer() as transformer:
            try:
                for insight in self.client.list_insights(
                    account_id, fields=fields, start_date=start_date
                ):
                    record = transformer.transform(insight, self.schema, self.mdata)
                    bookmark = record[self.bookmark_key]

                    if not prev_bookmark:
                        prev_bookmark = bookmark
                    if bookmark > prev_bookmark:
                        state = self.__advance_bookmark(
                            account_id, state, prev_bookmark
                        )
                        prev_bookmark = bookmark

                    singer.write_record(self.tap_stream_id, record)
            except Exception:
                self.__advance_bookmark(account_id, state, prev_bookmark)
                raise
        return self.__advance_bookmark(account_id, state, prev_bookmark)

    def __fields_from_catalog(self, catalog):
        props = metadata.to_map(catalog.metadata)

        fields = []
        for breadcrumb, mdata in props.items():
            if len(breadcrumb) != 2:
                # only focus on immediate fields
                # this ignores table-key-properties of the root
                # as well as items that are nested below the level of the
                # root object
                continue

            include = mdata.get("selected") or mdata.get("inclusion") == "automatic"
            if not include:
                continue

            _, field = breadcrumb
            fields.append(field)
        return fields

    def __get_start(self, account_id, state: dict):
        default_date = datetime.utcnow() + timedelta(weeks=4)

        config_start_date = self.config.get("start_date")
        if config_start_date:
            default_date = parser.isoparse(config_start_date)

        if not state:
            logger.info(f"using 'start_date' from config: {default_date}")
            return default_date

        account_record = singer.get_bookmark(state, self.tap_stream_id, account_id)
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
        self, account_id: str, state: dict, bookmark: Union[str, datetime, None]
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
            self.tap_stream_id,
            account_id,
            {self.bookmark_key: bookmark_datetime.isoformat()},
        )

        singer.write_state(state)
        return state
