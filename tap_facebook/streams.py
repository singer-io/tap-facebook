import singer
from singer import utils, metadata, CatalogEntry, Transformer, metrics
from typing import Sequence
from datetime import timedelta, datetime
from dateutil import parser

from .client import Facebook

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

        for account_id in account_ids:
            fields = self.__fields_from_catalog(self.catalog)

            logger.error(fields)
            state = self.process_account(account_id, fields, state)
        return state

    def process_account(self, account_id: str, fields: Sequence[str], state) -> dict:
        start_date = self.__get_start(account_id, state)

        logger.info(f"account_id: {account_id}")
        logger.info(f"start_date: {start_date}")

        bookmark = start_date
        with Transformer() as transformer:
            prev_bookmark = None
            for insight in self.client.list_insights(
                account_id, fields=fields, start_date=start_date
            ):
                record = transformer.transform(insight, self.schema, self.mdata)
                bookmark = record[self.bookmark_key]

                if not prev_bookmark:
                    prev_bookmark = bookmark
                elif bookmark > prev_bookmark:
                    state = self.__advance_bookmark(account_id, state, bookmark)
                    prev_bookmark = bookmark

                singer.write_record(self.tap_stream_id, record)

        return self.__advance_bookmark(account_id, state, bookmark)

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
            return default_date.isoformat()

        account_record = singer.get_bookmark(state, self.tap_stream_id, account_id)
        if not account_record:
            return default_date.isoformat()

        current_bookmark = account_record.get(self.bookmark_key, None)
        if not current_bookmark:
            return default_date.isoformat()

        return parser.isoparse(current_bookmark).isoformat()

    def __advance_bookmark(self, account_id: str, state: dict, bookmark: str):

        state = singer.write_bookmark(
            state, self.tap_stream_id, account_id, {self.bookmark_key: str(bookmark)}
        )

        singer.write_state(state)
        return state
