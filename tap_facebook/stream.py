import attr
import singer
from singer import utils, metadata
import pendulum

from tap_facebook.config import *

LOGGER = singer.get_logger()

@attr.s
class Stream(object):
    name = attr.ib()
    account = attr.ib()
    stream_alias = attr.ib()
    catalog_entry = attr.ib()
    state = attr.ib()
    config = attr.ib()

    def automatic_fields(self):
        fields = set()
        if self.catalog_entry:
            props = metadata.to_map(self.catalog_entry.metadata)
            for breadcrumb, data in props.items():
                if len(breadcrumb) != 2:
                    continue # Skip root and nested metadata

                if data.get('inclusion') == 'automatic':
                    fields.add(breadcrumb[1])
        return fields


    def fields(self):
        fields = set()
        if self.catalog_entry:
            props = metadata.to_map(self.catalog_entry.metadata)
            for breadcrumb, data in props.items():
                if len(breadcrumb) != 2:
                    continue # Skip root and nested metadata

                if data.get('selected') or data.get('inclusion') == 'automatic':
                    fields.add(breadcrumb[1])
        return fields
    
    def get_start(self, bookmark_key):
        tap_stream_id = self.name
        config_start_date = self.config.get(START_DATE_KEY)
        current_bookmark = singer.get_bookmark(self.state, tap_stream_id, bookmark_key)

        if current_bookmark:
            return pendulum.parse(current_bookmark)
        
        return pendulum.parse(config_start_date)
    
    def advance_bookmark(self, bookmark_key, date):
        tap_stream_id = self.name

        current_bookmark = self.get_start(bookmark_key)
        date = pendulum.parse(date) if date else None

        LOGGER.info(f"date: {date}")
        LOGGER.info(f"current_bookmark: {current_bookmark}")

        if date and date > current_bookmark:
            self.state = singer.write_bookmark(self.state, tap_stream_id, bookmark_key, str(date))
        else:
            self.state = singer.write_bookmark(self.state, tap_stream_id, bookmark_key, str(current_bookmark))

        return self.state

@attr.s
class IncrementalStream(Stream):
    def __attrs_post_init__(self):
        self.current_bookmark = self.get_start(UPDATED_TIME_KEY)

    def _iterate(self, generator, record_preparation):
        max_bookmark = None
        for recordset in generator:
            for record in recordset:
                updated_at = pendulum.parse(record[UPDATED_TIME_KEY])

                if self.current_bookmark and self.current_bookmark >= updated_at:
                    continue
                if not max_bookmark or updated_at > max_bookmark:
                    max_bookmark = updated_at

                record = record_preparation(record)
                yield {'record': record}

            if max_bookmark:
                yield {'state': self.advance_bookmark(UPDATED_TIME_KEY, str(max_bookmark))}

