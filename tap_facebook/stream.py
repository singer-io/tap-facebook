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
        print(self.config)
        config_start_date = self.config.get(START_DATE_KEY)
        tap_stream_id = self.name
        state = self.state or {}
        current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
        if current_bookmark is None:
            if isinstance(self, IncrementalStream):
                return None
            else:
                LOGGER.info("no bookmark found for %s, using start_date instead...%s", tap_stream_id, config_start_date)
                return pendulum.parse(config_start_date)
        LOGGER.info("found current bookmark for %s:  %s", tap_stream_id, current_bookmark)
        return pendulum.parse(current_bookmark)

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

    def advance_bookmark(self, bookmark_key, date):
        tap_stream_id = self.name
        state = self.state or {}
        LOGGER.info('advance(%s, %s)', tap_stream_id, date)
        date = pendulum.parse(date) if date else None
        current_bookmark = self.get_start(bookmark_key)

        if date is None:
            LOGGER.info('Did not get a date for stream %s '+
                        ' not advancing bookmark',
                        tap_stream_id)
        elif not current_bookmark or date > current_bookmark:
            LOGGER.info('Bookmark for stream %s is currently %s, ' +
                        'advancing to %s',
                        tap_stream_id, current_bookmark, date)
            state = singer.write_bookmark(state, tap_stream_id, bookmark_key, str(date))
        else:
            LOGGER.info('Bookmark for stream %s is currently %s ' +
                        'not changing to %s',
                        tap_stream_id, current_bookmark, date)
        return state