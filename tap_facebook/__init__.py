#!/usr/bin/env python3
import json
import os
import os.path
import sys
import time

import datetime
from datetime import timezone
import dateutil

import attr
import pendulum
import requests
import backoff

import singer
import singer.metrics as metrics
from singer import utils, metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)
from singer.catalog import Catalog, CatalogEntry

from functools import partial

from tap_facebook.ads_insights import AdsInsights
from tap_facebook.stream import Stream # IncrementalStream
from tap_facebook.retry import retry_pattern
from tap_facebook.config import *
from tap_facebook.utils import *
from tap_facebook.exceptions import *

from facebook_business import FacebookAdsApi
import facebook_business.adobjects.adcreative as adcreative

# import facebook_business.adobjects.adset as adset
# import facebook_business.adobjects.campaign as fb_campaign
import facebook_business.adobjects.adsinsights as adsinsights
import facebook_business.adobjects.user as fb_user


TODAY = pendulum.today()


LOGGER = singer.get_logger()


def ad_creative_success(response, stream=None):
    '''A success callback for the FB Batch endpoint used when syncing AdCreatives. Needs the stream
    to resolve schema refs and transform the successful response object.'''
    refs = load_shared_schema_refs()
    schema = singer.resolve_schema_references(stream.catalog_entry.schema.to_dict(), refs)

    rec = response.json()
    record = Transformer(pre_hook=transform_date_hook).transform(rec, schema)
    singer.write_record(stream.name, record, stream.stream_alias, utils.now())


def ad_creative_failure(response):
    '''A failure callback for the FB Batch endpoint used when syncing AdCreatives. Raises the error
    so it fails the sync process.'''
    raise response.error()


def initialize_stream(account, catalog_entry, state, config): # pylint: disable=too-many-return-statements

    name = catalog_entry.stream
    stream_alias = catalog_entry.stream_alias

    if name in INSIGHTS_BREAKDOWNS_OPTIONS:
        return AdsInsights(name, account, stream_alias, catalog_entry, state=state, config=config, options=INSIGHTS_BREAKDOWNS_OPTIONS[name])
    # elif name == 'campaigns':
    #     return Campaigns(name, account, stream_alias, catalog_entry, state=state)
    # elif name == 'adsets':
    #     return AdSets(name, account, stream_alias, catalog_entry, state=state)
    # elif name == 'ads':
    #     return Ads(name, account, stream_alias, catalog_entry, state=state)
    # elif name == 'adcreative':
    #     return AdCreative(name, account, stream_alias, catalog_entry)
    # else:
    #     raise TapFacebookException('Unknown stream {}'.format(name))


def get_streams_to_sync(account, catalog, state, config):
    for stream in STREAMS:
        catalog_entry = next((s for s in catalog.streams if s.tap_stream_id == stream), None)
        if catalog_entry and catalog_entry.is_selected():
            # TODO: Don't need name and stream_alias since it's on catalog_entry
            yield initialize_stream(account, catalog_entry, state, config)


def do_sync(account, catalog, state, config):
    streams_to_sync = get_streams_to_sync(account, catalog, state, config)
    refs = load_shared_schema_refs()

    for stream in streams_to_sync:
        LOGGER.info('Syncing %s, fields %s', stream.name, stream.fields())
        schema = singer.resolve_schema_references(load_schema(stream), refs)
        bookmark_key = BOOKMARK_KEYS.get(stream.name)
        singer.write_schema(stream.name, schema, stream.key_properties, bookmark_key, stream.stream_alias)

        # NB: The AdCreative stream is not an iterator
        if stream.name == 'adcreative':
            stream.sync()
            continue

        with Transformer(pre_hook=transform_date_hook) as transformer:
            with metrics.record_counter(stream.name) as counter:
                for message in stream:
                    if 'record' in message:
                        counter.increment()
                        time_extracted = utils.now()
                        record = transformer.transform(message['record'], schema)
                        singer.write_record(stream.name, record, stream.stream_alias, time_extracted)
                    elif 'state' in message:
                        singer.write_state(message['state'])
                    else:
                        raise TapFacebookException('Unrecognized message {}'.format(message))


def initialize_streams_for_discovery(config): # pylint: disable=invalid-name
    return [initialize_stream(None, CatalogEntry(stream=name), None, config)
            for name in ["ads_insights"]]

def discover_schemas():
    # Load Facebook's shared schemas
    refs = load_shared_schema_refs()

    result = {'streams': []}
    streams = initialize_streams_for_discovery(None)
    for stream in streams:
        LOGGER.info('Loading schema for %s', stream.name)
        schema = singer.resolve_schema_references(load_schema(stream), refs)

        mdata = metadata.to_map(metadata.get_standard_metadata(schema,
                                               key_properties=stream.key_properties))

        bookmark_key = BOOKMARK_KEYS.get(stream.name)
        if bookmark_key == UPDATED_TIME_KEY:
            mdata = metadata.write(mdata, ('properties', bookmark_key), 'inclusion', 'automatic')

        result['streams'].append({'stream': stream.name,
                                  'tap_stream_id': stream.name,
                                  'schema': schema,
                                  'metadata': metadata.to_list(mdata)})
    return result


def do_discover():
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    account_id = args.config['account_id']
    access_token = args.config['access_token']

    config = args.config

    result_return_limit = config.get('result_return_limit', RESULT_RETURN_LIMIT)

    API = FacebookAdsApi.init(access_token=access_token)
    user = fb_user.User(fbid='me')
    accounts = user.get_ad_accounts()
    account = None
    for acc in accounts:
        if acc['account_id'] == account_id:
            account = acc
    if not account:
        raise TapFacebookException("Couldn't find account with id {}".format(account_id))

    if args.discover:
        do_discover()
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        do_sync(account, catalog, args.state, args.config)
    else:
        LOGGER.info("No properties were selected")

def main():

    try:
        main_impl()
    except TapFacebookException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.exception(e)
        for line in str(e).splitlines():
            LOGGER.critical(line)
        raise e
