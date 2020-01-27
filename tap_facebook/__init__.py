#!/usr/bin/env python3
import os
import json
import sys

import singer

# import singer.metrics as metrics
from singer import utils, metadata, Catalog, CatalogEntry, Transformer, Schema

from tap_facebook.utils import load_schema
from tap_facebook.streams import AdsInsights
from tap_facebook.client import Facebook

logger = singer.get_logger()

STREAMS = {
    "ads_insights": {
        "key_properties": [
            "account_id",
            "ad_id",
            "adset_id",
            "campaign_id",
            "date_start",
        ],
        "bookmark_keys": ["start_date"],
        "valid_replication_keys": ["date_start", "account_id", "campaign_id"],
        "stream": AdsInsights,
    }
}


def do_sync(account_ids, client, config, catalog, state):
    for catalog_entry in catalog.streams:
        insights = AdsInsights(client, catalog_entry, config)
        state = insights.stream(account_ids, state)


def discover():
    streams = []
    for tap_stream_id, props in STREAMS.items():
        logger.info(f"loading schema for {tap_stream_id}")
        schema = load_schema(tap_stream_id)

        key_properties = props["key_properties"]
        valid_replication_keys = props["valid_replication_keys"]

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=props["key_properties"],
            valid_replication_keys=valid_replication_keys,
        )

        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=key_properties,
                schema=Schema.from_dict(schema),
                metadata=mdata,
            )
        )

    return Catalog(streams)


def do_discover():
    catalog = discover()
    catalog_dict = catalog.to_dict()
    json.dump(catalog_dict, sys.stdout, indent="  ", sort_keys=True)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args([])

    account_ids = args.config.get("account_ids", [])
    access_token = args.config.get("access_token") or os.environ.get(
        "FACEBOOK_ACCESS_TOKEN"
    )

    if not access_token:
        raise ValueError(
            f"missing required config 'access_token' or environment 'FACEBOOK_ACCESS_TOKEN'"
        )

    if args.discover:
        do_discover()
        return

    if args.catalog or args.properties:
        catalog = args.catalog or args.properties
    else:
        catalog = discover()

    client = Facebook(access_token)

    all_account_ids = {
        accnt["account_id"]: accnt["id"] for accnt in client.list_ad_accounts()
    }
    accnt_ids = []
    if not account_ids:
        accnt_ids = all_account_ids.values()
    else:
        for account_id in account_ids:
            if account_id not in all_account_ids:
                raise ValueError(
                    f"invalid account id: {account_id} not in list of valid account ids: {all_account_ids.keys()}"
                )
            accnt_ids.append(all_account_ids[account_id])

    do_sync(accnt_ids, client, args.config, catalog, args.state)


if __name__ == "__main__":
    main()
