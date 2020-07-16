#!/usr/bin/env python3
import os
import json
import sys

import singer

from tap_facebook.utils import load_schema
from tap_facebook.streams import AdsInsights
from tap_facebook.client import Facebook

logger = singer.get_logger()

STREAMS = ["ads_insights"]


def do_sync(account_ids, client, config, state):
    insights = AdsInsights(client, config)

    for tap_stream_id in STREAMS:
        state = insights.stream(account_ids, state, tap_stream_id)


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

    do_sync(accnt_ids, client, args.config, args.state)


if __name__ == "__main__":
    main()
