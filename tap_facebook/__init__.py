#!/usr/bin/env python3
import os

import singer

from tap_facebook.streams import AdsInsights

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User

logger = singer.get_logger()

STREAMS = {"ads_insights": AdsInsights}


def do_sync(account_ids, config, state):

    for tap_stream_id, streamer_class in STREAMS.items():
        streamer_inst = streamer_class(config)
        state = streamer_inst.stream(account_ids, state, tap_stream_id)


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

    # use
    api = FacebookAdsApi.init(access_token=access_token)

    all_account_ids = {
        accnt["account_id"]: accnt["id"]
        for accnt in User("me", api=api).get_ad_accounts(fields=["account_id", "id"])
    }
    accnt_ids = []

    for account_id in account_ids:
        if account_id not in all_account_ids:
            logger.warn(
                f"invalid account id: {account_id} not in list of valid account ids: {all_account_ids.keys()}"
            )
            continue

        accnt_ids.append(all_account_ids[account_id])

    print(f"account ids: ", accnt_ids)
    do_sync(accnt_ids, args.config, args.state)


if __name__ == "__main__":
    main()
