# Changelog

## 1.26.8
  * Fix `AdsInsights.job_params()` never actually excluding the `reach` field for breakdown queries older than 13 months, despite logging that it would -- `invalid_insights_fields` (the list actually subtracted from the requested fields) never included `reach`, so old-data requests kept sending it and risked Facebook rejecting them. The check is now also done per-day inside the loop (not once for the whole range), since a range starting >13 months ago still yields many recent days for which `reach` is valid and should still be requested.

## 1.26.7
  * Extend the proactive rate-limit throttle to also read `X-Business-Use-Case-Usage.*.call_count` (Ads Insights), not just `X-Ad-Account-Usage.acc_id_util_pct` (Ads Management) -- most of this tap's streams are `ads_insights` variants governed by the Business Use Case formula, so they previously had no proactive backoff at all, only reactive retry-after-failure. When both headers are present, the higher of the two utilization signals governs.
  * Make the proactive pause escalate in two tiers instead of one flat pause: a short pause from 80% utilization (unchanged), and a longer pause from 95% -- so a large first full sync backs off increasingly as it approaches the ceiling instead of cruising at full speed until it hits `code 17`.

## 1.26.6
  * Disable the Facebook SDK's crash-reporter (`FacebookAdsApi.init(..., crash_log=False)`). The SDK arms a `sys.excepthook` patch by default (`crash_log=True`) that, on any uncaught non-`FacebookError` exception, POSTs a crash report to Facebook's `/instruments` endpoint using `node_id=app_id`. This tap never passes `app_id` to `init()`, so that POST always fails with a real Facebook 400 (`GraphMethodException`, `error_subcode: 33`). That failing request is itself routed through `call_with_retry` (the global `FacebookAdsApi.call` monkeypatch), and the shared subcode-33 transient check (added for an unrelated AdsInsights race condition) treats it as retryable -- wasting ~75s retrying the SDK's own broken self-diagnostic call before the original exception is finally re-raised.
  * Make `call_with_retry` retry genuine transient network truncations directly: widen its exception tuple to also catch `requests.exceptions.ChunkedEncodingError` and `urllib3.exceptions.ProtocolError` (in addition to the existing `FacebookRequestError` and `requests.exceptions.ConnectionError`), and extend `is_transient_facebook_error()` to recognize both as always-retryable so `giveup` doesn't reject them. Fixes a production incident where an `AdCreative.sync_batches` batch call hit a transient `ChunkedEncodingError` (`IncompleteRead`) that wasn't a `FacebookRequestError`, so `backoff` never caught it at all and it crashed the tap uncaught -- triggering the crash-reporter detour above.
  * Also add `requests.exceptions.Timeout` to `call_with_retry`'s exception tuple -- it was already treated as transient by `is_transient_facebook_error()` but was never reachable there, the same class of gap as the two fixes above.
  * Add calculated, bounded rate-limit pacing to `call_with_retry`: when Meta's `X-Ad-Account-Usage` (`reset_time_duration`, seconds) or `X-Business-Use-Case-Usage` (`estimated_time_to_regain_access`, minutes) response headers are present on a `FacebookRequestError`, wait exactly as long as Meta reports instead of guessing with exponential backoff -- capped at 120s per wait and 180s total (`max_time`), falling back to the original exponential sequence when no usable header is found. Also add a small proactive pause (1s) after any successful call once `acc_id_util_pct` crosses 80%, easing off before actually hitting the rate limit rather than only reacting after the fact. `max_tries=5` remains an independent hard stop regardless of header values.

## 1.26.5
  * Merge the `v1.26.1`-`v1.26.4` fix lineage into `master`:
    - Support bool `include_deleted` config value (Meltano YAML passes a JSON boolean, which previously crashed with `AttributeError: 'bool' object has no attribute 'lower'`)
    - Retry on rate limit errors (Facebook error code 17) instead of aborting immediately
    - Reduce API call batches per stream (`iter_delivery_info_filter` sub-list length raised from 3 to 7), cutting total API volume when `include_deleted` is enabled
  * Fix rate-limit retry gap during pagination: extract a shared `is_transient_facebook_error()` condition used by both `call_with_retry` (the global `FacebookAdsApi.call` monkeypatch every SDK call passes through, including page 2+ of every paginated stream via `Cursor.load_next_page()`) and `retry_pattern` (applied per-stream to the call that creates the cursor). Previously, a code 17 rate limit error retried fine on page 1 but crashed immediately when hit on a later page, since `call_with_retry` only retried a narrow summary-param regex match

## 1.26.0
  * Add `ads_insights_comscore_market` stream to replace deprecated DMA breakdown
  * Deprecate `ads_insights_dma` stream (Meta removed DMA support on June 22, 2026) [#270](https://github.com/singer-io/tap-facebook/pull/270)
  * Add deprecation warning when `ads_insights_dma` stream is selected
  * Reference: https://developers.facebook.com/blog/post/2026/03/13/transitioning-to-comscore-markets-for-automotive-model-ads
  * For DMA to Comscore Market mapping: https://www.facebook.com/business/help/709868688063859

## 1.25.2
  * Bump pendulum dependency from 1.2.0 to 3.2.0 [#269](https://github.com/singer-io/tap-facebook/pull/269)
  * Bump attrs dependency from 17.3.0 to 26.1.0

## 1.25.1
  * Bump requests dependency from 2.32.4 to 2.34.0 [#266](https://github.com/singer-io/tap-facebook/pull/266)

## 1.25.0
  * Bump facebook_business SDK from v23.0.1 to v25.0.1 to stay ahead of v23.0 deprecation (June 9, 2026) [#265](https://github.com/singer-io/tap-facebook/pull/265)
  * Confirmed no schema changes required: `smart_promotion_type` was never present in campaigns schema
  * Add explicit "Job Failed" status handling in async Insights job polling; surface v25.0 error fields (`error_code`, `error_message`, `error_subcode`, `error_user_title`, `error_user_msg`)

## 1.24.0
  * Bump facebook_business SDK to v23.0.1 [#255](https://github.com/singer-io/tap-facebook/pull/255)
  * Remove Deprecated Fields from adcreative [#255](https://github.com/singer-io/tap-facebook/pull/255)

## 1.23.0
  * Add default value of missing pk for ads_insights_hourly_advertiser [#250](https://github.com/singer-io/tap-facebook/pull/250)

## 1.22.1
  * Bump dependency versions for twistlock compliance [#247](https://github.com/singer-io/tap-facebook/pull/247)

## 1.22.0
  * Adds warning when 'reach' is requested for breakdown queries older than 13 months due to Meta API changes  [#245](https://github.com/singer-io/tap-facebook/pull/245)

## 1.21.0
  * Bump facebook_business SDK to v21.0.5 [#242](https://github.com/singer-io/tap-facebook/pull/242)

## 1.20.2
  * Bump facebook_business SDK to v19.0.2 [#238](https://github.com/singer-io/tap-facebook/pull/239)

## 1.20.1
  * Bump facebook_business SDK to v19.0.0 [#238](https://github.com/singer-io/tap-facebook/pull/238)

## 1.20.0
  * Run on python 3.11.7 [#237](https://github.com/singer-io/tap-facebook/pull/237)

## 1.19.1
  * Add retry logic for status code - 503 [#226](https://github.com/singer-io/tap-facebook/pull/226)

## 1.19.0
  * Add conversions to insights streams [#204](https://github.com/singer-io/tap-facebook/pull/204)

## 1.18.6
  * Bump facebook_business SDK to v17.0.2 for token param bug fix [#219](https://github.com/singer-io/tap-facebook/pull/219)

## 1.18.5
  * Bump facebook_business SDK to v16.0.2 [#213](https://github.com/singer-io/tap-facebook/pull/213)

## 1.18.4
  * Facebook business API to v14.0 [#201](https://github.com/singer-io/tap-facebook/pull/201)

## 1.18.3
  * Facebook business API to V13.0 [#191] (https://github.com/singer-io/tap-facebook/pull/191)
## 1.18.2
  * Implemented Request Timeout [#173](https://github.com/singer-io/tap-facebook/pull/173)

## 1.18.1
  * Forced Replication Method implemented for couple of streams and replication keys [167](https://github.com/singer-io/tap-facebook/pull/167)
  * Added Tap-tester test cases [168](https://github.com/singer-io/tap-facebook/pull/168)
  * Updated schema file of ads_insights_age_and_gender and ads_insights_hourly_advertiser and added "format": "date-time" [#172](https://github.com/singer-io/tap-facebook/pull/172)

## 1.17.0
  * Added retry to AdsInsights job polling to resolve race-condition errors [#174](https://github.com/singer-io/tap-facebook/pull/174)

## 1.16.0
  * Bump tap dependency, `facebook_business`, from `10.0.0` to `12.0.0` [#164](https://github.com/singer-io/tap-facebook/pull/164)

## 1.15.1
  * Bump tap dependency, `attrs`, from `16.3.0` to `17.3.0` [#161](https://github.com/singer-io/tap-facebook/pull/161)

## 1.15.0
  * Add `country` to `ad_insights_country`'s composite primary key [#154](https://github.com/singer-io/tap-facebook/pull/154)

## 1.14.0
  * Add an Ads Insight Stream, broken down by `hourly_stats_aggregated_by_advertiser_time_zone` [#151](https://github.com/singer-io/tap-facebook/pull/151)

## 1.13.0
  * Bump API version from `v9` to `v10` [#146](https://github.com/singer-io/tap-facebook/pull/146)
  * Add feature for AdsInsights stream: The tap will shift the start date to 37 months ago in order to fetch data from this API
    * More info [here](https://www.facebook.com/business/help/1695754927158071?id=354406972049255)

## 1.12.1
  * Increased insights job timeout to 300 seconds [#148](https://github.com/singer-io/tap-facebook/pull/148)

## 1.12.0
  * Added leads stream [#143](https://github.com/singer-io/tap-facebook/pull/143)

## 1.11.2
  * Added unique_outbound_clicks to several streams [#138](https://github.com/singer-io/tap-facebook/pull/138)

## 1.11.1
  * Modifies the way FacebookRequestError is parsed [#135](https://github.com/singer-io/tap-facebook/pull/135)

## 1.11.0
  * Upgrades facebook_business library to version 9.0.0 [#133](https://github.com/singer-io/tap-facebook/pull/133)

## 1.10.0
  * Add consistent logging for `facebook_business.exceptions.FacebookError` errors [#129](https://github.com/singer-io/tap-facebook/pull/129)

## 1.9.7
  * Add check for `TypeError` and retry them on the `AdsInsights` and `AdCreative` streams [#126](https://github.com/singer-io/tap-facebook/pull/126)

## 1.9.6
  * Add check for `FacebookBadObjectError` and retry them on the `AdsInsights` stream [#124](https://github.com/singer-io/tap-facebook/pull/124)

## 1.9.5
  * Add check for `HTTP 500` and retry them on the `AdCreatives` stream [#121](https://github.com/singer-io/tap-facebook/pull/121)

## 1.9.4
  * Bump SDK version to get bug fixes [#105](https://github.com/singer-io/tap-facebook/pull/105)

## 1.9.3
  * Bump API version from `v6` to `v8` [#103](https://github.com/singer-io/tap-facebook/pull/103)

## 1.9.2
  * Fix retry pattern for non-insights incremental streams [#100](https://github.com/singer-io/tap-facebook/pull/100)
  * Remove workaround implemented in [#55](https://github.com/singer-io/tap-facebook/pull/55)

## 1.9.1
  * Pass metadata from the catalog to the Transformer to filter out unselected fields [#97](https://github.com/singer-io/tap-facebook/pull/97)

## 1.9.0
  * Bump API version from `v4` to `v6` [#88](https://github.com/singer-io/tap-facebook/pull/88)

## 1.8.2
  * Add `video_play_curve_actions` to ads_insights schemas [#80](https://github.com/singer-io/tap-facebook/pull/80)

## 1.8.1
  * Modifies the sync method of AdCreatives to use the FB Batch endpoint [#73](https://github.com/singer-io/tap-facebook/pull/73)

## 1.8.0
  * Add the ability to override `RESULT_RETURN_LIMIT` from the config [#71](https://github.com/singer-io/tap-facebook/pull/71)
  * Add date-windowing for the `adcreative` stream [#71](https://github.com/singer-io/tap-facebook/pull/71)

## 1.7.1
  * Bump `facebook_business` library to 4.0.5 [#68](https://github.com/singer-io/tap-facebook/pull/68)
  * Remove deprecated `video_p95_watched_actions` field
  * Change calls to `remote_read` to use `api_get` per deprecation [#69](https://github.com/singer-io/tap-facebook/pull/69)

## 1.7.0
  * Replaced `annotated_schema` with Singer `metadata`
    * Fixed unit tests to also use `metadata`
  * Added integration tests to CircleCI

## 1.6.0
  * Add DMA breakdown

## 1.5.12
  * Bump `facebook_business` library to 3.3.2 (#59)(https://github.com/singer-io/tap-facebook/pull/59)

## 1.5.9
  * Restore Insights job timeout, as Facebook seems to have stabilized.

## 1.5.8
  * Reduce Insights job wait to fail faster due to Facebook instability.

## 1.5.7
  * Bump `facebook_business` library to 3.2.0 [#51](https://github.com/singer-io/tap-facebook/pull/51)

## 1.5.6
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 1.5.5
  * Updates the `should_retry_api_error` function to include the error_subcode 99 when Facebook fails to retrieve Ad data

## 1.5.4
  * Add retry_pattern annotations to record_preparation function calls so they don't fall victim to Facebook 500's as often [#48](https://github.com/singer-io/tap-facebook/pull/48)

## 1.5.3
  * Yield records to IncrementalStreams instead of accumulating them [#47](https://github.com/singer-io/tap-facebook/pull/47)

## 1.5.2
  * Bump `facebook_business` library to 3.0.5
  * Remove deprecated fields from ad_insights schemas [FB 2/2018](https://developers.facebook.com/docs/graph-api/changelog/breaking-changes#feb2018)

## 1.5.1
  * Bump Insights job timeout to 120 seconds in an attempt to more closely match Facebook's API behavior.

## 1.5.0
  * Upgraded the Facebook Python API to version 3.0.4 [#44](https://github.com/singer-io/tap-facebook/pull/44)
  * Added outbound_clicks to the ads_insights streams

## 1.4.0
  * Upgraded the Facebook API to version 2.11 [#39](https://github.com/singer-io/tap-facebook/pull/39)
  * Added a new stream for Ads Insights with a breakdown of "region" [#40](https://github.com/singer-io/tap-facebook/pull/40)
