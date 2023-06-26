# Changelog

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
