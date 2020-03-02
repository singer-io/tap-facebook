# Changelog

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
