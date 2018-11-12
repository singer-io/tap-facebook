# Changelog

## 1.5.6
  * Update version of `requests` to `0.20.0` in response to CVE 2018-18074

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
