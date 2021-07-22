# tap-facebook
This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [Facebook Marketing API](https://developers.facebook.com/docs/marketing-apis)
- Extracts the following resources from Facebook for one Ad account:
  - Ad Creatives
  - Ads
  - Ad Sets
  - Campaigns
  - Leads
  - Ads Insights
    - Breakdown by age and gender
    - Breakdown by country
    - Breakdown by placement and device
    - Breakdown by region
    - Breakdown by the hour for advertisers
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

### Install

We recommend using a virtualenv:

```bash
> virtualenv -p python3 venv
> source venv/bin/activate
> pip install tap-facebook
```

### Create a Facebook Ads App

To use the Facebook Marketing API, you must create a Facebook Ads app. By creating a Facebook Ads app you will be able to use the Marketing API. [Create an app](https://developers.facebook.com/docs/marketing-apis)

Facebook has three access levels for the Marketing API. You can use this Tap with all three levels. Learn more about these levels in [Facebook documentation](https://developers.facebook.com/docs/marketing-api/access). https://developers.facebook.com/docs/marketing-api/access

### Get an access token

The Tap will need to use an access token to make authenticated requests to the Marketing API.

### Create the config file

The Facebook Tap will use an access token generated by the OAuth process. Additionally you will need:

  **start_date** - an initial date for the Tap to extract data  
  **account_id** - The Facebook Ad account id use when extracting data
  **access_token** - Token generated by Facebook OAuth handshake

The following is an example of the required configuration

```json
{"start_date":"",
"account_id":"",
"access_token":""}
```

### Create a properties file

The properties file will indicate what streams and fields to replicate from the Facebook Marketing API. The Tap takes advantage of the Singer best practices for [schema discovery and property selection](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

### [Optional] Create the initial state file

You can provide JSON file that contains a date for the streams to force the application to only fetch data newer than those dates. If you omit the file it will fetch all data for the selected streams.

```json
{"ads":"2017-01-01T00:00:00Z",
 "adcreative":"2017-01-01T00:00:00Z",
 "ads_insights":"2017-01-01T00:00:00Z"}
```

### Run the Tap

`tap-facebook -c config.json -p properties.json -s state.json`

---

Copyright &copy; 2018 Stitch
