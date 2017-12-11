[![Build Status](https://travis-ci.org/treasure-data/embulk-input-mixpanel.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-mixpanel)
[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel)
[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel/coverage)

# Mixpanel input plugin for Embulk

embulk-input-mixpanel is the Embulk input plugin for [Mixpanel](https://mixpanel.com).

## Overview

Required Embulk version >= 0.8.6 (since v0.4.0).

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Setup

### How to get API configuration

This plugin uses API key and API secret for target project. Before you make your config.yml, you should get API key and API secret in mixpanel website.

For API configuration, you should log in mixpanel website, and click "Account" at the header. When you select "Projects" panel, you can get "API Key" and "API Secret" for each project.

### How to get project's timezone

This plugin uses project's timezone to adjust timestamp to UTC.

To get it, you should log in mixpanel website, and click gear icon at the lower left. Then an opened dialog shows timezone at "Timezone" column in "Management" tab.

### Configuration

- **api_key**: project API Key (string, required)
- **api_secret**: project API Secret (string, required)
- **export_endpoint**: the Data Export API's endpoint (string, default to "http://data.mixpanel.com/api/2.0/export")
- **timezone**: project timezone(string, required)
- **from_date**: From date to export (string, optional, default: today - 2)
  - NOTE: Mixpanel API supports to export data from at least 2 days before to at most the previous day.
- **fetch_days**: Count of days range for exporting (integer, optional, default: from_date - (today - 1))
  - NOTE: Mixpanel doesn't support to from_date > today - 2
- **incremental**: Run incremental mode nor not (boolean, optional, default: true)
- **incremental_column**: Column to be add to where query as a constraint for incremental time. Only data that have incremental_column timestamp > than previous latest_fetched_time will be return (string, optional, default: nil)
- **back_fill_time**: Amount of time that will be subtracted from `from_date` to calculate the final `from_date` that will be use for API Request. This is due to Mixpanel caching data on user devices before sending it to Mixpanel server (integer, optional, default: 5)
  - NOTE: Only have effect when incremental is true and incremental_column is specified
- **incremental_column_upper_limit_delay_in_seconds**: When query with incremental column, plugin will lock the upper limit of incremental column query with the job start time, in order to avoid issue with data that commit when the job is running
 ex: `where mp_processing_time <= job_start_time`. The upper limit will be calculated by using job_start_time minus with this configuration parameter. This is to support case when Mixpanel have delay in their processing (integer, optional, default: 0)
- **fetch_unknown_columns**(deprecated): If you want this plugin fetches unknown (unconfigured in config) columns (boolean, optional, default: false)
  - NOTE: If true, `unknown_columns` column is created and added unknown columns' data.
- **fetch_custom_properties**: All custom properties into `custom_properties` key. "custom properties" are not desribed Mixpanel document [1](https://mixpanel.com/help/questions/articles/special-or-reserved-properties), [2](https://mixpanel.com/help/questions/articles/what-properties-do-mixpanels-libraries-store-by-default).  (boolean, optional, default: true)
  - NOTE: Cannot set both `fetch_unknown_columns` and `fetch_custom_properties` to `true`.
- **event**: The event or events to filter data (array, optional, default: nil)
- **where**: Expression to filter data (c.f. https://mixpanel.com/docs/api-documentation/data-export-api#segmentation-expressions) (string, optional, default: nil)
- **bucket**:The data backet to filter data (string, optional, default: nil)
- **retry_initial_wait_sec** Wait seconds for exponential backoff initial value (integer, default: 1)
- **retry_limit**: Try to retry this times (integer, default: 5)
- **allow_partial_import**: Allow plugin to skip errored import (boolean, default: true)

### `fetch_unknown_columns` and `fetch_custom_properties`

If you have such data and set config.yml as below.

| event | $city   | $custom | $foobar |
| ----- | ------- | ------- | ------- |
| ev    | Tokyo   | custom  | foobar  |

(NOTE: `$city` is a [reserved key](https://mixpanel.com/help/questions/articles/what-properties-do-mixpanels-libraries-store-by-default), `$custom` and `$foobar` are not)

```yaml
in:
  type: mixpanel
  api_key: "API_KEY"
  api_secret: "API_SECRET"
  timezone: "US/Pacific"
  from_date: "2015-07-19"
  fetch_days: 5
  columns:
    - {name: event, type: string}
    - {name: $custom, type: string}
```


`fetch_unknown_columns: true` will fetch as:

| event | $custom | unknown_columns (json) |
| ----- | ------- | ----------------- |
| ev    | custom  | `{"$city":"Tokyo", "$foobar": "foobar"}` |

`fetch_custom_properties: true` will fetch as:

| event | $custom | custom_properties (json) |
| ----- | ------- | ----------------- |
| ev    | custom  | `{"$foobar": "foobar"}` |


`fetch_unknown_columns` recognize `$city` and `$foobar` as `unknown_columns` because they are not described in config.yml.

`fetch_custom_properties` recognize `$foobar` as `custom_properties`. `$custom` is also custom property but it was described in config.yml.

## Example

```yaml
in:
  type: mixpanel
  api_key: "API_KEY"
  api_secret: "API_SECRET"
  timezone: "US/Pacific"
  from_date: "2015-07-19"
  fetch_days: 5
```

## Run test

```
$ rake
```
