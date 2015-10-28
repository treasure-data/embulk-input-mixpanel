[![Build Status](https://travis-ci.org/treasure-data/embulk-input-mixpanel.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-mixpanel)
[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel)
[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-mixpanel/coverage)

# Mixpanel input plugin for Embulk

embulk-input-mixpanel is the Embulk input plugin for [Mixpanel](https://mixpanel.com).

## Overview

Required Embulk version >= 0.6.16.

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
- **timezone**: project timezone(string, required)
- **from_date**: From date to export (string, optional, default: today - 2)
  - NOTE: Mixpanel API supports to export data from at least 2 days before to at most the previous day.
- **fetch_days**: Count of days range for exporting (integer, optional, default: from_date - (today - 1))
  - NOTE: Mixpanel doesn't support to from_date > today - 2
- **fetch_unknown_columns**: If you want this plugin fetches unknown (unconfigured in config) columns (boolean, optional, default: true)
  - NOTE: If true, `unknown_columns` column is created and added unknown columns' data.
- **event**: The event or events to filter data (array, optional, default: nil)
- **where**: Expression to filter data (c.f. https://mixpanel.com/docs/api-documentation/data-export-api#segmentation-expressions) (string, optional, default: nil)
- **bucket**:The data backet to filter data (string, optional, default: nil)
- **retry_initial_wait_sec** Wait seconds for exponential backoff initial value (integer, default: 1)
- **retry_limit**: Try to retry this times (integer, default: 5)

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
