[![Build Status](https://travis-ci.org/treasure-data/embulk-input-mixpanel.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-mixpanel)

# Mixpanel input plugin for Embulk

TODO: Write short description here and embulk-input-mixpanel.gemspec file.

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: yes
* **Guess supported**: no

## Setup

### How to get API configuration

This plugin uses API key and API token for target project. Before you make your config.yml, you should get API key and API token in mixpanel website.

For API configuration, you should log in mixpanel website, and click "Account" at the header. When you select "Projects" panel, you can get "API Key" and "API Token" for each project.

### Configuration

- **api_key**: project API Key (string, required)
- **api_token**: project API Token (string, required)
- **timezone**: project timezone(string, required)
- **from_date**: From date to export (string, optional, default: Today)
- **to_date**: To date to export (string, optional, default: Today)
- **event**: The event or events to filter data (array, optional, default: nil)
- **where**: Expression to filter data (c.f. https://mixpanel.com/docs/api-documentation/data-export-api#segmentation-expressions) (string, optional, default: nil)
- **bucket**:The data backet to filter data (string, optional, default: nil)

## Example

```yaml
in:
  type: mixpanel
  api_key: "API_KEY"
  api_token: "API_TOKEN"
  timezone: "US/Pacific"
  from_date: "2015-07-19"
  to_date: "2015-07-20"
```

## Run test

```
$ rake
```
