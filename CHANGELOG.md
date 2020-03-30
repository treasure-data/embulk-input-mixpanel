## 0.6.0 - 2020-03-30

* [enhancement] Support JQL script [#65](https://github.com/treasure-data/embulk-input-mixpanel/pull/65)

## 0.5.15 - 2020-01-22

* [enhancement] Update the authentication method to latest [#63](https://github.com/treasure-data/embulk-input-mixpanel/pull/63)

## 0.5.14 - 2018-10-22

* [enhancement] Handle the wrong period during transition from standard to daylight saving time exception [#62](https://github.com/treasure-data/embulk-input-mixpanel/pull/62)

## 0.5.13 - 2018-10-04

* [enhancement] Limit number of returned records in guess and preview [#60](https://github.com/treasure-data/embulk-input-mixpanel/pull/60)

## 0.5.12 - 2017-02-09

* [bug] Fix incorrect from_date [#59](https://github.com/treasure-data/embulk-input-mixpanel/pull/59)

## 0.5.11 - 2017-12-11

* [enhancement] Enable API endpoint configuration [#58](https://github.com/treasure-data/embulk-input-mixpanel/pull/58)

## 0.5.10 - 2017-12-03

* [enhancement] Add logic to detect error from Mixpanel when doing import, add option to failed embulk job if encounter error import [#57](https://github.com/treasure-data/embulk-input-mixpanel/pull/57)

## 0.5.9 - 2017-11-10

* [enhancement] Add upper limit delay to incremental column query [#56](https://github.com/treasure-data/embulk-input-mixpanel/pull/56)

## 0.5.8 - 2017-09-26

* [bug] Fix issue when back_fill data get processed by Mixpanel when plugin is running
* [bug] Revert PR #54

## 0.5.6 - 2017-09-20
* [bug] Fix issue when back_fill data get processed by Mixpanel when plugin is running [#54](https://github.com/treasure-data/embulk-input-mixpanel/pull/54)

## 0.5.5 - 2017-09-11
* [enhancement] Add slice_range configuration [#52](https://github.com/treasure-data/embulk-input-mixpanel/pull/52)

## 0.5.4 - 2017-08-15
* [bug] Fix a bug when `fetch_days` is 1 plugin will fetch 2 days instead of 1 [#51](https://github.com/treasure-data/embulk-input-mixpanel/pull/51)

## 0.5.3 - 2017-08-07
* [enhancement] Allow user to choose to run incremental or not `incremental` option default to true [#50](https://github.com/treasure-data/embulk-input-mixpanel/pull/50)
* [enhancement] Allow user to specify an `incremental_column`, which will be add to the where praramter is API requests[#50](https://github.com/treasure-data/embulk-input-mixpanel/pull/50)
* [enhancement] Allow user to specifiy backfill days, this `back_fill_time` option will tell plugin how many days we look back for data [#50](https://github.com/treasure-data/embulk-input-mixpanel/pull/50)


## 0.5.2 - 2017-07-26
* [enhancement]Enable realtime data export[#47](https://github.com/treasure-data/embulk-input-mixpanel/pull/47)  
* [maintenance]Fix incorrect error message[#49](https://github.com/treasure-data/embulk-input-mixpanel/pull/49)

## 0.5.1 - 2016-12-13
* Enable TCP Keepalive to protect from NAT [#48](https://github.com/treasure-data/embulk-input-mixpanel/pull/48)

## 0.5.0 - 2016-11-18
This version contains compatibility breaking for the default config, but you can use old config `fetch_unknown_columns` in this version.

* [enhancement] Change default value to fetch_unknown_columns: false [#46](https://github.com/treasure-data/embulk-input-mixpanel/pull/46)

## 0.4.7 - 2016-09-08
* [fixed] Retry was only enabled on preview and run. [#45](https://github.com/treasure-data/embulk-input-mixpanel/pull/45)

## 0.4.6 - 2016-09-07
* [enhancement] Retry with too frequency requests error [#44](https://github.com/treasure-data/embulk-input-mixpanel/pull/44)

## 0.4.5 - 2016-09-05
* [fixed] Don't try to guess future date [#43](https://github.com/treasure-data/embulk-input-mixpanel/pull/43)

## 0.4.4 - 2016-09-02
* [enhancement] Reduce memory usage by streaming processing [#42](https://github.com/treasure-data/embulk-input-mixpanel/pull/42)

## 0.4.3 - 2016-03-16
* [enhancement] Custom properties json [#40](https://github.com/treasure-data/embulk-input-mixpanel/pull/40)

## 0.4.2 - 2016-03-08
* [fixed] Fix Range request was not satisfied [#39](https://github.com/treasure-data/embulk-input-mixpanel/pull/39)

## 0.4.1 - 2016-03-08
* [enhancement] Reduce data bytes with range [#38](https://github.com/treasure-data/embulk-input-mixpanel/pull/38)

## 0.4.0 - 2016-03-04

This version contains compatibility breaking. Only support Embulk 0.8 or later since this version, no longer support Embulk 0.7.x or earlier.

* [enhancement] Support json type [#35](https://github.com/treasure-data/embulk-input-mixpanel/pull/35)
* [enhancement] Check Mixpanel availability before run [#37](https://github.com/treasure-data/embulk-input-mixpanel/pull/37)
* [enhancement] Guessing time column as statically [#36](https://github.com/treasure-data/embulk-input-mixpanel/pull/36)
* [enhancement] Reduce guess and preview records [#34](https://github.com/treasure-data/embulk-input-mixpanel/pull/34)
* [maintenance] Use perfect_retry [#33](https://github.com/treasure-data/embulk-input-mixpanel/pull/33)


## 0.3.4 - 2015-11-02

* [enhancement] Create `unknown_columns` only when option is true [#32](https://github.com/treasure-data/embulk-input-mixpanel/pull/32)

## 0.3.3 - 2015-10-29

* [enhancement] Exponential backoff retry [#31](https://github.com/treasure-data/embulk-input-mixpanel/pull/31)
* [enhancement] Treat unguessed columns [#30](https://github.com/treasure-data/embulk-input-mixpanel/pull/30)
* [enhancement] Loosely guess [#27](https://github.com/treasure-data/embulk-input-mixpanel/pull/27)
* [maintenance] Refactor [#26](https://github.com/treasure-data/embulk-input-mixpanel/pull/26)

## 0.3.2 - 2015-10-06

* [enhancement] Support embulk 0.7 [#25](https://github.com/treasure-data/embulk-input-mixpanel/pull/25)

## 0.3.1 - 2015-09-08

* [enhancement] Show ignore dates as range [#23](https://github.com/treasure-data/embulk-input-mixpanel/pull/23) [[Reported by @muga](https://github.com/treasure-data/embulk-input-mixpanel/issues/20). Thanks!!]

## 0.3.0 - 2015-08-31

This version breaks backword compatibility of mixpanel. `days` key in config was changed to `fetch_days`. For detail, please check README.md to modify your config.

* [fixed] Fix the bug 1 day data can't be fetched [#21](https://github.com/treasure-data/embulk-input-mixpanel/pull/21)

## 0.2.1 - 2015-08-26

* [fixed] Fix guess with recently from date [#18](https://github.com/treasure-data/embulk-input-mixpanel/pull/18)
* [fixed] Fix error handling when invalid date set given [#17](https://github.com/treasure-data/embulk-input-mixpanel/pull/17)

## 0.2.0 - 2015-08-17

* [enhanement] Raise config error for unretryable [#15](https://github.com/treasure-data/embulk-input-mixpanel/pull/15) [[Reported by @muga](https://github.com/treasure-data/embulk-input-mixpanel/issues/11). Thanks!!]
* [maintenance] Use everyleaf-embulk_helper [#14](https://github.com/treasure-data/embulk-input-mixpanel/pull/14)
* [enhancement] Support scheduled execution [#13](https://github.com/treasure-data/embulk-input-mixpanel/pull/13) [[Reported by @muga](https://github.com/treasure-data/embulk-input-mixpanel/issues/12). Thanks!!]
* [maintenance] Improve coverage [#10](https://github.com/treasure-data/embulk-input-mixpanel/pull/10)
* [fixed] README: Add description for how to get project's timezone [#9](https://github.com/treasure-data/embulk-input-mixpanel/pull/9)

## 0.1.0 - 2015-07-28

The first release!!
