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
