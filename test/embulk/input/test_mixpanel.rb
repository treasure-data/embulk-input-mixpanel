require "prepare_embulk"
require "override_assert_raise"
require "embulk/input/mixpanel"
require "json"

module Embulk
  module Input
    class MixpanelTest < Test::Unit::TestCase
      include OverrideAssertRaise

      API_KEY = "api_key".freeze
      API_SECRET = "api_secret".freeze
      FROM_DATE = "2015-02-22".freeze
      TO_DATE = "2015-03-02".freeze
      DAYS = 8
      DATES = Date.parse(FROM_DATE)..(Date.parse(FROM_DATE) + DAYS - 1)
      TIMEZONE = "Asia/Tokyo".freeze

      DURATIONS = [
        {from_date: FROM_DATE, to_date: "2015-02-28"}, # It has 7 days between 2015-02-22 and 2015-02-28
        {from_date: "2015-03-01", to_date: TO_DATE},
      ]

      def setup
        setup_client
        setup_logger
        stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? { true }
      end

      def setup_client
        params = {
          api_key: API_KEY,
          event: nil,
          where: nil,
          bucket: nil,
        }

        any_instance_of(MixpanelApi::Client) do |klass|
          DURATIONS.each do |duration|
            from_date = duration[:from_date]
            to_date = duration[:to_date]

            params = params.merge(from_date: from_date, to_date: to_date)
            stub(klass).export(params) { records }
          end
        end
      end

      def setup_logger
        stub(Embulk).logger { ::Logger.new(IO::NULL) }
      end

      class GuessTest < self
        def setup
          # Do nothing from parent
          mute_warn
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? { true }
        end

        def test_from_date_old_date
          config = {
            type: "mixpanel",
            api_key: API_KEY,
            api_secret: API_SECRET,
            from_date: FROM_DATE,
          }

          stub_export_all
          mock(Embulk.logger).info(/^Guessing.*#{Regexp.escape FROM_DATE}\.\./)

          actual = Mixpanel.guess(embulk_config(config))
          assert_equal(expected, actual)
        end

        def test_from_date_today
          config = {
            type: "mixpanel",
            api_key: API_KEY,
            api_secret: API_SECRET,
            from_date: Date.today.to_s,
          }

          stub_export_all
          mock(Embulk.logger).info(/Guessing.*#{Regexp.escape Mixpanel.default_guess_start_date.to_s}/)

          Mixpanel.guess(embulk_config(config))
        end

        def test_from_date_yesterday
          from_date = (Date.today - 1).to_s
          config = {
            type: "mixpanel",
            api_key: API_KEY,
            api_secret: API_SECRET,
            from_date: from_date,
          }

          stub_export_all
          mock(Embulk.logger).info(/Guessing.*#{Regexp.escape from_date}/)

          Mixpanel.guess(embulk_config(config))
        end

        def test_no_from_date
          config = {
            type: "mixpanel",
            api_key: API_KEY,
            api_secret: API_SECRET,
          }

          stub_export_all
          mock(Embulk.logger).info(/Guessing.*#{Regexp.escape Mixpanel.default_guess_start_date.to_s}/)

          Mixpanel.guess(embulk_config(config))
        end

        def test_json_type
          sample_records = records.map do |r|
            r.merge("properties" => {"array" => [1,2], "hash" => {foo: "FOO"}})
          end
          actual = Mixpanel.guess_from_records(sample_records)
          assert actual.include?(name: "array", type: :json)
          assert actual.include?(name: "hash", type: :json)
        end

        def test_mixpanel_is_down
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? { false }
          config = {
            type: "mixpanel",
            api_key: API_KEY,
            api_secret: API_SECRET,
          }

          assert_raise(Embulk::DataError) do
            Mixpanel.guess(embulk_config(config))
          end
        end

        def test_too_many_requests
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? { true }

          httpclient = HTTPClient.new
          httpclient.test_loopback_http_response << [
            "HTTP/1.1 422",
            "Content-Type: application/json",
            "",
            {error: "too many export requests in progress for this project"}.to_json
          ].join("\r\n")

          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).httpclient { httpclient }
            mock(klass).request_for_each_day(anything, MixpanelApi::Client::SMALLSET_BYTE_MAX) do
              records.map(&:to_json).join("\n")
            end
          end

          stub(Embulk.logger).info
          mock(Embulk.logger).warn(/Retry/)

          assert_nothing_raised do
            Mixpanel.guess(embulk_config(config))
          end
        end

        private

        def stub_export_all
          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).export_for_small_dataset(anything) { records }
          end
        end

        def mute_warn
          stub(Embulk.logger).warn(anything) {}
        end

        def embulk_config(config)
          DataSource[*config.to_a.flatten(1)]
        end

        def expected
          {
            "columns" => [
              {name: "time", type: :long},
              {name: "event", type: :string},
              {name: "foo", type: :string},
              {name: "int", type: :long},
            ]
          }
        end
      end

      class TransactionDateTest < self
        def test_valid_from_date
          from_date = "2015-08-14"
          mock(Mixpanel).resume(anything, anything, 1)

          Mixpanel.transaction(transaction_config(from_date))
        end

        def test_invalid_from_date
          from_date = "2015-08-41"

          assert_raise(Embulk::ConfigError) do
            Mixpanel.transaction(transaction_config(from_date))
          end
        end

        def test_future
          from_date = (Date.today + 10).to_s
          mock(Mixpanel).resume(anything, anything, 1)

          Mixpanel.transaction(transaction_config(from_date))
        end

        def test_negative_days
          assert_raise(Embulk::ConfigError) do
            Mixpanel.transaction(transaction_config((Date.today - 1).to_s).merge(fetch_days: -1))
          end
        end

        private

        def transaction_config(from_date)
          _config = config.merge(
            from_date: from_date,
            timezone: TIMEZONE,
            columns: schema,
          )
          DataSource[*_config.to_a.flatten(1)]
        end
      end

      class TransactionTest < self
        class FromDateTest < self
          def setup
          end

          def test_ignore_early_days
            stub(Embulk).logger { Logger.new(File::NULL) }

            mock(Mixpanel).resume(task.merge(dates: target_dates), columns, 1, &control)
            Mixpanel.transaction(transaction_config, &control)
          end

          def test_info
            stub(Mixpanel).resume(task.merge(dates: target_dates), columns, 1, &control)

            info_message_regexp = /#{Regexp.escape(target_dates.first)}.+#{Regexp.escape(target_dates.last)}/
            mock(Embulk.logger).info(info_message_regexp)
            stub(Embulk.logger).warn

            Mixpanel.transaction(transaction_config, &control)
          end

          def test_warn
            stub(Mixpanel).resume(task.merge(dates: target_dates), columns, 1, &control)
            stub(Embulk.logger).info

            ignore_dates = dates.map{|date| date.to_s}.to_a - target_dates
            warn_message_regexp = /#{Regexp.escape(ignore_dates.first)}.+#{Regexp.escape(ignore_dates.last)}/
            mock(Embulk.logger).warn(warn_message_regexp)

            Mixpanel.transaction(transaction_config, &control)
          end

          private

          def dates
            (Date.today - 10)..(Date.today + 10)
          end

          def target_dates
            dates.find_all{|d| d < Date.today}.map {|date| date.to_s}
          end

          def transaction_config
            _config = config.merge(
              from_date: dates.first.to_s,
              fetch_days: dates.to_a.size,
              timezone: TIMEZONE,
              columns: schema
            )
            DataSource[*_config.to_a.flatten(1)]
          end
        end

        class TimezoneTest < self
          def test_valid_timezone
            timezone = TIMEZONE
            mock(Mixpanel).resume(transaction_task(timezone), columns, 1, &control)

            Mixpanel.transaction(transaction_config(timezone), &control)
          end

          def test_invalid_timezone
            timezone = "#{TIMEZONE}ooooo"

            assert_raise(Embulk::ConfigError) do
              Mixpanel.transaction(transaction_config(timezone), &control)
            end
          end

          private

          def transaction_task(timezone)
            task.merge(
              dates: DATES.map {|date| date.to_s},
              api_key: API_KEY,
              api_secret: API_SECRET,
              timezone: timezone,
              schema: schema
            )
          end

          def transaction_config(timezone)
            _config = config.merge(
              timezone: timezone,
              columns: schema,
            )
            DataSource[*_config.to_a.flatten(1)]
          end
        end

        class DaysTest < self
          def test_valid_days
            days = 5

            mock(Mixpanel).resume(transaction_task(days), columns, 1, &control)
            Mixpanel.transaction(transaction_config(days), &control)
          end

          def test_invalid_days
            days = 0

            assert_raise(Embulk::ConfigError) do
              Mixpanel.transaction(transaction_config(days), &control)
            end
          end

          private

          def transaction_task(days)
            from_date = Date.parse(FROM_DATE)
            task.merge(
              dates: (from_date..(from_date + days - 1)).map {|date| date.to_s},
              api_key: API_KEY,
              api_secret: API_SECRET,
              timezone: TIMEZONE,
              schema: schema
            )
          end

          def transaction_config(days)
            _config = config.merge(
              fetch_days: days,
              columns: schema,
              timezone: TIMEZONE,
            )
            DataSource[*_config.to_a.flatten(1)]
          end
        end

        class TestCustomProps < self
          setup do
            stub(Mixpanel).resume {}
          end

          data(
            "false/false" => [false, false],
            "false/true" => [false, true],
            "true/false" => [true, false],
          )
          def test_valid_combination(data)
            fetch_unknown_columns, fetch_custom_properties = data
            conf = DataSource[*transaction_config.merge(fetch_unknown_columns: fetch_unknown_columns, fetch_custom_properties: fetch_custom_properties).to_a.flatten(1)]

            assert_nothing_raised do
              Mixpanel.transaction(conf, &control)
            end
          end

          def test_both_true_then_raise_config_error
            conf = DataSource[*transaction_config.merge(fetch_unknown_columns: true, fetch_custom_properties: true).to_a.flatten(1)]

            assert_raise(Embulk::ConfigError) do
              Mixpanel.transaction(conf, &control)
            end
          end

          private

          def transaction_config
            config.merge(
              columns: schema,
              fetch_days: 2,
              timezone: "UTC",
            )
          end
        end

        def test_resume
          today = Date.today
          control = proc { [{to_date: today.to_s}] }
          actual = Mixpanel.resume(transaction_task, columns, 1, &control)
          assert_equal({from_date: today.next.to_s}, actual)
        end

        def control
          proc {} # dummy
        end

        def transaction_task
          task.merge(
            dates: DATES.map {|date| date.to_s},
            api_key: API_KEY,
            api_secret: API_SECRET,
            timezone: TIMEZONE,
            schema: schema
          )
        end

        def columns
          schema.map do |col|
            Column.new(nil, col["name"], col["type"].to_sym)
          end
        end
      end

      def test_export_params
        config_params = [
          :type, "mixpanel",
          :api_key, API_KEY,
          :api_secret, API_SECRET,
          :from_date, FROM_DATE,
          :to_date, TO_DATE,
          :event, ["ViewHoge", "ViewFuga"],
          :where, 'properties["$os"] == "Windows"',
          :bucket, "987",
        ]

        config = DataSource[*config_params]

        expected = {
          api_key: API_KEY,
          event: "[\"ViewHoge\",\"ViewFuga\"]",
          where: 'properties["$os"] == "Windows"',
          bucket: "987",
        }
        actual = Mixpanel.export_params(config)

        assert_equal(expected, actual)
      end

      sub_test_case "retry" do
        def setup
          @page_builder = Object.new
          @plugin = Mixpanel.new(task, nil, nil, @page_builder)
          @plugin.init
          @httpclient = HTTPClient.new
          stub(HTTPClient).new { @httpclient }
          stub(@page_builder).add {}
          stub(@page_builder).finish {}
          stub(Embulk.logger).warn {}
          stub(Embulk.logger).info {}
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? { true }
        end

        test "200" do
          stub_response(200)
          mock(Embulk.logger).warn(/Retrying/).never
          mock(@page_builder).finish
          @plugin.run
        end

        test "400" do
          stub_response(400)
          mock(Embulk.logger).warn(/Retrying/).never
          assert_raise(Embulk::ConfigError) do
            @plugin.run
          end
        end

        test "401" do
          stub_response(401)
          mock(Embulk.logger).warn(/Retrying/).never
          assert_raise(Embulk::ConfigError) do
            @plugin.run
          end
        end

        test "500" do
          stub_response(500)
          mock(Embulk.logger).warn(/Retrying/).times(task[:retry_limit])
          assert_raise(PerfectRetry::TooManyRetry) do
            @plugin.run
          end
        end

        test "timeout" do
          stub(@httpclient).get { raise HTTPClient::TimeoutError, "timeout" }
          mock(Embulk.logger).warn(/Retrying/).times(task[:retry_limit])

          assert_raise(PerfectRetry::TooManyRetry) do
            @plugin.run
          end
        end

        test "Mixpanel is down" do
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? { false }

          assert_raise(Embulk::DataError) do
            @plugin.run
          end
        end

        def stub_response(code)
          stub(@httpclient.test_loopback_http_response).shift { "HTTP/1.1 #{code}\r\n\r\n" }
        end

        def task
          {
            api_key: API_KEY,
            api_secret: API_SECRET,
            timezone: TIMEZONE,
            schema: schema,
            dates: DATES.to_a.map(&:to_s),
            params: Mixpanel.export_params(embulk_config),
            fetch_unknown_columns: false,
            fetch_custom_properties: false,
            retry_initial_wait_sec: 0,
            retry_limit: 3,
          }
        end
      end

      class RunTest < self
        def setup_client

          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).request { records_raw_response }
          end
        end

        def setup
          super

          @page_builder = Object.new
          @plugin = Mixpanel.new(task, nil, nil, @page_builder)
        end

        def test_preview
          stub(@plugin).preview? { true }
          mock(@page_builder).add(anything).times(records.length)
          mock(@page_builder).finish

          @plugin.run
        end

        def test_run
          stub(@plugin).preview? { false }
          mock(@page_builder).add(anything).times(records.length * 2)
          mock(@page_builder).finish

          @plugin.run
        end

        def test_timezone
          stub(@plugin).preview? { false }
          adjusted = record_epoch - timezone_offset_seconds
          mock(@page_builder).add(["FOO", adjusted, "event"]).times(records.length * 2)
          mock(@page_builder).finish

          @plugin.run
        end

        class CustomPropertiesTest < self
          def setup
            super
            @page_builder = Object.new
            @plugin = Mixpanel.new(task, nil, nil, @page_builder)
            stub(@plugin).fetch { [record] }
          end

          def test_run
            stub(@plugin).preview? { false }

            custom_property_keys = %w($foobar)

            added = [
              record["event"],
              record["properties"]["$specified"],
              custom_property_keys.map{|k| {k => record["properties"][k] }}.inject(&:merge)
            ]

            mock(@page_builder).add(added).at_least(1)
            mock(@page_builder).finish

            @plugin.run
          end

          private

          def task
            super.merge(schema: schema, fetch_unknown_columns: false, fetch_custom_properties: true)
          end

          def record
            {
              "event" => "EV",
              "properties" => {
                "$os" => "Android",
                "$specified" => "foo",
                "$foobar" => "foobar",
              }
            }
          end

          def schema
            [
              {"name" => "event", "type" => "string"},
              {"name" => "$specified", "type" => "string"},
            ]
          end
        end

        class UnknownColumnsTest < self
          def setup
            super
            @page_builder = Object.new
            @plugin = Mixpanel.new(task, nil, nil, @page_builder)
          end

          def test_run
            Embulk.logger.warn(anything)
            stub(@plugin).preview? { false }

            # NOTE: Expect records are contained same record
            record = records.first
            properties = record["properties"]

            time = properties["time"]
            tz = TZInfo::Timezone.get(TIMEZONE)
            offset = tz.period_for_local(time, true).offset.utc_offset
            adjusted_time = time - offset

            added = [
              properties["foo"],
              adjusted_time,
              {"int" => properties["int"], "event" => record["event"]}.to_json
            ]

            mock(@page_builder).add(added).times(records.length * 2)
            mock(@page_builder).finish

            @plugin.run
          end

          private

          def task
            super.merge(schema: schema, fetch_unknown_columns: true)
          end

          def schema
            [
              {"name" => "foo", "type" => "long"},
              {"name" => "time", "type" => "long"},
            ]
          end
        end

        private

        def timezone_offset_seconds
          60 * 60 * 9 # Asia/Tokyo
        end
      end

      private

      def schema
        [
          {"name" => "foo", "type" => "long"},
          {"name" => "time", "type" => "long"},
          {"name" => "event", "type" => "string"},
        ]
      end

      def task
        {
          api_key: API_KEY,
          api_secret: API_SECRET,
          timezone: TIMEZONE,
          schema: schema,
          dates: DATES.to_a.map(&:to_s),
          params: Mixpanel.export_params(embulk_config),
          fetch_unknown_columns: false,
          fetch_custom_properties: false,
          retry_initial_wait_sec: 2,
          retry_limit: 3,
        }
      end

      def records
        [
          {
            "event" => "event",
            "properties" => {
              "foo" => "FOO",
              "time" => record_epoch,
              "int" => 42,
            }
          },
        ] * 30
      end

      def records_raw_response
        records.map(&:to_json).join("\n")
      end

      def record_epoch
        1234567890
      end

      def config
        {
          type: "mixpanel",
          api_key: API_KEY,
          api_secret: API_SECRET,
          from_date: FROM_DATE,
          fetch_days: DAYS,
          fetch_unknown_columns: false,
          fetch_custom_properties: false,
          retry_initial_wait_sec: 2,
          retry_limit: 3,
        }
      end

      def embulk_config
        DataSource[*config.to_a.flatten(1)]
      end
    end
  end
end
