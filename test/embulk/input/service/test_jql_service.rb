require "prepare_embulk"
require "override_assert_raise"
require "embulk/input/mixpanel"
require "embulk/input/service/base_service"
require "embulk/input/service/jql_service"
require "active_support/core_ext/time"
require "json"

module Embulk
  module Input
    class JQLServiceTest < Test::Unit::TestCase
      include OverrideAssertRaise

      API_SECRET = "api_secret".freeze
      FROM_DATE = "2015-02-22".freeze
      TO_DATE = "2015-03-02".freeze
      DAYS = 8
      SLICE_RANGE = 10
      DATES = Date.parse(FROM_DATE)..(Date.parse(FROM_DATE) + DAYS - 1)
      TIMEZONE = "Asia/Tokyo".freeze
      SMALL_NUM_OF_RECORDS = 10
      DURATIONS = [
        {from_date: FROM_DATE, to_date: "2015-02-28"}, # It has 7 days between 2015-02-22 and 2015-02-28
        {from_date: "2015-03-01", to_date: TO_DATE},
      ]
      JQL_SCRIPT = 'function main() { return Events({ from_date: "2015-01-01", to_date:"2015-01-02"}'
      JQL_SCRIPT_WITH_PARAMS = 'function main() { return Events({ from_date: params.from_date, to_date: to_date}'

      def setup
        setup_client
        setup_logger
        stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? {true}
      end

      def setup_client
        params = {
          params: nil,
          script: nil,
        }

        any_instance_of(MixpanelApi::Client) do |klass|
          DURATIONS.each do |duration|
            from_date = duration[:from_date]
            to_date = duration[:to_date]

            params = params.merge(from_date: from_date, to_date: to_date)
            stub(klass).send_jql_script_small_dataset(anything) {records}
          end
        end
      end

      def satisfy_task_ignore_start_time(expected_task)
        satisfy {|input_task|
          assert_equal(expected_task, input_task)
          true
        }
      end

      def setup_logger
        stub(Embulk).logger {::Logger.new(IO::NULL)}
      end

      class GuessTest < self
        def setup
          # Do nothing from parent
          mute_warn
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? {true}
        end

        def test_from_date_old_date
          config = {
            type: "mixpanel",
            jql_mode: true,
            incremental: false,
            jql_script: JQL_SCRIPT,
            api_secret: API_SECRET,
            from_date: FROM_DATE,
            timezone: TIMEZONE,
          }

          stub_export_all
          mock(Embulk.logger).info(/^Guessing.*#{Regexp.escape FROM_DATE}\.\./)

          actual = Mixpanel.guess(embulk_config(config))
          assert_equal(expected, actual)
        end

        def test_from_date_future
          config = {
            type: "mixpanel",
            api_secret: API_SECRET,
            jql_mode: true,
            incremental: false,
            jql_script: JQL_SCRIPT,
            timezone: TIMEZONE,
            from_date: (today + 1).to_s
          }

          stub_export_all
          mock(Embulk.logger).info(/Guessing.*#{Regexp.escape Embulk::Input::Service::JqlService.new(config).default_guess_start_date(TIMEZONE).to_s}/)

          Mixpanel.guess(embulk_config(config))
        end

        def test_from_date_yesterday
          from_date = (today - 1).to_s
          config = {
            type: "mixpanel",
            api_secret: API_SECRET,
            from_date: from_date,
            timezone: TIMEZONE,
            incremental: false,
            jql_mode: true,
            jql_script: JQL_SCRIPT,
          }

          stub_export_all
          mock(Embulk.logger).info(/Guessing.*#{Regexp.escape from_date}/)

          Mixpanel.guess(embulk_config(config))
        end

        def test_no_from_date
          config = {
            type: "mixpanel",
            api_secret: API_SECRET,
            timezone: TIMEZONE,
            jql_mode: true,
            incremental: false,
            jql_script: JQL_SCRIPT,
          }

          stub_export_all
          mock(Embulk.logger).info(/Guessing.*#{Regexp.escape Embulk::Input::Service::JqlService.new(config).default_guess_start_date(TIMEZONE).to_s}/)

          Mixpanel.guess(embulk_config(config))
        end

        def test_json_type
          sample_records = records.map do |r|
            r.merge("properties"=>{"time"=>1, "array"=>[1, 2], "hash"=>{foo: "FOO"}})
          end

          config = {
            type: "mixpanel",
            api_secret: API_SECRET,
            timezone: TIMEZONE,
            jql_mode: true,
            incremental: false,
            jql_script: JQL_SCRIPT,
          }

          actual = Embulk::Input::Service::JqlService.new(config).guess_from_records(sample_records)

          assert actual.include?(name: "properties", type: :json)
        end

        def test_mixpanel_is_down
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? {false}
          config = {
            type: "mixpanel",
            api_secret: API_SECRET,
            timezone: TIMEZONE,
            jql_mode: true,
            incremental: false,
            jql_script: JQL_SCRIPT,
          }

          assert_raise(Embulk::DataError) do
            Mixpanel.guess(embulk_config(config))
          end
        end

        private

        def stub_export_all
          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).send_brief_checked_jql_script(anything) {}
            stub(klass).send_jql_script_small_dataset(anything) {records}
          end
        end

        def mute_warn
          stub(Embulk.logger).warn(anything) {}
        end

        def embulk_config(config)
          DataSource[*config.to_a.flatten(1)]
        end

        def expected
          {"columns"=>
            [{:name=>:name, :type=>:string},
              {:name=>:distinct_id, :type=>:string},
              {:name=>:labels, :type=>:json},
              {:name=>:time, :type=>:long},
              {:name=>:sampling_factor, :type=>:long},
              {:name=>:dataset, :type=>:string},
              {:name=>:properties, :type=>:json}]
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
          from_date = (today + 10).to_s
          mock(Mixpanel).resume(anything, anything, 1)

          Mixpanel.transaction(transaction_config(from_date))
        end

        def test_negative_days
          assert_raise(Embulk::ConfigError) do
            Mixpanel.transaction(transaction_config((today - 1).to_s).merge(fetch_days: -1))
          end
        end

        def test_default_configuration
          stub(Mixpanel).resume {|task|
            assert_true(task[:jql_mode])
            assert_true(task[:incremental])
          }
          Mixpanel.transaction(transaction_config(today))
        end

        private

        def transaction_config(from_date)
          _config = config.merge(
            from_date: from_date,
            timezone: TIMEZONE,
            columns: schema,
            jql_mode: true,
            jql_script: JQL_SCRIPT,
          )
          DataSource[*_config.to_a.flatten(1)]
        end
      end

      class TransactionTest < self
        class FromDateTest < self
          def setup
            any_instance_of(MixpanelApi::Client) do |klass|
              stub(klass).send_brief_checked_jql_script(anything) {}
            end
          end

          def test_info
            stub(Mixpanel).resume(satisfy_task_ignore_start_time(task.merge(dates: target_dates)), columns, 1, &control)

            info_message_regexp = /#{Regexp.escape(target_dates.first)}.+#{Regexp.escape(target_dates.last)}/
            mock(Embulk.logger).info(info_message_regexp)
            stub(Embulk.logger).warn

            Mixpanel.transaction(transaction_config, &control)
          end

          def test_warn
            stub(Mixpanel).resume(satisfy_task_ignore_start_time(task.merge(dates: target_dates)), columns, 1, &control)
            stub(Embulk.logger).info

            ignore_dates = dates.map {|date| date.to_s}.to_a - target_dates
            warn_message_regexp = /#{Regexp.escape(ignore_dates.first)}.+#{Regexp.escape(ignore_dates.last)}/
            mock(Embulk.logger).warn(warn_message_regexp)

            Mixpanel.transaction(transaction_config, &control)
          end

          def test_warn_jql_script_contain_time_params
            stub(Mixpanel).resume(satisfy_task_ignore_start_time(task.merge({dates: target_dates, jql_script: JQL_SCRIPT_WITH_PARAMS})), columns, 1, &control)
            stub(Embulk.logger).info

            error_response = {"request"=>"/api/2.0/jql/", "error"=>"[Validate failed]Events() argument must be an object with to_date' properties\n"}

            any_instance_of(MixpanelApi::Client) do |klass|
              stub(klass).send_brief_checked_jql_script(anything) {error_response}
              stub(klass).send_jql_script_small_dataset(anything) {records}
            end

            mock(Embulk.logger).warn(anything)
            mock(Embulk.logger).warn("Missing params.start_date and params.end_date in the JQL. Use these parameters to limit the amount of returned data.")

            Mixpanel.transaction(transaction_config.merge("jql_script"=>JQL_SCRIPT_WITH_PARAMS), &control)
          end

          private

          def dates
            (today - 10)..(today + 10)
          end

          def target_dates
            dates.find_all {|d| d <= today}.map {|date| date.to_s}
          end

          def transaction_config
            _config = config.merge(
              from_date: dates.first.to_s,
              fetch_days: dates.to_a.size,
              timezone: TIMEZONE,
              columns: schema,
              jql_mode: true,
              jql_script: JQL_SCRIPT,
            )
            DataSource[*_config.to_a.flatten(1)]
          end
        end

        class TimezoneTest < self
          def test_valid_timezone
            timezone = TIMEZONE
            mock(Mixpanel).resume(satisfy_task_ignore_start_time(transaction_task(timezone)), columns, 1, &control)
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
              api_secret: API_SECRET,
              incremental: true,
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

            mock(Mixpanel).resume(satisfy_task_ignore_start_time(transaction_task(days)), columns, 1, &control)
            Mixpanel.transaction(transaction_config(days), &control)
          end

          def test_next_to_date
            next_config_diff = Mixpanel.resume(transaction_task(1).merge(incremental: true), columns, 1) do
              [{to_date: today.to_s, latest_fetched_time: 1502707247000}]
            end
            assert_equal((today).to_s, next_config_diff[:from_date])
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
              api_secret: API_SECRET,
              timezone: TIMEZONE,
              schema: schema,
              jql_mode: true,
              jql_script: JQL_SCRIPT,
            )
          end

          def transaction_config(days)
            _config = config.merge(
              fetch_days: days,
              columns: schema,
              timezone: TIMEZONE,
              jql_mode: true,
              jql_script: JQL_SCRIPT,
            )
            DataSource[*_config.to_a.flatten(1)]
          end
        end

        class ValidateTest < self
          def setup_client
            any_instance_of(MixpanelApi::Client) do |klass|
              stub(klass).send_jql_script(anything) {[1]}
              stub(klass).send_jql_script_small_dataset(anything) {[1]}
            end
          end

          def setup
            super
            @page_builder = Object.new
            @plugin = Mixpanel.new(task, nil, nil, @page_builder)
          end

          def test_unsupport_data_format
            assert_raise(Embulk::ConfigError) do
              Mixpanel.guess(embulk_config)
            end
          end
        end

        def control
          proc {} # dummy
        end

        def transaction_task
          task.merge(
            dates: DATES.map {|date| date.to_s},
            api_secret: API_SECRET,
            timezone: TIMEZONE,
            schema: schema,
            jql_mode: true,
            jql_script: JQL_SCRIPT,
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
          :api_secret, API_SECRET,
          :from_date, FROM_DATE,
          :to_date, TO_DATE,
          :jql_mode, true,
          :jql_script, JQL_SCRIPT,
        ]

        config = DataSource[*config_params]

        expected = {
          params: {:from_date=>:from_date, :to_date=>:today},
          script: "function main() { return Events({ from_date: \"2015-01-01\", to_date:\"2015-01-02\"}",
        }
        actual = Embulk::Input::Service::JqlService.new(config).parameters(JQL_SCRIPT, :from_date, :today)

        assert_equal(expected, actual)
      end

      sub_test_case "retry" do
        def setup
          @page_builder = Object.new
          @plugin = Mixpanel.new(task, nil, nil, @page_builder)
          @plugin.init
          @httpclient = HTTPClient.new
          stub(HTTPClient).new {@httpclient}
          stub(@page_builder).add {}
          stub(@page_builder).finish {}
          stub(Embulk.logger).warn {}
          stub(Embulk.logger).info {}
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? {true}
        end

        test "200 and don't support format" do
          stub_response(200)
          mock(Embulk.logger).warn(/Retrying/).never
          assert_raise(Embulk::DataError) do
            @plugin.run
          end
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
          stub(@httpclient).post {raise HTTPClient::TimeoutError, "timeout"}
          mock(Embulk.logger).warn(/Retrying/).times(task[:retry_limit])

          assert_raise(PerfectRetry::TooManyRetry) do
            @plugin.run
          end
        end

        test "Mixpanel is down" do
          stub(Embulk::Input::MixpanelApi::Client).mixpanel_available? {false}

          assert_raise(Embulk::ConfigError) do
            @plugin.run
          end
        end

        def stub_response(code)
          stub(@httpclient.test_loopback_http_response).shift {"HTTP/1.1 #{code} \r\n\r\n"}
        end

        def task
          {
            api_secret: API_SECRET,
            jql_endpoint: "https://mixpanel.com/api/2.0/jql/",
            timezone: TIMEZONE,
            incremental: true,
            schema: schema,
            dates: DATES.to_a.map(&:to_s),
            retry_initial_wait_sec: 2,
            retry_limit: 3,
            jql_mode: true,
            jql_script: JQL_SCRIPT,
            slice_range: SLICE_RANGE,
          }
        end
      end

      class RunTest < self
        def setup_client
          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).send_jql_script(anything) {records}
          end
        end

        def setup
          super
          @page_builder = Object.new
          @plugin = Mixpanel.new(DataSource[task.to_a], nil, nil, @page_builder)
        end

        def test_run
          any_instance_of(Embulk::Input::Service::JqlService) do |klass|
            stub(klass).preview? {false}
          end
          mock(@page_builder).add(anything).times(records.length)
          mock(@page_builder).finish
          task_report = @plugin.run
          assert_equal("2015-03-01", task_report[:to_date])
        end

        def test_run_with_incremental_column
          any_instance_of(Embulk::Input::Service::JqlService) do |klass|
            stub(klass).preview? {false}
          end
          mock(@page_builder).add(anything).times(records.length)
          mock(@page_builder).finish
          plugin = Mixpanel.new(DataSource[task.to_a].merge({"incremental_column"=>"time", "latest_fetched_time"=>"1452027551999"}), nil, nil, @page_builder)
          task_report = plugin.run
          assert_equal("2015-03-01", task_report[:to_date])
          assert_equal("1452027552000", task_report[:latest_fetched_time])
        end

        def test_run_with_incremental_column_skip
          any_instance_of(Embulk::Input::Service::JqlService) do |klass|
            stub(klass).preview? {false}
          end

          mock(@page_builder).add(anything).times(0)
          mock(@page_builder).finish
          plugin = Mixpanel.new(DataSource[task.to_a].merge({"incremental_column"=>"time", "latest_fetched_time"=>"1452027552001"}), nil, nil, @page_builder)
          task_report = plugin.run
          assert_equal("2015-03-01", task_report[:to_date])
          assert_equal("1452027552001", task_report[:latest_fetched_time])
        end

        class SliceRangeRunTest < self
          def test_default_slice_range
            plugin = Mixpanel.new(task.merge(slice_range: 4), nil, nil, @page_builder)
            any_instance_of(Embulk::Input::Service::JqlService) do |klass|
              stub(klass).preview? {false}
            end

            mock(@page_builder).add(anything).times(records.length * 2)
            mock(@page_builder).finish
            plugin.run
          end
        end

        def test_preview
          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).request_jql(anything) {Struct.new(:code, :body).new(200, records.to_json)}
          end
          any_instance_of(Embulk::Input::Service::JqlService) do |klass|
            stub(klass).preview? {true}
          end
          mock(@page_builder).add(anything).times(SMALL_NUM_OF_RECORDS)
          mock(@page_builder).finish
          @plugin.run
        end
      end

      private

      def schema
        [
          {"name"=>"name", "type"=>"string"},
          {"name"=>"distinct_id", "type"=>"string"},
          {"name"=>"labels", "type"=>"json"},
          {"name"=>"time", "type"=>"long"},
          {"name"=>"sampling_factor", "type"=>"long"},
          {"name"=>"dataset", "type"=>"string"},
          {"name"=>"properties", "type"=>"json"}
        ]
      end

      def task
        {
          api_secret: API_SECRET,
          jql_endpoint: "https://mixpanel.com/api/2.0/jql/",
          timezone: TIMEZONE,
          incremental: true,
          schema: schema,
          dates: DATES.to_a.map(&:to_s),
          retry_initial_wait_sec: 2,
          retry_limit: 3,
          jql_mode: true,
          jql_script: JQL_SCRIPT,
          slice_range: SLICE_RANGE,
          incremental_column: nil,
          latest_fetched_time: 0,
        }
      end

      def records
        [
          {
            "name": "pageview",
            "distinct_id": "02a99746-0f52-4acd-9a53-7deb763803ca",
            "labels": [],
            "time": 1452027552000,
            "sampling_factor": 1,
            "dataset": "$mixpanel",
            "properties": {
              "$email": "Alexander.Davidson@hotmailx.com",
              "$import": true,
              "country": "UK",
              "load_time_ms": 4
            }
          },
        ] * 30
      end

      def record

          {
            "name": "pageview",
            "distinct_id": "02a99746-0f52-4acd-9a53-7deb763803ca",
            "labels": [],
            "time": 1452027552000,
            "sampling_factor": 1,
            "dataset": "$mixpanel",
            "properties": {
              "$email": "Alexander.Davidson@hotmailx.com",
              "$import": true,
              "country": "UK",
              "load_time_ms": 4
            }
          }

      end

      def config
        {
          type: "mixpanel",
          api_secret: API_SECRET,
          from_date: FROM_DATE,
          timezone: TIMEZONE,
          fetch_days: DAYS,
          retry_initial_wait_sec: 2,
          retry_limit: 3,
          jql_mode: true,
          jql_script: JQL_SCRIPT,
          slice_range: SLICE_RANGE,
        }
      end

      def embulk_config
        DataSource[*config.to_a.flatten(1)]
      end

      def today
        ActiveSupport::TimeZone[TIMEZONE].today
      end
    end
  end
end
