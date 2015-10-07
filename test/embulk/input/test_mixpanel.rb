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

        private

        def stub_export_all
          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).export(anything) { records }
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
              {name: "event", type: :string},
              {name: "foo", type: :string},
              {name: "time", type: :long},
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

      class RunTest < self
        def setup_client

          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).export(anything) { records }
          end
        end

        def setup
          super

          @page_builder = Object.new
          @plugin = Mixpanel.new(task, nil, nil, @page_builder)
        end

        def test_preview_check
          mock(@plugin).preview? { true }
          stub(@page_builder).add(anything)
          stub(@page_builder).finish

          @plugin.run
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
          mock(@page_builder).add(["FOO", adjusted]).times(records.length * 2)
          mock(@page_builder).finish

          @plugin.run
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
        }
      end

      def embulk_config
        DataSource[*config.to_a.flatten(1)]
      end
    end
  end
end
