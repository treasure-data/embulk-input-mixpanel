require "prepare_embulk"
require "embulk/input/mixpanel"
require "json"

module Embulk
  module Input
    class MixpanelTest < Test::Unit::TestCase
      API_KEY = "api_key".freeze
      API_SECRET = "api_secret".freeze
      FROM_DATE = "2015-02-22".freeze
      TO_DATE = "2015-03-02".freeze
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

            stub(klass).export(params) { records }
          end
        end
      end

      def setup_logger
        stub(Embulk).logger { ::Logger.new(IO::NULL) }
      end

      def test_guess
        expected = {
          "columns" => [
            {name: "event", type: :string},
            {name: "foo", type: :string},
            {name: "time", type: :long},
            {name: "int", type: :long},
          ]
        }

        actual = Mixpanel.guess(embulk_config)
        assert_equal(expected, actual)
      end

      class TransactionTest < self
        def test_valid_timezone
          timezone = TIMEZONE
          mock(Mixpanel).resume(transaction_task(timezone), columns, 1, &control)

          Mixpanel.transaction(transaction_config(timezone), &control)
        end

        def test_invalid_timezone
          timezone = "#{TIMEZONE}ooooo"

          assert_raise(TZInfo::InvalidTimezoneIdentifier) do
            Mixpanel.transaction(transaction_config(timezone), &control)
          end
        end

        def test_resume
          actual = Mixpanel.resume(transaction_task(TIMEZONE), columns, 1, &control)
          assert_equal({}, actual)
        end

        def control
          proc {} # dummy
        end

        def transaction_config(timezone)
          _config = config.merge(
            timezone: timezone,
            columns: schema,
          )
          DataSource[*_config.to_a.flatten(1)]
        end

        def transaction_task(timezone)
          task.merge(
            dates: (Date.parse(FROM_DATE)..Date.parse(TO_DATE)).map {|date| date.to_s},
            api_key: API_KEY,
            api_secret: API_SECRET,
            timezone: timezone,
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
          dates: (Date.parse(FROM_DATE)..Date.parse(TO_DATE)).to_a,
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
          days: (Date.parse(TO_DATE) - Date.parse(FROM_DATE)).to_i
        }
      end

      def embulk_config
        DataSource[*config.to_a.flatten(1)]
      end
    end
  end
end
