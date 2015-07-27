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

      DURATIONS = [
        {from_date: FROM_DATE, to_date: "2015-02-28"}, # 2015-02-28 is after 7 days from FROM_DATE
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

        def test_invalid_timezone
          assert_raise(TZInfo::InvalidTimezoneIdentifier) do
            Mixpanel.new(task.merge(timezone: "Asia/Tokyooooooooo"), nil, nil, @page_builder).run
          end
        end

        private

        def task
          {
            api_key: API_KEY,
            api_secret: API_SECRET,
            timezone: "Asia/Tokyo",
            schema: [
              {"name" => "foo", "type" => "long"},
              {"name" => "time", "type" => "long"},
            ],
            dates: (Date.parse(FROM_DATE)..Date.parse(TO_DATE)).to_a,
            params: Mixpanel.export_params(embulk_config),
          }
        end

        def timezone_offset_seconds
          60 * 60 * 9 # Asia/Tokyo
        end
      end

      private

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

      def embulk_config
        config = {
          type: "mixpanel",
          api_key: API_KEY,
          api_secret: API_SECRET,
          from_date: FROM_DATE,
          to_date: TO_DATE,
        }
        DataSource[*config.to_a.flatten(1)]
      end
    end
  end
end
