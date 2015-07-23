require "prepare_embulk"
require "embulk/input/mixpanel"
require "json"

module Embulk
  module Input
    class MixpanelTest < Test::Unit::TestCase
      def setup
        httpclient = HTTPClient.new
        httpclient.test_loopback_response << dummy_jsonl
        any_instance_of(MixpanelApi::Client) do |klass|
          stub(klass).httpclient { httpclient }
        end
      end

      def test_guess
        actual = Mixpanel.guess(embulk_config)
        assert_equal(expected, actual)
      end

      def test_export_params
        config_params = [
          :type, "mixpanel",
          :api_key, "key",
          :api_secret, "SECRET",
          :from_date, "2015-01-01",
          :to_date, "2015-03-02",
          :event, ["ViewHoge", "ViewFuga"],
          :where, 'properties["$os"] == "Windows"',
          :bucket, "987",
        ]

        config = DataSource[*config_params]

        expected = {
          api_key: "key",
          from_date: "2015-01-01",
          to_date: "2015-03-02",
          event: "[\"ViewHoge\",\"ViewFuga\"]",
          where: 'properties["$os"] == "Windows"',
          bucket: "987",
        }
        actual = Mixpanel.export_params(config)

        assert_equal(expected, actual)
      end

      class RunTest < self
        def setup
          httpclient = HTTPClient.new
          httpclient.test_loopback_response << records.map{|record| JSON.dump(record)}.join("\n")
          any_instance_of(MixpanelApi::Client) do |klass|
            stub(klass).httpclient { httpclient }
          end
          @page_builder = Object.new
          @plugin = Mixpanel.new(task, nil, nil, @page_builder)
        end

        def test_preview
          stub(@plugin).preview? { true }
          mock(@page_builder).add(anything).times(Mixpanel::PREVIEW_RECORDS_COUNT)
          mock(@page_builder).finish

          @plugin.run
        end

        def test_run
          stub(@plugin).preview? { false }
          mock(@page_builder).add(anything).times(records.length)
          mock(@page_builder).finish

          @plugin.run
        end

        def test_timezone
          stub(@plugin).preview? { false }
          adjusted = record_epoch - timezone_offset_seconds
          mock(@page_builder).add(["FOO", adjusted]).times(records.length)
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
            api_key: "key",
            api_secret: "secret",
            timezone: "Asia/Tokyo",
            schema: [
              {"name" => "foo", "type" => "long"},
              {"name" => "time", "type" => "long"},
            ],
            params: Mixpanel.export_params(embulk_config),
          }
        end

        def record_epoch
          1234567890
        end

        def timezone_offset_seconds
          60 * 60 * 9 # Asia/Tokyo
        end

        def records
          [
            {
              event: "event",
              properties: {
                foo: "FOO",
                time: record_epoch,
              }
            },
          ] * Mixpanel::PREVIEW_RECORDS_COUNT * 2
        end
      end

      private

      def dummy_jsonl
        json1 = JSON.dump({
          event: "event",
          properties: {
            foo: "FOO",
            bar: "2000-01-01 11:11:11",
            int: 42
          }
        })
        json2 = JSON.dump({
          event: "event2",
          properties: {
            foo: "fooooooooo",
            bar: "1988-12-01 12:11:11",
            int: 1
          }
        })

        [json1, json2].join("\n")
      end

      def embulk_config
        DataSource[*config.to_a.flatten(1)]
      end

      def config
        {
          type: "mixpanel",
          api_key: "key",
          api_secret: "SECRET",
          from_date: "2015-01-01",
          to_date: "2015-03-02",
        }
      end

      def expected
        {
          "columns" => [
            {name: "event", type: :string},
            {name: "foo", type: :string},
            {name: "bar", format: "%Y-%m-%d %H:%M:%S", type: :timestamp},
            {name: "int", type: :long},
          ]
        }
      end
    end
  end
end
