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
