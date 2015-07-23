require "embulk/input/mixpanel_api/client"
module Embulk
  module Input
    module MixpanelApi
      class ClientTest < Test::Unit::TestCase
        def setup
          @client = Client.new("api_key", "api_secret")
        end

        # NOTE: Client#signature is private method but this value
        # can't be checked via other methods.a
        def test_signature
          now = Time.parse("2015-07-22 00:00:00")
          stub(Time).now { now }

          params = {
            string: "string",
            array: ["elem1", "elem2"],
          }
          expected = "4be4a4f92f57e12b543a2a5f2f5897b6"

          assert_equal(expected, @client.__send__(:signature, params))
        end
      end
    end
  end
end
