require "embulk/input/mixpanel_api/client"

module Embulk
  module Input
    module MixpanelApi
      class ClientTest < Test::Unit::TestCase
        API_KEY = "api_key".freeze
        API_SECRET = "api_secret".freeze

        def setup
          @client = Client.new(API_KEY, API_SECRET)
        end

        # NOTE: Client#signature is private method but this value
        # can't be checked via other methods.
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

        class ExportTest < self
          def setup
            super

            @httpclient = HTTPClient.new
          end

          def test_httpclient
            stub_response(success_response)
            mock(@client).httpclient { @httpclient }

            @client.export(params)
          end

          def test_response_class
            stub_client
            stub_response(success_response)

            actual = @client.export(params)

            assert_equal(Enumerator, actual.class)
          end

          def test_http_request
            stub_client
            mock(@httpclient).get(Client::ENDPOINT_EXPORT, params) do
              success_response
            end

            @client.export(params)
          end

          def test_success
            stub_client
            stub_response(success_response)

            actual = @client.export(params)

            assert_equal(dummy_responses, actual.to_a)
          end

          def test_failure
            stub_client
            stub_response(failure_response)

            stub(Embulk.logger).error(failure_response.body) {}

            assert_raise(Embulk::ConfigError) do
              @client.export(params)
            end
          end

          def test_failure_logging
            stub_client
            stub_response(failure_response)

            mock(Embulk.logger).error(failure_response.body) {}

            assert_raise(Embulk::ConfigError) do
              @client.export(params)
            end
          end

          private

          def stub_client
            stub(@client).httpclient { @httpclient }
          end

          def stub_response(response)
            stub(@httpclient).get(Client::ENDPOINT_EXPORT, params) do
              response
            end
          end

          def success_response
            Struct.new(:code, :body).new(200, jsonl_dummy_responses)
          end

          def failure_response
            Struct.new(:code, :body).new(400, "{'error': 'invalid'}")
          end

          def params
            {
              api_key: API_KEY,
              api_secret: API_SECRET,
              from_date: "2015-01-01",
              to_date: "2015-03-02",
            }
          end

          def dummy_responses
            [
              {
                "event" => "event",
                "properties" => {
                  "foo" => "FOO",
                  "bar" => "2000-01-01 11:11:11",
                  "int" => 42,
                }
              },
              {
                "event" => "event2",
                "properties" => {
                  "foo" => "fooooooooo",
                  "bar" => "1988-12-01 12:11:11",
                  "int" => 1,
                }
              },
            ]
          end

          def jsonl_dummy_responses
            dummy_responses.map{|res| JSON.dump(res)}.join("\n")
          end
        end
      end
    end
  end
end
