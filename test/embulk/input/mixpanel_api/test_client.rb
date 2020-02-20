# require "embulk/input/mixpanel_api/client"
# require "embulk/input/mixpanel_api/exceptions"
# require "override_assert_raise"
#
# module Embulk
#   module Input
#     module MixpanelApi
#       class ClientTest < Test::Unit::TestCase
#         include OverrideAssertRaise
#
#         API_SECRET = "api_secret".freeze
#
#         def setup
#           @client = Client.new(API_SECRET)
#           stub(Embulk).logger { ::Logger.new(IO::NULL) }
#         end
#
#         class TestKeepAlive < self
#           def test_tcp_keepalive_enabled
#             client = Client.new(API_SECRET)
#             assert client.send(:httpclient).tcp_keepalive
#           end
#         end
#
#         class TryToDatesTest < self
#           def setup
#             @client = Client.new(API_SECRET)
#           end
#
#
#           def test_candidates_case1
#             from_str = "2000-01-01"
#             from = Date.parse(from_str)
#             yesterday = Date.today - 1
#             dates = @client.try_to_dates(from.to_s)
#             expect = [
#               from + 1,
#               from + 10,
#               from + 100,
#               from + 1000,
#               from + 10000,
#               yesterday,
#             ].find_all{|d| d <= yesterday}
#
#             assert_equal expect, dates
#             assert dates.all?{|d| d <= yesterday}
#           end
#
#           def test_candidates_case2
#             from_str = (Date.today - 3).to_s
#             from = Date.parse(from_str)
#             yesterday = Date.today - 1
#             dates = @client.try_to_dates(from.to_s)
#             expect = [
#                 from + 1,
#                 from + 10,
#                 from + 100,
#                 from + 1000,
#                 from + 10000,
#                 yesterday,
#             ].find_all{|d| d <= yesterday}
#
#             assert_equal expect, dates
#             assert dates.all?{|d| d <= yesterday}
#           end
#         end
#
#         class ExportTest < self
#           def setup
#             super
#
#             @httpclient = HTTPClient.new
#           end
#
#           def test_success
#             stub_client
#             # stub(@client).set_signatures(anything) {}
#             stub_response(success_response)
#
#             records = []
#             @client.export(params) do |record|
#               records << record
#             end
#
#             assert_equal(dummy_responses, records)
#           end
#
#           def test_export_partial_with_export_terminated_early
#             stub_client
#             # stub(@client).set_signatures(anything) {}
#             stub_response(Struct.new(:code, :body).new(200, jsonl_dummy_responses+"\nexport terminated early"))
#
#             records = []
#             assert_raise MixpanelApi::IncompleteExportResponseError do
#               @client.export(params) do |record|
#                 records << record
#               end
#             end
#             assert_equal(dummy_responses, records)
#           end
#
#           def test_export_partial_with_error_json
#             stub_client
#             # stub(@client).set_signatures(anything) {}
#             stub_response(Struct.new(:code, :body).new(200, jsonl_dummy_responses+"\n{\"error\":"))
#             records = []
#             assert_raise MixpanelApi::IncompleteExportResponseError do
#               @client.export(params) do |record|
#                 records << record
#               end
#             end
#             assert_equal(dummy_responses, records)
#           end
#
#           def test_failure_with_400
#             stub_client
#             stub_response(failure_response(400))
#
#             assert_raise(Embulk::ConfigError) do
#               @client.export(params)
#             end
#           end
#
#           def test_failure_with_500
#             stub_client
#             stub_response(failure_response(500))
#
#             assert_raise(RuntimeError) do
#               @client.export(params)
#             end
#           end
#
#           def test_retry_for_429_temporary_fail
#             stub_client
#             stub_response(failure_response(429))
#
#             assert_raise(RuntimeError) do
#               @client.export(params)
#             end
#           end
#
#           class ExportSmallDataset < self
#             def test_to_date_after_1_day
#               to = (Date.parse(params["from_date"]) + 1).to_s
#               mock(@client).request_small_dataset(params.merge("to_date" => to), Client::SMALL_NUM_OF_RECORDS) { [:foo] }
#
#               @client.export_for_small_dataset(params)
#             end
#
#             def test_retry_for_429_temporary_fail
#               stub_client
#               stub_response(failure_response(429))
#
#               assert_raise(RuntimeError) do
#                 @client.export_for_small_dataset(params)
#               end
#             end
#
#             def test_to_date_after_1_day_after_10_days_if_empty
#               stub_client
#               to1 = (Date.parse(params["from_date"]) + 1).to_s
#               to2 = (Date.parse(params["from_date"]) + 10).to_s
#               mock(@client).request_small_dataset(params.merge("to_date" => to1), Client::SMALL_NUM_OF_RECORDS) { [] }
#               mock(@client).request_small_dataset(params.merge("to_date" => to2), Client::SMALL_NUM_OF_RECORDS) { [:foo] }
#
#               @client.export_for_small_dataset(params)
#             end
#
#             def test_config_error_when_too_long_empty_dates
#               stub(@client).request(anything, anything) { "" }
#
#               assert_raise(Embulk::ConfigError) do
#                 @client.export_for_small_dataset(params)
#               end
#             end
#           end
#
#           private
#
#           def stub_client
#             stub(@client).httpclient { @httpclient }
#           end
#
#           def stub_response(response)
#             @httpclient.test_loopback_http_response << [
#               "HTTP/1.1 #{response.code}",
#               "Content-Type: application/json",
#               "",
#               response.body
#             ].join("\r\n")
#           end
#
#           def success_response
#             Struct.new(:code, :body).new(200, jsonl_dummy_responses)
#           end
#
#           def failure_response(code)
#             Struct.new(:code, :body).new(code, "{'error': 'invalid'}")
#           end
#
#           def params
#             {
#               "api_secret" => API_SECRET,
#               "from_date" => "2015-01-01",
#               "to_date" => "2015-03-02",
#             }
#           end
#
#           def dummy_responses
#             [
#               {
#                 "event" => "event",
#                 "properties" => {
#                   "foo" => "FOO",
#                   "bar" => "2000-01-01 11:11:11",
#                   "int" => 42,
#                 }
#               },
#               {
#                 "event" => "event2",
#                 "properties" => {
#                   "foo" => "fooooooooo",
#                   "bar" => "1988-12-01 12:11:11",
#                   "int" => 1,
#                 }
#               },
#             ]
#           end
#
#           def jsonl_dummy_responses
#             dummy_responses.map{|res| JSON.dump(res)}.join("\n")
#           end
#         end
#       end
#     end
#   end
# end
