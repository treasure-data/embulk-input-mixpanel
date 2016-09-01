require "uri"
require "digest/md5"
require "json"
require "httpclient"

module Embulk
  module Input
    module MixpanelApi
      class Client
        ENDPOINT_EXPORT = "https://data.mixpanel.com/api/2.0/export/".freeze
        TIMEOUT_SECONDS = 3600
        PING_TIMEOUT_SECONDS = 3
        PING_RETRY_LIMIT = 3
        PING_RETRY_WAIT = 2
        SMALLSET_BYTE_RANGE = "0-#{5 * 1024 * 1024}"

        attr_reader :retryer

        def self.mixpanel_available?
          retryer = PerfectRetry.new do |config|
            config.limit = PING_RETRY_LIMIT
            config.sleep = PING_RETRY_WAIT
            config.logger = Embulk.logger
            config.log_level = nil
          end

          begin
            retryer.with_retry do
              client = HTTPClient.new
              client.connect_timeout = PING_TIMEOUT_SECONDS
              client.get("https://data.mixpanel.com")
            end
            true
          rescue PerfectRetry::TooManyRetry
            false
          end
        end

        def initialize(api_key, api_secret, retryer = nil)
          @api_key = api_key
          @api_secret = api_secret
          @retryer = retryer || PerfectRetry.new do |config|
            # for test
            config.limit = 0
            config.dont_rescues = [RuntimeError]
            config.log_level = nil
            config.logger = Embulk.logger
            config.raise_original_error = true
          end
        end

        def export(params = {}, &block)
          retryer.with_retry do
            request(params, &block)
          end
        end

        def export_for_small_dataset(params = {})
          try_to_dates = 5.times.map do |n|
            # from_date + 1, from_date + 10, from_date + 100, ... so on
            days = 1 * (10 ** n)
            Date.parse(params["from_date"].to_s) + days
          end

          try_to_dates.each do |to_date|
            params["to_date"] = to_date.strftime("%Y-%m-%d")
            records = retryer.with_retry do
              request_small_dataset(params, SMALLSET_BYTE_RANGE)
            end
            next if records.first.nil?
            return records
          end

          raise ConfigError.new "#{params["from_date"]} + #{days} days has no record. too old date?"
        end

        private

        def response_to_enum(response_body)
          Enumerator.new do |y|
            response_body.lines.each do |json|
              # TODO: raise Embulk::DataError when invalid json given for Embulk 0.7+
              y << JSON.parse(json)
            end
          end
        end

        def request(params, &block)
          # https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
          Embulk.logger.debug "Export param: #{params.to_s}"
          set_signatures(params)

          buf = ""
          response = httpclient.get(ENDPOINT_EXPORT, params) do |chunk|
            chunk.each_line do |line|
              begin
                record = JSON.parse(buf + line)
                block.call record
                buf = ""
              rescue JSON::ParserError => e
                buf << line
              end
            end
          end
          handle_error(response)
        end

        def request_small_dataset(params, range)
          # guess/preview
          # Try to fetch first `range` bytes
          set_signatures(params)
          res = httpclient.get(ENDPOINT_EXPORT, params, {"Range" => "bytes=#{range}"})
          if res.code == 416
            # cannot satisfied requested Range, get full body
            res = httpclient.get(ENDPOINT_EXPORT, params)
          end
          handle_error(res)
          response_to_enum(res.body)
        end

        def handle_error(response)
          Embulk.logger.debug "response code: #{response.code}"
          case response.code
          when 400..499
            raise ConfigError.new("[#{response.code}] #{response.body}")
          when 500..599
            raise RuntimeError.new("[#{response.code}] #{response.body}")
          end
        end

        def set_signatures(params)
          params[:expire] ||= Time.now.to_i + TIMEOUT_SECONDS
          params[:sig] = signature(params)
          params
        end

        def signature(params)
          # https://mixpanel.com/docs/api-documentation/data-export-api#auth-implementation
          params.delete(:sig)
          sorted_keys = params.keys.map(&:to_s).sort
          signature = sorted_keys.inject("") do |sig, key|
            value = params[key] || params[key.to_sym]
            next sig unless value
            sig << "#{key}=#{value}"
          end

          Digest::MD5.hexdigest(signature + @api_secret)
        end

        def httpclient
          @client ||=
            begin
              client = HTTPClient.new
              client.receive_timeout = TIMEOUT_SECONDS
              client.default_header = {Accept: "application/json; charset=UTF-8"}
              # client.debug_dev = STDERR
              client
            end
        end
      end
    end
  end
end
