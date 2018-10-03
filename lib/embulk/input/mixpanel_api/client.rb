require "uri"
require "digest/md5"
require "json"
require "httpclient"
require "embulk/input/mixpanel_api/exceptions"

module Embulk
  module Input
    module MixpanelApi
      class Client
        TIMEOUT_SECONDS = 3600
        PING_TIMEOUT_SECONDS = 3
        PING_RETRY_LIMIT = 3
        PING_RETRY_WAIT = 2
        SMALL_NUM_OF_RECORDS = 10
        DEFAULT_EXPORT_ENDPOINT = "https://data.mixpanel.com/api/2.0/export/".freeze

        attr_reader :retryer

        def self.mixpanel_available?(endpoint = nil)
          endpoint ||= DEFAULT_EXPORT_ENDPOINT
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
              client.get(URI.join(endpoint, '/'))
            end
            true
          rescue PerfectRetry::TooManyRetry
            false
          end
        end

        def initialize(api_key, api_secret, retryer = nil, endpoint = DEFAULT_EXPORT_ENDPOINT)
          @endpoint = endpoint
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
          yesterday = Date.today - 1
          latest_tried_to_date = nil
          try_to_dates(params["from_date"]).each do |to_date|
            next if yesterday < to_date
            latest_tried_to_date = to_date
            params["to_date"] = to_date.strftime("%Y-%m-%d")
            records = retryer.with_retry do
              request_small_dataset(params, SMALL_NUM_OF_RECORDS)
            end
            next if records.first.nil?
            return records
          end

          raise ConfigError.new "#{params["from_date"]}..#{latest_tried_to_date} has no record."
        end

        def try_to_dates(from_date)
          try_to_dates = 5.times.map do |n|
            # from_date + 1, from_date + 10, from_date + 100, ... so on
            days = 1 * (10 ** n)
            Date.parse(from_date.to_s) + days
          end
          yesterday = Date.today - 1
          try_to_dates << yesterday
          try_to_dates.find_all {|date| date <= yesterday}.uniq
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
          error_response = ''
          Embulk.logger.info "Sending request to #{@endpoint}"
          response = httpclient.get(@endpoint, params) do |response, chunk|
            # Only process data if response status is 200..299
            if response.status/100 == 2
              chunk.each_line do |line|
                begin
                  record = JSON.parse(buf + line)
                  block.call record
                  buf = ""
                rescue JSON::ParserError => e
                  buf << line
                end
              end
            else
               error_response << chunk
            end
          end
          handle_error(response, error_response)
          if !buf.empty?
            #   buffer is not empty mean the last json line is incomplete
            Embulk.logger.error "Received incomplete data from Mixpanel, #{buf}"
            raise MixpanelApi::IncompleteExportResponseError.new("Incomplete data received")
          end
        end

        def request_small_dataset(params, num_of_records)
          # guess/preview
          # Try to fetch first number of records
          params["limit"] = num_of_records
          set_signatures(params)
          Embulk.logger.info "Sending request to #{@endpoint}"
          res = httpclient.get(@endpoint, params)
          handle_error(res,res.body)
          response_to_enum(res.body)
        end

        def handle_error(response, error_response)
          Embulk.logger.debug "response code: #{response.code}"
          case response.code
          when 400..499
            if response.code == 429
              # [429] {"error": "too many export requests in progress for this project"}
              raise RuntimeError.new("[#{response.code}] #{error_response} (will retry)")
            end
            raise ConfigError.new("[#{response.code}] #{error_response}")
          when 500..599
            raise RuntimeError.new("[#{response.code}] #{error_response}")
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
              client.tcp_keepalive = true
              client.default_header = {Accept: "application/json; charset=UTF-8"}
              # client.debug_dev = STDERR
              client
            end
        end
      end
    end
  end
end
