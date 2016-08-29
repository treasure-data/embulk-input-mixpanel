require "uri"
require "digest/md5"
require "json"
require "httpclient"

module Embulk
  module Input
    module MixpanelApi
      class Client
        class TooManyExportRequestError < StandardError
          attr_reader :response, :params

          def initialize(response, params)
            super("#{response.body}")
            @response = response
            @params = params
          end

          def to_s
            # NOTE: Define `to_s` rather than `message` for Embulk error handling.
            "[#{response.code}] #{response.body} with #{params["from_date"]}..#{params["to_date"]}"
          end
        end

        ENDPOINT_EXPORT = "https://data.mixpanel.com/api/2.0/export/".freeze
        TIMEOUT_SECONDS = 3600
        PING_TIMEOUT_SECONDS = 3
        PING_RETRY_LIMIT = 3
        PING_RETRY_WAIT = 2
        SMALLSET_BYTE_MAX = 5 * 1024 * 1024
        SMALLSET_BYTE_RANGE = "0-#{SMALLSET_BYTE_MAX}"

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

        def initialize(api_key, api_secret)
          @api_key = api_key
          @api_secret = api_secret
        end

        def export(params = {})
          body =
            begin
              request(params.dup)
            rescue TooManyExportRequestError => e
              Embulk.logger.warn "#{e.message}. Retrying"
              request_for_each_day(params)
            end
          response_to_enum(body)
        end

        def export_for_small_dataset(params = {}, times = 0)
          days = (1 * (10 ** times))
          to_date = Date.parse(params["from_date"].to_s) + days
          params["to_date"] = to_date.strftime("%Y-%m-%d")

          body =
            begin
              request(params.dup, SMALLSET_BYTE_RANGE)
            rescue TooManyExportRequestError => e
              Embulk.logger.warn "#{e.message}. Retrying"
              request_for_each_day(params, SMALLSET_BYTE_MAX)
            end

          result = response_to_enum(body)
          if result.first.nil?
            if times >= 5
              raise ConfigError.new "#{params["from_date"]} + #{days} days has no record. too old date?"
            end
            export_for_small_dataset(params, times + 1)
          else
            result
          end
        end

        private

        def response_to_enum(response_body)
          Enumerator.new do |y|
            response_body.lines.each do |json|
              next if json.strip.empty?
              # TODO: raise Embulk::DataError when invalid json given for Embulk 0.7+
              y << JSON.parse(json)
            end
          end
        end

        def request(params, range = nil)
          # https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
          params[:expire] ||= Time.now.to_i + TIMEOUT_SECONDS
          params[:sig] = signature(params)
          Embulk.logger.debug "Export param: #{params.to_s}"

          headers = {}
          response =
            if range
              # guess/preview
              res = httpclient.get(ENDPOINT_EXPORT, params, {"Range" => "bytes=#{range}"})
              if res.code == 416
                # cannot satisfied requested Range, get full body
                httpclient.get(ENDPOINT_EXPORT, params)
              else
                res
              end
            else
              httpclient.get(ENDPOINT_EXPORT, params)
            end
          Embulk.logger.debug "response code: #{response.code}"

          if response.body.include?("too many export requests in progress for this project")
            raise TooManyExportRequestError.new(response, params)
          end
          case response.code
          when 400..499
            raise ConfigError.new response.body
          when 500..599
            raise RuntimeError, response.body
          end

          response.body
        end

        def request_for_each_day(params, range = nil)
          # https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
          params[:expire] ||= Time.now.to_i + TIMEOUT_SECONDS
          params[:sig] = signature(params)

          from = Date.parse(params["from_date"])
          to = Date.parse(params["to_date"])
          diff = (to.jd - from.jd)

          jsonl_body = ""
          (diff + 1).times.each do |day|
            target_date = from + day
            reduced_params = params.merge("from_date" => target_date.to_s, "to_date" => target_date.to_s)
            jsonl_body << request(reduced_params, range) << "\n"
            if range && range < jsonl_body.bytesize
              break
            end
          end
          jsonl_body
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
              client
            end
        end
      end
    end
  end
end
