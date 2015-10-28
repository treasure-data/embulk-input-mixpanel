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

        def initialize(api_key, api_secret)
          @api_key = api_key
          @api_secret = api_secret
        end

        def export_with_retry(params = {}, retry_initial_wait_sec, retry_limit)
          body = with_retry(retry_initial_wait_sec, retry_limit) do
            request(params)
          end

          response_to_enum(body)
        end

        def export(params = {})
          body = request(params)
          response_to_enum(body)
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

        def request(params)
          # https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
          params[:expire] ||= Time.now.to_i + TIMEOUT_SECONDS
          params[:sig] = signature(params)
          Embulk.logger.debug "Export param: #{params.to_s}"

          response = httpclient.get(ENDPOINT_EXPORT, params)
          Embulk.logger.debug "response code: #{response.code}"
          case response.code
          when 400..499
            raise ConfigError.new response.body
          when 500..599
            raise RuntimeError, response.body
          end
          response.body
        end

        def with_retry(initial_wait, retry_limit, &block)
          retry_count = 0
          wait_sec = initial_wait
          begin
            yield
          rescue Embulk::ConfigError => e # TODO: rescue Embulk::DataError for Embulk 0.7+
            # Don't retry
            raise e
          rescue => e
            if retry_limit <= retry_count
              Embulk.logger.error "'#{e}(#{e.class})' error occured and reached retry limit (#{retry_limit} times)"
              raise e
            end
            retry_count += 1
            Embulk.logger.warn "Retrying after #{wait_sec} seconds [#{retry_count}/#{retry_limit}] '#{e}(#{e.class})' error occured"
            sleep wait_sec
            wait_sec *= 2
            retry
          end
        end

        def signature(params)
          # https://mixpanel.com/docs/api-documentation/data-export-api#auth-implementation
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
