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
          with_retry(retry_initial_wait_sec, retry_limit) do
            export(params)
          end
        end

        def export(params = {})
          # https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
          params[:expire] ||= Time.now.to_i + TIMEOUT_SECONDS
          params[:sig] = signature(params)

          Embulk.logger.debug "Export param: #{params.to_s}"

          body = request(params)

          Enumerator.new do |y|
            body.lines.each do |json|
              begin
                y << JSON.parse(json)
              rescue => e
                raise Embulk::DataError.new(e.message)
              end
            end
          end
        end

        private

        def request(params)
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
          rescue Embulk::ConfigError, Embulk::DataError => e
            # Don't retry
            raise e
          rescue => e
            if retry_limit <= retry_count
              Embulk.logger.error "'#{e}(#{e.class})' error occured and reached retry count (#{retry_limit} times)"
              raise e
            end
            retry_count += 1
            Embulk.logger.warn "'#{e}(#{e.class})' error occured. sleep and retry (#{retry_count}/#{retry_limit})"
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
