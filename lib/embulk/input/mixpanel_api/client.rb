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

        attr_reader :api_key, :api_secret

        def initialize(api_key, api_secret)
          @api_key = api_key
          @api_secret = api_secret
        end

        def export(params = {})
          # https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
          params[:expire] ||= Time.now.to_i + TIMEOUT_SECONDS
          params[:sig] = signature(params)
          response = httpclient.get(ENDPOINT_EXPORT, params)

          if response.code >= 400
            Embulk.logger.error response.body
            return Enumerator.new{|y| }
          end

          Enumerator.new do |y|
            response.body.lines.each do |json|
              y << JSON.parse(json)
            end
          end
        end

        def signature(params)
          # https://mixpanel.com/docs/api-documentation/data-export-api#auth-implementation
          sorted_keys = params.keys.map(&:to_s).sort.uniq
          signature = sorted_keys.inject("") do |sig, key|
            value = params[key] || params[key.to_sym]
            next sig unless value
            sig << "#{key}=#{URI.encode_www_form_component(value)}"
          end
          Digest::MD5.hexdigest(signature + api_secret)
        end

        def httpclient
          @client ||=
            begin
              client = HTTPClient.new
              client.receive_timeout = TIMEOUT_SECONDS
              client
            end
        end
      end
    end
  end
end
