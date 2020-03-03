require "perfect_retry"
require "range_generator"
require "timezone_validator"
require "active_support/core_ext/time"
require "tzinfo"
require "embulk/input/mixpanel_api/client"
require "embulk/input/mixpanel_api/exceptions"

module Embulk
  module Input
    module Service
      class BaseService

        NOT_PROPERTY_COLUMN = "event".freeze
        DEFAULT_FETCH_DAYS = 7
        DEFAULT_TIME_COLUMN = 'time'

        def initialize(config)
          @config = config
        end

        def default_guess_start_date(timezone)
          today(timezone) - DEFAULT_FETCH_DAYS - 1
        end

        def create_next_config_diff(task_report)
          next_to_date = Date.parse(task_report[:to_date])
          {
            from_date: next_to_date.to_s,
            latest_fetched_time: task_report[:latest_fetched_time],
          }
        end

        protected

        def validate_config
          timezone = @config.param(:timezone, :string)
          validate_timezone(timezone)
        end

        def validate_timezone(timezone)
          TimezoneValidator.new(timezone).validate
        end

        def giveup_when_mixpanel_is_down
          unless MixpanelApi::Client.mixpanel_available?(export_endpoint)
            raise Embulk::DataError.new("Mixpanel service is down. Please retry later.")
          end
        end

        def adjust_timezone(epoch)
          # Adjust timezone offset to get UTC time
          # c.f. https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel#export
          if epoch.present?
            tz = TZInfo::Timezone.get(@timezone)
            offset = tz.period_for_local(epoch, true).offset.utc_total_offset
            epoch - offset
          end
        end

        def today(timezone)
          if timezone.nil?
            Date.today
          else
            zone = ActiveSupport::TimeZone[timezone]
            zone.nil? ? Date.today : zone.today
          end
        end

        def export_endpoint
          jql_mode = @config.param(:jql_mode, :bool, default: false)
          if jql_mode
            @config.param(:export_endpoint, :string, default: Embulk::Input::MixpanelApi::Client::DEFAULT_JQL_ENDPOINT)
          else
            @config.param(:export_endpoint, :string, default: Embulk::Input::MixpanelApi::Client::DEFAULT_EXPORT_ENDPOINT)
          end
        end

        def extract_values(record)
          @schema.map do |column|
            extract_value(record, column["name"])
          end
        end

        def preview?
          begin
            org.embulk.spi.Exec.isPreview()
          rescue java.lang.NullPointerException=>e
            false
          end
        end

        def create_client
          if @client.present?
            @client
          else
            retryer = perfect_retry({
              # retry_initial_wait_sec: @config[:retry_initial_wait_sec] ? @config[:retry_initial_wait_sec] : 1,
              # retry_limit: @config[:retry_limit] ?  @config[:retry_limit] : 5,
              retry_initial_wait_sec: @config.param(:retry_initial_wait_sec, :integer, default: 1),
              retry_limit: @config.param(:retry_limit, :integer, default: 5),
            })
            MixpanelApi::Client.new(@config.param(:api_secret, :string), retryer, export_endpoint)
          end
        end

        def perfect_retry(task)
          PerfectRetry.new do |config|
            config.limit = task[:retry_limit]
            config.sleep = proc {|n| task[:retry_initial_wait_sec] * (2 * (n - 1))}
            config.dont_rescues = [Embulk::ConfigError, MixpanelApi::IncompleteExportResponseError]
            config.rescues = [RuntimeError]
            config.log_level = nil
            config.logger = Embulk.logger
          end
        end
      end
    end
  end
end