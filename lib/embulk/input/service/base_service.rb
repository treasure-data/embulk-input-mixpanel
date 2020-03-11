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
        DEFAULT_TIME_COLUMN = 'time'.freeze

        def initialize(config)
          @config = config
        end

        def default_guess_start_date(timezone)
          today(timezone) - DEFAULT_FETCH_DAYS - 1
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
          unless MixpanelApi::Client.mixpanel_available?(endpoint)
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
            MixpanelApi::Client.new(@config.param(:api_secret, :string), endpoint, retryer)
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

        def range
          timezone = @config.param(:timezone, :string, default: "")
          from_date = @config.param(:from_date, :string, default: (today(timezone) - 2).to_s)
          incremental = @config.param(:incremental, :bool, default: true)
          incremental_column = @config.param(:incremental_column, :string, default: nil)
          latest_fetched_time =  @config.param(:latest_fetched_time, :integer, default: 0)
          fetch_days = @config.param(:fetch_days, :integer, default: nil)

          # Backfill from date if incremental and an incremental field is set and we are in incremental run
          if incremental && incremental_column && latest_fetched_time !=0
            back_fill_days = @config.param(:back_fill_days, :integer, default: 5)
            Embulk.logger.info "Backfill days #{back_fill_days}"
            from_date = (Date.parse(from_date) - back_fill_days).to_s
            fetch_days = fetch_days.nil? ? nil : fetch_days + back_fill_days
          end

          RangeGenerator.new(from_date, fetch_days, timezone).generate_range
        end
      end
    end
  end
end