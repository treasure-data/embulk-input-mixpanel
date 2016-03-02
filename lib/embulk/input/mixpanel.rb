require "tzinfo"
require "perfect_retry"
require "embulk/input/mixpanel_api/client"
require "range_generator"
require "timezone_validator"

module Embulk
  module Input
    class Mixpanel < InputPlugin
      Plugin.register_input("mixpanel", self)

      GUESS_RECORDS_COUNT = 10
      NOT_PROPERTY_COLUMN = "event".freeze

      # NOTE: It takes long time to fetch data between from_date to
      # to_date by one API request. So this plugin fetches data
      # between each 7 (SLICE_DAYS_COUNT) days.
      SLICE_DAYS_COUNT = 7

      def self.transaction(config, &control)
        timezone = config.param(:timezone, :string)
        TimezoneValidator.new(timezone).validate

        from_date = config.param(:from_date, :string, default: (Date.today - 2).to_s)
        fetch_days = config.param(:fetch_days, :integer, default: nil)
        range = RangeGenerator.new(from_date, fetch_days).generate_range
        Embulk.logger.info "Try to fetch data from #{range.first} to #{range.last}"

        fetch_unknown_columns = config.param(:fetch_unknown_columns, :bool, default: true)

        task = {
          params: export_params(config),
          dates: range,
          timezone: timezone,
          api_key: config.param(:api_key, :string),
          api_secret: config.param(:api_secret, :string),
          schema: config.param(:columns, :array),
          fetch_unknown_columns: fetch_unknown_columns,
          retry_initial_wait_sec: config.param(:retry_initial_wait_sec, :integer, default: 1),
          retry_limit: config.param(:retry_limit, :integer, default: 5),
        }

        columns = task[:schema].map do |column|
          name = column["name"]
          type = column["type"].to_sym

          Column.new(nil, name, type, column["format"])
        end

        if fetch_unknown_columns
          columns << Column.new(nil, "unknown_columns", :json)
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        # NOTE: If this plugin supports to run by multi threads, this
        # implementation is terrible.
        task_report = task_reports.first
        next_to_date = Date.parse(task_report[:to_date]).next

        next_config_diff = {from_date: next_to_date.to_s}
        return next_config_diff
      end

      def self.guess(config)
        client = MixpanelApi::Client.new(config.param(:api_key, :string), config.param(:api_secret, :string))

        range = guess_range(config)
        Embulk.logger.info "Guessing schema using #{range.first}..#{range.last} records"

        params = export_params(config).merge(
          from_date: range.first,
          to_date: range.last,
        )

        columns = guess_from_records(client.export(params))
        return {"columns" => columns}
      end

      def init
        @api_key = task[:api_key]
        @api_secret = task[:api_secret]
        @params = task[:params]
        @timezone = task[:timezone]
        @schema = task[:schema]
        @dates = task[:dates]
        @fetch_unknown_columns = task[:fetch_unknown_columns]
        @retryer = PerfectRetry.new do |config|
          config.limit = task[:retry_limit]
          config.sleep = proc{|n| task[:retry_initial_wait_sec] * (2 * (n - 1)) }
          config.dont_rescues = [Embulk::ConfigError]
          config.rescues = [RuntimeError]
          config.log_level = nil
          config.logger = Embulk.logger
        end
      end

      def run
        @dates.each_slice(SLICE_DAYS_COUNT) do |dates|
          Embulk.logger.info "Fetching data from #{dates.first} to #{dates.last} ..."

          fetch(dates).each do |record|
            values = extract_values(record)
            if @fetch_unknown_columns
              unknown_values = extract_unknown_values(record)
              values << unknown_values.to_json
            end
            page_builder.add(values)
          end

          break if preview?
        end

        page_builder.finish

        task_report = {to_date: @dates.last || (Date.today - 1)}
        return task_report
      end

      private

      def extract_values(record)
        @schema.map do |column|
          extract_value(record, column["name"])
        end
      end

      def extract_value(record, name)
        case name
        when NOT_PROPERTY_COLUMN
          record[NOT_PROPERTY_COLUMN]
        when "time"
          time = record["properties"]["time"]
          adjust_timezone(time)
        else
          record["properties"][name]
        end
      end

      def extract_unknown_values(record)
        record_keys = record["properties"].keys + [NOT_PROPERTY_COLUMN]
        schema_keys = @schema.map {|column| column["name"]}
        unknown_keys = record_keys - schema_keys

        unless unknown_keys.empty?
          Embulk.logger.warn("Unknown columns exists in record: #{unknown_keys.join(', ')}")
        end

        unknown_keys.inject({}) do |result, key|
          result[key] = extract_value(record, key)
          result
        end
      end

      def fetch(dates)
        from_date = dates.first
        to_date = dates.last
        params = @params.merge(
          "from_date" => from_date,
          "to_date" => to_date,
        )
        client = MixpanelApi::Client.new(@api_key, @api_secret)
        @retryer.with_retry do
          client.export(params)
        end
      end

      def adjust_timezone(epoch)
        # Adjust timezone offset to get UTC time
        # c.f. https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel#export
        tz = TZInfo::Timezone.get(@timezone)
        offset = tz.period_for_local(epoch, true).offset.utc_offset
        epoch - offset
      end

      def preview?
        begin
          org.embulk.spi.Exec.isPreview()
        rescue java.lang.NullPointerException => e
          false
        end
      end

      def self.export_params(config)
        event = config.param(:event, :array, default: nil)
        event = event.nil? ? nil : event.to_json

        {
          api_key: config.param(:api_key, :string),
          event: event,
          where: config.param(:where, :string, default: nil),
          bucket: config.param(:bucket, :string, default: nil),
        }
      end

      def self.default_guess_start_date
        Date.today - SLICE_DAYS_COUNT - 1
      end

      def self.guess_range(config)
        from_date = config.param(:from_date, :string, default: default_guess_start_date.to_s)
        fetch_days = config.param(:fetch_days, :integer, default: SLICE_DAYS_COUNT)
        range = RangeGenerator.new(from_date, fetch_days).generate_range
        if range.empty?
          return default_guess_start_date..(Date.today - 1)
        end
        range
      end

      def self.guess_from_records(records)
        sample_props = records.first(GUESS_RECORDS_COUNT).map{|r| r["properties"]}
        schema = Guess::SchemaGuess.from_hash_records(sample_props)
        columns = schema.map do |col|
          result = {
            name: col.name,
            type: col.type,
          }
          result[:format] = col.format if col.format
          result
        end
        columns.unshift(name: NOT_PROPERTY_COLUMN, type: :string)
      end
    end

  end
end
