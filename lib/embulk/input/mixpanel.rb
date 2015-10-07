require "tzinfo"
require "embulk/input/mixpanel_api/client"
require "range_generator"
require "timezone_validator"

module Embulk
  module Input
    class Mixpanel < InputPlugin
      Plugin.register_input("mixpanel", self)

      GUESS_RECORDS_COUNT = 10

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

        task = {
          params: export_params(config),
          dates: range,
          timezone: timezone,
          api_key: config.param(:api_key, :string),
          api_secret: config.param(:api_secret, :string),
          schema: config.param(:columns, :array),
        }

        columns = task[:schema].map do |column|
          name = column["name"]
          type = column["type"].to_sym

          Column.new(nil, name, type, column["format"])
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

        from_date_str = config.param(:from_date, :string, default: (Date.today - 1 - SLICE_DAYS_COUNT).to_s)

        from_date = Date.parse(from_date_str)

        if from_date > Date.today - 1
          raise ConfigError.new "Please specify date later than yesterday (inclusive) as 'from_date'"
        end

        # NOTE: to_date is yeasterday if from_date..Date.Today doesn't have
        # more SLICE_DAYS_COUNT days.
        to_date = [from_date + SLICE_DAYS_COUNT, Date.today - 1].min

        params = export_params(config)
        params = params.merge(
          from_date: from_date.to_s,
          to_date: to_date.to_s,
        )

        records = client.export(params)
        sample_records = records.first(GUESS_RECORDS_COUNT)
        properties = Guess::SchemaGuess.from_hash_records(sample_records.map{|r| r["properties"]})
        columns = properties.map do |col|
          result = {
            name: col.name,
            type: col.type,
          }
          result[:format] = col.format if col.format
          result
        end
        columns.unshift(name: "event", type: :string)
        return {"columns" => columns}
      end

      def init
        @api_key = task[:api_key]
        @api_secret = task[:api_secret]
        @params = task[:params]
        @timezone = task[:timezone]
        @schema = task[:schema]
        @dates = task[:dates]
      end

      def run
        @dates.each_slice(SLICE_DAYS_COUNT) do |dates|
          Embulk.logger.info "Fetching data from #{dates.first} to #{dates.last} ..."

          fetch(dates).each do |record|
            values = @schema.map do |column|
              case column["name"]
              when "event"
                record["event"]
              when "time"
                time = record["properties"]["time"]
                adjust_timezone(time)
              else
                record["properties"][column["name"]]
              end
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

      def fetch(dates)
        from_date = dates.first
        to_date = dates.last
        params = @params.merge(
          "from_date" => from_date,
          "to_date" => to_date,
        )
        client = MixpanelApi::Client.new(@api_key, @api_secret)
        client.export(params)
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
    end

  end
end
