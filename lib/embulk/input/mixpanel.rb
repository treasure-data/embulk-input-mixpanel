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

      # https://mixpanel.com/help/questions/articles/special-or-reserved-properties
      # https://mixpanel.com/help/questions/articles/what-properties-do-mixpanels-libraries-store-by-default
      #
      # JavaScript to extract key names from HTML: run it on Chrome Devtool when opening their document
      # > Array.from(document.querySelectorAll("strong")).map(function(s){ return s.textContent.match(/[A-Z]/) ? s.parentNode.textContent.match(/\((.*?)\)/)[1] : s.textContent.split(",").join(" ") }).join(" ")
      # > Array.from(document.querySelectorAll("li")).map(function(s){ m = s.textContent.match(/\((.*?)\)/); return m && m[1] }).filter(function(k) { return k && !k.match("utm") }).join(" ")
      KNOWN_KEYS = %W(
        #{NOT_PROPERTY_COLUMN}
        distinct_id ip mp_name_tag mp_note token time mp_country_code length campaign_id $email $phone $distinct_id $ios_devices $android_devices $first_name  $last_name  $name $city $region $country_code $timezone $unsubscribed
        $city $region mp_country_code $browser $browser_version $device $current_url $initial_referrer $initial_referring_domain $os $referrer $referring_domain $screen_height $screen_width $search_engine $city $region $mp_country_code $timezone $browser_version $browser $initial_referrer $initial_referring_domain $os $last_seen $city $region mp_country_code $app_release $app_version $carrier $ios_ifa $os_version $manufacturer $lib_version $model $os $screen_height $screen_width $wifi $city $region $mp_country_code $timezone $ios_app_release $ios_app_version $ios_device_model $ios_lib_version $ios_version $ios_ifa $last_seen $city $region mp_country_code $app_version $bluetooth_enabled $bluetooth_version $brand $carrier $has_nfc $has_telephone $lib_version $manufacturer $model $os $os_version $screen_dpi $screen_height $screen_width $wifi $google_play_services $city $region mp_country_code $timezone $android_app_version $android_app_version_code $android_lib_version $android_os $android_os_version $android_brand $android_model $android_manufacturer $last_seen
      ).uniq.freeze


      DEFAULT_FETCH_DAYS = 7
      DEFAULT_TIME_COLUMN = 'time'

      def self.transaction(config, &control)
        timezone = config.param(:timezone, :string)
        TimezoneValidator.new(timezone).validate

        from_date = config.param(:from_date, :string, default: (Date.today - 2).to_s)
        fetch_days = config.param(:fetch_days, :integer, default: nil)


        fetch_unknown_columns = config.param(:fetch_unknown_columns, :bool, default: false)

        incremental_column = config.param(:incremental_column, :string, default: nil)
        incremental = config.param(:incremental, :bool, default: true)
        latest_fetched_time = config.param(:latest_fetched_time, :integer, default: 0)

        # Backfill from date if incremental and an incremental field is set and we are in incremental run
        if incremental && incremental_column && latest_fetched_time !=0
          back_fill_days = config.param(:back_fill_days, :integer, default: 5)
          Embulk.logger.info "Backfill days #{back_fill_days}"
          from_date = (Date.parse(from_date) - back_fill_days).to_s
          fetch_days = fetch_days.nil? ? nil : fetch_days + back_fill_days
        end

        range = RangeGenerator.new(from_date, fetch_days).generate_range
        Embulk.logger.info "Try to fetch data from #{range.first} to #{range.last}"
        task = {
          params: export_params(config),
          dates: range,
          timezone: timezone,
          api_key: config.param(:api_key, :string),
          api_secret: config.param(:api_secret, :string),
          schema: config.param(:columns, :array),
          fetch_unknown_columns: fetch_unknown_columns,
          fetch_custom_properties: config.param(:fetch_custom_properties, :bool, default: true),
          retry_initial_wait_sec: config.param(:retry_initial_wait_sec, :integer, default: 1),
          incremental_column: incremental_column,
          retry_limit: config.param(:retry_limit, :integer, default: 5),
          latest_fetched_time: latest_fetched_time,
          incremental: incremental,
          slice_range: config.param(:slice_range, :integer, default: 7)
        }

        if task[:fetch_unknown_columns] && task[:fetch_custom_properties]
          raise Embulk::ConfigError.new("Don't set true both `fetch_unknown_columns` and `fetch_custom_properties`.")
        end

        columns = task[:schema].map do |column|
          name = column["name"]
          type = column["type"].to_sym

          Column.new(nil, name, type, column["format"])
        end

        if fetch_unknown_columns
          Embulk.logger.warn "Deprecated `unknown_columns`. Use `fetch_custom_properties` instead."
          columns << Column.new(nil, "unknown_columns", :json)
        end

        if task[:fetch_custom_properties]
          columns << Column.new(nil, "custom_properties", :json)
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        # NOTE: If this plugin supports to run by multi threads, this
        # implementation is terrible.
        if task[:incremental]
          task_report = task_reports.first
          next_to_date = Date.parse(task_report[:to_date])

          next_config_diff = {
            from_date: next_to_date.to_s,
            latest_fetched_time: task_report[:latest_fetched_time],
          }
          return next_config_diff
        end
        return {}
      end

      def self.guess(config)
        giveup_when_mixpanel_is_down

        retryer = perfect_retry({
          retry_initial_wait_sec: config.param(:retry_initial_wait_sec, :integer, default: 1),
          retry_limit: config.param(:retry_limit, :integer, default: 5),
        })

        client = MixpanelApi::Client.new(config.param(:api_key, :string), config.param(:api_secret, :string), retryer)

        range = guess_range(config)
        Embulk.logger.info "Guessing schema using #{range.first}..#{range.last} records"

        params = export_params(config).merge(
          "from_date" => range.first,
          "to_date" => range.last,
        )
        columns = guess_from_records(client.export_for_small_dataset(params))
        return {"columns" => columns}
      end

      def self.perfect_retry(task)
        PerfectRetry.new do |config|
          config.limit = task[:retry_limit]
          config.sleep = proc{|n| task[:retry_initial_wait_sec] * (2 * (n - 1)) }
          config.dont_rescues = [Embulk::ConfigError]
          config.rescues = [RuntimeError]
          config.log_level = nil
          config.logger = Embulk.logger
        end
      end

      def init
        @api_key = task[:api_key]
        @api_secret = task[:api_secret]
        @params = task[:params]
        @timezone = task[:timezone]
        @schema = task[:schema]
        @dates = task[:dates]
        @fetch_unknown_columns = task[:fetch_unknown_columns]
        @incremental_column = task[:incremental_column]
        @incremental = task[:incremental]
      end

      def run
        self.class.giveup_when_mixpanel_is_down
        prev_latest_fetched_time = task[:latest_fetched_time] || 0
        prev_latest_fetched_time_format = Time.at(prev_latest_fetched_time).strftime("%F %T %z")
        current_latest_fetched_time = prev_latest_fetched_time
        @dates.each_slice(task[:slice_range]) do |dates|
          ignored_record_count = 0
          unless preview?
            Embulk.logger.info "Fetching data from #{@dates.first} to #{dates.last} ..."
          end
          record_time_column=@incremental_column || DEFAULT_TIME_COLUMN
          fetch([@dates.first, dates.last], prev_latest_fetched_time).each do |record|
            if @incremental
              if !record["properties"].include?(record_time_column)
                raise Embulk::ConfigError.new("Incremental column not exists in fetched data #{record_time_column}")
              end
              record_time = record["properties"][record_time_column]
              if @incremental_column.nil?
                if record_time <= prev_latest_fetched_time
                  ignored_record_count += 1
                  next
                end
              end

              current_latest_fetched_time= [
                current_latest_fetched_time,
                record_time,
              ].max
            end
            values = extract_values(record)
            if @fetch_unknown_columns
              unknown_values = extract_unknown_values(record)
              values << unknown_values.to_json
            end
            if task[:fetch_custom_properties]
              values << collect_custom_properties(record)
            end
            page_builder.add(values)
          end
          prev_latest_fetched_time = [current_latest_fetched_time, prev_latest_fetched_time].max
          if ignored_record_count > 0
            Embulk.logger.warn "Skipped already loaded #{ignored_record_count} records. These record times are older or equal than previous fetched record time (#{prev_latest_fetched_time} @ #{prev_latest_fetched_time_format})."
          end
          break if preview?
        end
        page_builder.finish
        task_report = {
          latest_fetched_time: current_latest_fetched_time,
          to_date: @dates.last || Date.today - 1,
        }
        task_report
      end

      private

      def self.giveup_when_mixpanel_is_down
        unless MixpanelApi::Client.mixpanel_available?
          raise Embulk::DataError.new("Mixpanel service is down. Please retry later.")
        end
      end

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

      def collect_custom_properties(record)
        specified_columns = @schema.map{|col| col["name"]}
        custom_keys = record["properties"].keys.find_all{|key| !KNOWN_KEYS.include?(key.to_s) && !specified_columns.include?(key.to_s) }
        custom_keys.inject({}) do |result, key|
          result.merge({
            key => record["properties"][key]
          })
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

      def fetch(dates,last_fetch_time, &block)
        from_date = dates.first
        to_date = dates.last
        params = @params.merge(
          "from_date" => from_date,
          "to_date" => to_date
        )
        if !@incremental_column.nil? && !last_fetch_time.nil? && last_fetch_time!=0 # can't do filter on time column, time column need to be filter manually.
          params = params.merge(
            "where" => "#{params['where'].nil? ? '' : "(#{params['where']}) and " }properties[\"#{@incremental_column}\"] > #{last_fetch_time}"
          )
        end
        Embulk.logger.info "Where params is #{params["where"]}"
        client = MixpanelApi::Client.new(@api_key, @api_secret, self.class.perfect_retry(task))

        if preview?
          client.export_for_small_dataset(params)
        else
          Enumerator.new do |y|
            client.export(params) do |record|
              y << record
            end
          end
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
        Date.today - DEFAULT_FETCH_DAYS - 1
      end

      def self.guess_range(config)
        from_date = config.param(:from_date, :string, default: default_guess_start_date.to_s)
        fetch_days = config.param(:fetch_days, :integer, default: DEFAULT_FETCH_DAYS)
        range = RangeGenerator.new(from_date, fetch_days).generate_range
        if range.empty?
          return default_guess_start_date..(Date.today - 1)
        end
        range
      end

      def self.guess_from_records(records)
        sample_props = records.first(GUESS_RECORDS_COUNT).map {|r| r["properties"]}
        schema = Guess::SchemaGuess.from_hash_records(sample_props)
        columns = schema.map do |col|
          next if col.name == "time"
          result = {
            name: col.name,
            type: col.type,
          }
          result[:format] = col.format if col.format
          result
        end.compact
        columns.unshift(name: NOT_PROPERTY_COLUMN, type: :string)
        # Shift incremental column to top
        columns.unshift(name: "time", type: :long)
      end
    end

  end
end
