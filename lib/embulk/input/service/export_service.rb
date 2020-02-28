require 'embulk/input/service/base_service'

module Embulk
  module Input
    module Service
      class ExportService < BaseService

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

        def validate_config
          super

          incremental_column = @config.param(:incremental_column, :string, default: nil)
          latest_fetched_time = @config.param(:latest_fetched_time, :integer, default: 0)
          fetch_custom_properties = @config.param(:fetch_custom_properties, :bool, default: true)
          fetch_unknown_columns = @config.param(:fetch_unknown_columns, :bool, default: false)

          if !incremental_column.nil? && !latest_fetched_time.nil? && (incremental_column_upper_limit <= latest_fetched_time)
            raise Embulk::ConfigError.new("Incremental column upper limit (job_start_time - incremental_column_upper_limit_delay_in_seconds) can't be smaller or equal latest fetched time #{latest_fetched_time}")
          end

          if fetch_unknown_columns && fetch_custom_properties
            raise Embulk::ConfigError.new("Don't set true both `fetch_unknown_columns` and `fetch_custom_properties`.")
          end
        end

        def create_task
          {
            params: export_params,
            dates: range,
            timezone: @config.param(:timezone, :string, default: ""),
            export_endpoint: export_endpoint,
            api_secret: @config.param(:api_secret, :string),
            schema: @config.param(:columns, :array),
            fetch_unknown_columns: @config.param(:fetch_unknown_columns, :bool, default: false),
            fetch_custom_properties: @config.param(:fetch_custom_properties, :bool, default: true),
            retry_initial_wait_sec: @config.param(:retry_initial_wait_sec, :integer, default: 1),
            incremental_column: @config.param(:incremental_column, :string, default: nil),
            retry_limit: @config.param(:retry_limit, :integer, default: 5),
            latest_fetched_time: @config.param(:latest_fetched_time, :integer, default: 0),
            incremental: @config.param(:incremental, :bool, default: true),
            slice_range: @config.param(:slice_range, :integer, default: 7),
            job_start_time: Time.now.to_i * 1000,
            incremental_column_upper_limit: incremental_column_upper_limit,
            allow_partial_import: @config.param(:allow_partial_import, :bool, default: true)
          }
        end

        def create_next_config_diff(task_report)
          next_to_date = Date.parse(task_report[:to_date])
          {
            from_date: next_to_date.to_s,
            latest_fetched_time: task_report[:latest_fetched_time],
          }
        end

        def ingest(task, page_builder)
          giveup_when_mixpanel_is_down

          @schema = task[:schema]
          @timezone = task[:timezone]

          Embulk.logger.info "Job start time is #{task[:job_start_time]}"

          dates = task[:dates]
          prev_latest_fetched_time = task[:latest_fetched_time] || 0
          prev_latest_fetched_time_format = Time.at(prev_latest_fetched_time).strftime("%F %T %z")
          current_latest_fetched_time = prev_latest_fetched_time
          incremental_column = task[:incremental_column]
          incremental = task[:incremental]
          fetch_unknown_columns = task[:fetch_unknown_columns]

          dates.each_slice(task[:slice_range]) do |slice_dates|
            ignored_fetched_record_count = 0
            # There is the issue with Mixpanel time field during the transition from standard to daylight saving time
            # in the US timezone i.e. 11 Mar 2018 2AM - 2:59AM, time within that period must not be existed,
            # due to daylight saving, time will be forwarded 1 hour from 2AM to 3AM.
            #
            # All of records with wrong timezone will be ignored instead of throw exception out
            ignored_wrong_daylight_tz_record_count = 0
            unless preview?
              Embulk.logger.info "Fetching data from #{slice_dates.first} to #{slice_dates.last} ..."
            end
            record_time_column = incremental_column || DEFAULT_TIME_COLUMN
            begin
              fetch(slice_dates, prev_latest_fetched_time, task).each do |record|
                if incremental
                  if !record["properties"].include?(record_time_column)
                    raise Embulk::ConfigError.new("Incremental column not exists in fetched data #{record_time_column}")
                  end
                  record_time = record["properties"][record_time_column]
                  if incremental_column.nil?
                    if record_time <= prev_latest_fetched_time
                      ignored_fetched_record_count += 1
                      next
                    end
                  end

                  current_latest_fetched_time = [
                    current_latest_fetched_time,
                    record_time,
                  ].max
                end
                begin
                  values = extract_values(record)
                  if fetch_unknown_columns
                    unknown_values = extract_unknown_values(record)
                    values << unknown_values.to_json
                  end
                  if task[:fetch_custom_properties]
                    values << collect_custom_properties(record)
                  end
                  page_builder.add(values)
                rescue TZInfo::PeriodNotFound
                  ignored_wrong_daylight_tz_record_count += 1
                end
              end
            rescue MixpanelApi::IncompleteExportResponseError
              if !task[:allow_partial_import]
                #   re raise the exception if we don't allow partial import
                raise
              end
            end
            if ignored_fetched_record_count > 0
              Embulk.logger.warn "Skipped already loaded #{ignored_fetched_record_count} records. These record times are older or equal than previous fetched record time (#{prev_latest_fetched_time} @ #{prev_latest_fetched_time_format})."
            end
            if ignored_wrong_daylight_tz_record_count > 0
              Embulk.logger.warn "Skipped #{ignored_wrong_daylight_tz_record_count} records due to corrupted Mixpanel time transition from standard to daylight saving"
            end
            break if preview?
          end
          page_builder.finish
          create_task_report(current_latest_fetched_time, dates.last, task[:timezone])
        end

        def create_task_report(current_latest_fetched_time, to_date, timezone)
          {
            latest_fetched_time: current_latest_fetched_time,
            to_date: to_date || today(timezone) - 1,
          }
        end

        def guess_columns
          giveup_when_mixpanel_is_down
          range = guess_range
          Embulk.logger.info "Guessing schema using #{range.first}..#{range.last} records"

          params = export_params.merge(
            "from_date"=>range.first,
            "to_date"=>range.last,
          )

          client = create_client
          guess_from_records(client.export_for_small_dataset(params))
        end

        def export_params
          event = @config.param(:event, :array, default: nil)
          event = event.nil? ? nil : event.to_json
          {
            event: event,
            where: @config.param(:where, :string, default: nil),
            bucket: @config.param(:bucket, :string, default: nil),
          }
        end

        def guess_from_records(records)
          sample_props = records.map {|r| r["properties"]}
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

        def fetch(dates, last_fetch_time, task, &block)
          from_date = dates.first
          to_date = dates.last
          params = task[:params].merge(
            "from_date"=>from_date,
            "to_date"=>to_date
          )
          incremental_column = task[:incremental_column]
          if !incremental_column.nil? # can't do filter on time column, time column need to be filter manually.
            params = params.merge(
              "where"=>"#{params['where'].nil? ? '' : "(#{params['where']}) and " }properties[\"#{incremental_column}\"] > #{last_fetch_time || 0} and properties[\"#{incremental_column}\"] < #{task[:incremental_column_upper_limit]}"
            )
          end
          Embulk.logger.info "Where params is #{params["where"]}"

          client = create_client

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

        private

        def incremental_column_upper_limit
          job_start_time = Time.now.to_i * 1000
          upper_limit_delay = @config.param(:incremental_column_upper_limit_delay_in_seconds, :integer, default: 0)
          job_start_time - (upper_limit_delay * 1000)
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
          specified_columns = @schema.map {|col| col["name"]}
          custom_keys = record["properties"].keys.find_all {|key| !KNOWN_KEYS.include?(key.to_s) && !specified_columns.include?(key.to_s)}
          custom_keys.inject({}) do |result, key|
            result.merge({
              key=>record["properties"][key]
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
