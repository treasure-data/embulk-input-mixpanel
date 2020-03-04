require 'embulk/input/service/base_service'

module Embulk
  module Input
    module Service
      class JqlService < BaseService

        FROM_DATE_PARAM = "params.from_date"
        TO_DATE_PARAM = "params.to_date"

        def validate_config
          super

          validate_jql_script
          validate_fetch_days

          if @config.param(:incremental, :bool, default: true)
            validate_jql_script_contain_time_params
          end
        end

        def create_task
          {
            timezone: @config.param(:timezone, :string, default: ""),
            api_secret: @config.param(:api_secret, :string),
            jql_endpoint: endpoint,
            dates: range,
            incremental: @config.param(:incremental, :bool, default: true),
            slice_range: @config.param(:slice_range, :integer, default: 7),
            schema: @config.param(:columns, :array),
            retry_initial_wait_sec: @config.param(:retry_initial_wait_sec, :integer, default: 1),
            retry_limit: @config.param(:retry_limit, :integer, default: 5),
            incremental_column: @config.param(:incremental_column, :string, default: nil),
            latest_fetched_time: @config.param(:latest_fetched_time, :integer, default: 0),
            jql_mode: true,
            jql_script: @config.param(:jql_script, :string, nil)
          }
        end

        def guess_columns
          giveup_when_mixpanel_is_down
          range = guess_range
          Embulk.logger.info "Guessing schema using #{range.first}..#{range.last}"

          client = create_client

          sample_records = client.send_jql_script_small_dataset(parameters(@config.param(:jql_script, :string, nil), range.first, range.last))
          guess_from_records(sample_records)
        end

        def ingest(task, page_builder)
          @dates = task[:dates]
          @schema = task[:schema]
          @timezone = task[:timezone]
          incremental_column = task[:incremental_column]
          latest_fetched_time = task[:latest_fetched_time]
          incremental = task[:incremental]

          client = create_client

          ignored_fetched_record_count = 0
          next_fetched_time = latest_fetched_time
          @dates.each_slice(task[:slice_range]) do |slice_dates|
            Embulk.logger.info "Fetching date from #{slice_dates.first}..#{slice_dates.last}"
            if preview?
              records = client.send_jql_script_small_dataset(parameters(@config.param(:jql_script, :string, default: nil), slice_dates.first, slice_dates.last))
            else
              records = client.send_jql_script(parameters(task[:jql_script], slice_dates.first, slice_dates.last))
            end
            validate_result(records)
            records.each do |record|
              if incremental
                unless incremental_column
                  Embulk.logger.warn "incremental_column should be specified when running in incremental mode to avoid duplicated"
                  Embulk.logger.warn "Use default value #{DEFAULT_TIME_COLUMN}"
                  incremental_column = DEFAULT_TIME_COLUMN
                end

                if @schema.map {|col| col["name"]}.include?(incremental_column)
                  record_incremental_column = record[incremental_column.to_sym]
                  if record_incremental_column
                    if record_incremental_column <= latest_fetched_time.to_i
                      ignored_fetched_record_count += 1
                      next
                    else
                      next_fetched_time = [record_incremental_column, latest_fetched_time.to_i].max
                    end
                  end
                end
              end
              values = extract_values(record)
              page_builder.add(values)
            end
            break if preview?
          end
          Embulk.logger.info "Skip #{ignored_fetched_record_count} rows"
          page_builder.finish

          if task[:incremental] && !preview?
            return create_task_report(next_fetched_time)
          end
          {}
        end

        def guess_range
          time_zone = @config.param(:timezone, :string, default: "")
          from_date = @config.param(:from_date, :string, default: default_guess_start_date(time_zone).to_s)
          fetch_days = @config.param(:fetch_days, :integer, default: DEFAULT_FETCH_DAYS)

          fetch_days = [fetch_days, DEFAULT_FETCH_DAYS].min

          range = RangeGenerator.new(from_date, fetch_days, time_zone).generate_range
          if range.empty?
            return default_guess_start_date(time_zone)..(today(time_zone) - 1)
          end
          range
        end

        def guess_from_records(sample_props)
          validate_result(sample_props)
          schema = Guess::SchemaGuess.from_hash_records(sample_props)

          schema.map do |col|
            result = {
              name: col.name,
              type: col.type,
            }
            if (col.name.eql? "time") || (col.eql? "last_seen")
              result[:format] = col.format if col.format
            end
            result
          end

          begin
            schema = Guess::SchemaGuess.from_hash_records(sample_props)
            schema.map do |col|
              result = {
                name: col.name,
                type: col.type,
              }
              if (col.name.eql? "time") || (col.eql? "last_seen")
                result[:format] = col.format if col.format
              end
              result
            end
          rescue DataError
            raise Embulk::ConfigError.new("Non-supported result #{sample_props}. Revise your JQL.")
          end
        end

        def parameters(script, from_date, to_date)
          {
            params: params(from_date, to_date),
            script: script
          }
        end

        def adjust_timezone(epoch)
          # Adjust timezone offset to get UTC time
          # c.f. https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel#export
          tz = TZInfo::Timezone.get(@timezone)

          begin
            if epoch.present?
              offset = tz.period_for_local(epoch, true).offset.utc_total_offset
              epoch - offset
            end
              # TZInfo::PeriodNotFound signals that there is no equivalent UTC time (for example,
              # during the transition from standard time to daylight savings time when the clocks are moved forward and an hour is skipped).
              # tz.local_time(2018, 3, 11, 2, 30, 0, 0)
          rescue TZInfo::PeriodNotFound
            epoch + 1.hour
            offset = tz.period_for_local(epoch, true).offset.utc_total_offset
            epoch - offset
          end
        end

        def next_from_date(task_report)
          next_to_date = Date.parse(task_report[:to_date])
          {
            from_date: next_to_date.to_s,
            latest_fetched_time: task_report[:latest_fetched_time],
          }
        end

        def endpoint
          @config.param(:jql_endpoint, :string, default: Embulk::Input::MixpanelApi::Client::DEFAULT_JQL_ENDPOINT)
        end

        private

        def create_task_report(next_fetched_time)
          {
            to_date: @dates.last || today(@timezone) - 1,
            latest_fetched_time: next_fetched_time.to_s
          }
        end

        def params(from_date, to_date)
          {
            from_date: from_date,
            to_date: to_date
          }
        end

        def range
          timezone = @config.param(:timezone, :string, default: "")
          from_date = @config.param(:from_date, :string, default: (today(timezone) - 2).to_s)
          fetch_days = @config.param(:fetch_days, :integer, default: nil)

          RangeGenerator.new(from_date, fetch_days, timezone).generate_range
        end

        def extract_value(record, name)
          case name
          when NOT_PROPERTY_COLUMN
            record[NOT_PROPERTY_COLUMN]
          when "time"
            time = record[:time]
            adjust_timezone(time)
          when "last_seen"
            last_seen = record[:last_seen]
            adjust_timezone(last_seen)
          else
            record[name]
          end
        end

        def validate_result(records)
          if records.is_a?(Array) && records.first.is_a?(Integer)
            # incase using reduce, it only return the number of records
            raise Embulk::ConfigError.new("Non-supported result. Revise your JQL.")
          end
        end

        def validate_jql_script_contain_time_params
          client = create_client

          params_script_only = {
            params: {},
            script: @config[:jql_script]
          }

          response = client.send_brief_checked_jql_script(params_script_only)

          if response
            message = response["error"]
            if message
              unless message.include?("argument must be an object with 'from_date' and 'to_date' properties")
                Embulk.logger.warn "Missing params.start_date and params.end_date in the JQL. Use these parameters to limit the amount of returned data."
              end
            end
          end
        end

        def validate_jql_script
          jql_script = @config.param(:jql_script, :string, default: nil)
          if jql_script.blank?
            raise Embulk::ConfigError.new("JQL script shouldn't be empty or null")
          end
        end

        def validate_fetch_days
          fetch_days = @config.param(:fetch_days, :integer, default: nil)
          if fetch_days && fetch_days <= 0
            raise Embulk::ConfigError.new("fetch_days should be larger than 0")
          end
        end

      end
    end
  end
end
