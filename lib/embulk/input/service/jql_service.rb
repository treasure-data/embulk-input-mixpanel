require 'embulk/input/service/base_service'
require 'pry'
module Embulk
  module Input
    module Service
      class JqlService < BaseService

        def validate_config
          super
          jql_script = @config.param(:jql_script, :string, default: nil)

          validate_jql_script(jql_script)
        end

        def create_task
          {
            timezone: @config.param(:timezone, :string, default: ""),
            api_secret: @config.param(:api_secret, :string),
            export_endpoint: export_endpoint,
            incremental: @config.param(:incremental, :bool, default: true),
            dates: range,
            schema: @config.param(:columns, :array),
            retry_initial_wait_sec: @config.param(:retry_initial_wait_sec, :integer, default: 1),
            retry_limit: @config.param(:retry_limit, :integer, default: 5),
            jql_mode: true,
            jql_script: @config.param(:jql_script, :string, nil)
          }
        end

        def create_next_config_diff(task_report)
          next_to_date = Date.parse(task_report[:to_date]).next_day(1)
          {
            from_date: next_to_date.to_s
          }
        end

        def guess_columns
          sample_records = []
          range = guess_range
          giveup_when_mixpanel_is_down
          Embulk.logger.info "Guessing schema using #{range.first}..#{range.last}"
          client = create_client

          client.send_jql_script_small_dataset(parameters(range.first, range.last, @config.param(:jql_script, :string, nil))) do |record|
            sample_records << record
          end

          guess_from_records(sample_records)
        end

        def ingest(task, page_builder)
          giveup_when_mixpanel_is_down
          @dates = task[:dates]
          @schema = task[:schema]
          @timezone = task[:timezone]

          client = create_client

          if preview?
            client.send_jql_script_small_dataset(parameters(@dates.first, @dates.last, task[:jql_script])) do |record|
              values = extract_values(record)
              page_builder.add(values)
            end
          else
            client.send_jql(parameters(@dates.first, @dates.last, task[:jql_script])) do |record|
              values = extract_values(record)
              page_builder.add(values)
            end
          end
          page_builder.finish
          create_task_report
        end

        private

        def create_task_report
          {
            to_date: @dates.last || today(@timezone) - 1,
          }
        end

        def parameters(from_date, to_date, script)
          {
            params: params(from_date, to_date),
            script: script
          }
        end

        def params(from_date, to_date)
          {
            from_date: from_date,
            to_date: to_date
          }
        end

        def guess_from_records(sample_props)
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
        end

        def validate_jql_script(jql_script)
          if jql_script.blank?
            raise Embulk::ConfigError.new("JQL script shouldn't be empty or null")
          end
        end

        def extract_value(record, name)
          case name
          when NOT_PROPERTY_COLUMN
            record[NOT_PROPERTY_COLUMN]
          when "time"
            time = record["time"]
            adjust_timezone(time)
          when "last_seen"
            last_seen = record["last_seen"]
            adjust_timezone(last_seen)
          else
            record[name]
          end
        end

        def range
          timezone = @config.param(:timezone, :string, default: "")
          from_date = @config.param(:from_date, :string, default: (today(timezone) - 2).to_s)
          fetch_days = @config.param(:fetch_days, :integer, default: nil)

          RangeGenerator.new(from_date, fetch_days, timezone).generate_range
        end
      end
    end
  end
end
