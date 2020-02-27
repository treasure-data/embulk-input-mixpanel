require "embulk/input/service/jql_service"
require "embulk/input/service/export_service"
require "pry"

module Embulk
  module Input
    class Mixpanel < InputPlugin
      Plugin.register_input("mixpanel", self)
      
      def self.transaction(config, &control)
        service = service(config)
        service.validate_config
        task = service.create_task

        columns = task[:schema].map do |column|
          name = column["name"]
          type = column["type"].to_sym

          Column.new(nil, name, type, column["format"])
        end

        if task[:fetch_custom_properties]
          columns << Column.new(nil, "custom_properties", :json)
        end

        if task[:fetch_unknown_columns]
          Embulk.logger.warn "Deprecated `unknown_columns`. Use `fetch_custom_properties` instead."
          columns << Column.new(nil, "unknown_columns", :json)
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        # NOTE: If this plugin supports to run by multi threads, this
        # implementation is terrible.
        if task[:incremental]
          task_report = task_reports.first
          service = service(task)
          next_config_diff = service.create_next_config_diff(task_report)
          return next_config_diff
        end
        return {}
      end

      def self.guess(config)
        service = service(config)
        service.validate_config
        return {"columns"=>service.guess_columns}
      end

      def init
        @export_endpoint = task[:export_endpoint]
        @api_secret = task[:api_secret]
      end

      def run

        Mixpanel::service(task).ingest(task, page_builder)
      end

      private

      def self.service(config)
        jql_mode = config[:jql_mode]
        if jql_mode
          Service::JqlService.new(config)
        else
          Service::ExportService.new(config)
        end
      end
    end
  end
end
