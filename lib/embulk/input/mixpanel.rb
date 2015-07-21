require "embulk/input/mixpanel_api/client"

module Embulk
  module Input

    class Mixpanel < InputPlugin
      Plugin.register_input("mixpanel", self)

      def self.transaction(config, &control)
        # configuration code:
        task = {
          "property1" => config.param("property1", :string),
          "property2" => config.param("property2", :integer, default: 0),
        }

        columns = [
          Column.new(0, "example", :string),
          Column.new(1, "column", :long),
          Column.new(2, "value", :double),
        ]

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        commit_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        api_key = config.param(:api_key, :string)
        client = MixpanelApi::Client.new(api_key, config.param(:api_secret, :string))
        records = client.export(config_to_export_params(config))
        sample_records = records.first(10)
        properties = Guess::SchemaGuess.from_hash_records(sample_records.map{|r| r["properties"]})
        columns = properties.map do |col|
          {
            name: col.name,
            type: col.type,
            format: col.format
          }
        end
        columns.unshift({name: "event", type: "string"})
        return {"columns" => columns}
      end

      def init
        # initialization code:
        @property1 = task["property1"]
        @property2 = task["property2"]
      end

      def run
        page_builder.add(["example-value", 1, 0.1])
        page_builder.add(["example-value", 2, 0.2])
        page_builder.finish

        commit_report = {}
        return commit_report
      end

      private

      def self.config_to_export_params(config)
        {
          api_key: config.param(:api_key, :string),
          from_date: config.param(:from_date, :string),
          to_date: config.param(:to_date, :string),
          event: config.param(:event, :array, default: nil),
          where: config.param(:where, :string, default: nil),
          bucket: config.param(:bucket, :string, default: nil),
        }
      end
    end

  end
end
