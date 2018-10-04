require "range_generator"
require "override_assert_raise"
require "active_support/core_ext/time"

class RangeGeneratorTest < Test::Unit::TestCase
  include OverrideAssertRaise
  DEFAULT_TIMEZONE = "America/Chicago"
  DEFAULT_LOCAL = ActiveSupport::TimeZone["America/Chicago"]
  class GenerateRangeTest < self
    data do
      {
        from_date: ["aaaaaaaaa", 1],
        fetch_days: ["2010-01-01", -9],
      }
    end
    def test_invalid(args)
      assert_raise(Embulk::ConfigError) do
        generate_range(*args)
      end
    end

    def test_all_days_past
      days = 5
      from = "2010-01-01"
      expected_from = Date.parse(from)
      expected_to = Date.parse("2010-01-05")

      expected = (expected_from..expected_to).to_a.map{|date| date.to_s}

      actual = RangeGenerator.new(from, days, DEFAULT_TIMEZONE).generate_range

      assert_equal(expected, actual)
    end

    def test_1_fetch_day
      days = 1
      from = "2017-08-04"
      expected_from = Date.parse(from)
      expected_to = Date.parse("2017-08-04")
      expected = (expected_from..expected_to).to_a.map {|date| date.to_s}
      actual = RangeGenerator.new(from, days, DEFAULT_TIMEZONE).generate_range
      assert_equal(expected, actual)
    end

    def test_5_fetch_day
      days = 5
      from = "2017-08-04"
      expected_from = Date.parse(from)
      expected_to = Date.parse("2017-08-08")
      expected = (expected_from..expected_to).to_a.map {|date| date.to_s}
      actual = RangeGenerator.new(from, days, DEFAULT_TIMEZONE).generate_range
      assert_equal(expected, actual)
    end

    class OverDaysTest < self
      def setup
        @from = today - 5
        @days = 10
        @warn_message_regexp = /ignored them/
      end

      def test_range_only_present
        
        expected_to = today
        expected = (@from..expected_to).to_a.map{|date| date.to_s}

        stub(Embulk.logger).warn(@warn_message_regexp)

        assert_equal(expected, generate_range)
      end

      def test_warn
        mock(Embulk.logger).warn(@warn_message_regexp)

        generate_range
      end

      private

      def generate_range
        super(@from.to_s, @days)
      end
    end

    class FromDateEarlyTest < self
      def setup
        @from = today + 5
        @days = 10
        @warn_message_regexp = /allow 2 days/
      end

      def test_empty_range
        stub(Embulk.logger).warn(@warn_message_regexp)

        assert_equal([], generate_range)
      end

      def test_warn
        mock(Embulk.logger).warn(@warn_message_regexp)

        generate_range
      end

      private

      def generate_range
        super(@from.to_s, @days)
      end
    end

    private

    def generate_range(from_date_str, fetch_days)
      RangeGenerator.new(from_date_str, fetch_days, DEFAULT_TIMEZONE).generate_range
    end
    def today
      DEFAULT_LOCAL.today
    end
  end
end
