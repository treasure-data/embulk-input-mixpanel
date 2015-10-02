require "date_util"

class DateUtilTest < Test::Unit::TestCase
  class GenerateRangeTest < self
    data do
      valid_timezone = "Asia/Tokyo"

      {
        from_date: ["aaaaaaaaa", 1, valid_timezone],
        fetch_days: ["2010-01-01", -9, valid_timezone],
      }
    end
    def test_invalid(args)
      assert_raise(Embulk::ConfigError) do
        generate_range(*args)
      end
    end

    # TODO: timezone validation should be moved to othe class
    def test_invalid_timezone
      mock(Embulk.logger).error(/#{Regexp.new(invalid_timezone)}/)
      assert_raise(Embulk::ConfigError) do
        generate_range("2010-01-01", 1, invalid_timezone)
      end
    end

    def test_all_days_past
      days = 5
      from = "2010-01-01"
      expected_from = Date.parse(from)
      expected_to = Date.parse("2010-01-05")

      expected = (expected_from..expected_to).to_a.map{|date| date.to_s}

      actual = DateUtil.new(from, days, valid_timezone).generate_range

      assert_equal(expected, actual)
    end

    class OverDaysTest < self
      def setup
        @from = Date.today - 5
        @days = 10
        @warn_message_regexp = /ignored them/
      end

      def test_range_only_past
        expected_to = Date.today - 1
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
        super(@from.to_s, @days, valid_timezone)
      end
    end

    class FromDateEarlyTest < self
      def setup
        @from = Date.today + 5
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
        super(@from.to_s, @days, valid_timezone)
      end
    end

    private

    def valid_timezone
      "Asia/Tokyo"
    end

    def invalid_timezone
      "Asia/Tokyoooooooooooooo"
    end

    def generate_range(from_date_str, fetch_days, timezone)
      DateUtil.new(from_date_str, fetch_days, timezone).generate_range
    end
  end
end
