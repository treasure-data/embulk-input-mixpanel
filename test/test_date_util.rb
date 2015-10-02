require "date_util"

class DateUtilTest < Test::Unit::TestCase
  class ValidateTest < self
    def test_invalid_from_date
      util = DateUtil.new("aaaaaaaaa", 1, valid_timezone)
      assert_raise(Embulk::ConfigError) do
        util.validate
      end
    end

    def test_invalid_fetch_days
      util = DateUtil.new("2010-01-01", -9, valid_timezone)
      assert_raise(Embulk::ConfigError) do
        util.validate
      end
    end

    def test_invalid_timezone
      util = DateUtil.new("2010-01-01", 1, invalid_timezone)
      mock(Embulk.logger).error(/#{Regexp.new(invalid_timezone)}/)
      assert_raise(Embulk::ConfigError) do
        util.validate
      end
    end
  end

  class RangeTest < self
    def setup
    end

    class AllPastTest < self
      def setup
        @from = Date.parse("2010-01-01")
        @days = 5
        @util = DateUtil.new("2010-01-01", 5, valid_timezone)
      end

      def test_range
        assert_equal @util.range, @from..Date.parse("2010-01-05")
      end

      def test_overdays
        assert_equal @util.overdays, []
      end

      def test_overdays?
        assert_equal @util.overdays?, false
      end

      def test_from_date_too_early?
        assert_equal @util.from_date_too_early?, false
      end
    end

    class OverdaysTest < self
      def setup
        @from = Date.today - 5
        @days = 10
        @util = DateUtil.new(@from.to_s, @days, valid_timezone)
      end

      def test_range_only_past
        assert_equal @util.range_only_past, @util.range.find_all{|d| d < Date.today}
      end

      def test_overdays
        assert_equal @util.overdays, @util.range.find_all{|d| d >= Date.today}
      end

      def test_overdays?
        assert @util.overdays?
      end

      def test_from_date_too_early?
        assert_equal @util.from_date_too_early?, false
      end
    end

    class FromDateEarlyTest < self
      def setup
        @from = Date.today + 5
        @days = 10
        @util = DateUtil.new(@from.to_s, @days, valid_timezone)
      end

      def test_from_date_too_early?
        assert @util.from_date_too_early?
      end
    end
  end

  def valid_timezone
    "Asia/Tokyo"
  end

  def invalid_timezone
    "Asia/Tokyoooooooooooooo"
  end
end
