require "timezone_validator"
require "override_assert_raise"

class TimezoneValidatorTest < Test::Unit::TestCase
  include OverrideAssertRaise

  def test_valid
    valid_timezone = "Asia/Tokyo"

    assert_nothing_raised do
      TimezoneValidator.new(valid_timezone).validate
    end
  end

  def test_invalid
    invalid_timezone = "Asia/Tokyoooooooooooooo"

    mock(Embulk.logger).error(/#{Regexp.new(invalid_timezone)}/)

    assert_raise(Embulk::ConfigError) do
      TimezoneValidator.new(invalid_timezone).validate
    end
  end
end
