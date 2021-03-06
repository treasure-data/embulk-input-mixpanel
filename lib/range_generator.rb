require "active_support/core_ext/time"

class RangeGenerator
  attr_reader :from_date_str, :fetch_days, :profile_time_zone

  def initialize(from_date_str, fetch_days, profile_time_zone)
    @from_date_str = from_date_str
    @fetch_days = fetch_days
    @profile_time_zone = profile_time_zone
  end

  def generate_range
    validate
    show_warnings
    range_only_present.map{|date| date.to_s}
  end

  private

  def from_date
    Date.parse(from_date_str)
  end

  def validate
    begin
      from_date
    rescue ArgumentError # invalid date
      raise Embulk::ConfigError.new "from_date '#{from_date_str}' is invalid date"
    end

    if fetch_days && fetch_days < 1
      # `days` only allowed nil or positive number
      raise Embulk::ConfigError.new "fetch_days '#{fetch_days}' is invalid. Please specify bigger number than 0."
    end
  end

  def show_warnings
    if from_date_too_early?
      Embulk.logger.warn "Mixpanel allow 2 days before to from_date, so no data is input."
    end

    if overdays?
      Embulk.logger.warn "These dates are too early access, ignored them: from #{overdays.first} to #{overdays.last}"
    end
  end

  def range
    if from_date_too_early?
      return []
    end

    if fetch_days
      from_date..(from_date + fetch_days - 1)
    else
      from_date..today
    end
  end

  def range_only_present
    range.find_all{|date| date <= today}
  end

  def overdays?
    ! overdays.empty?
  end

  def overdays
    range.to_a - range_only_present.to_a
  end

  def from_date_too_early?
    from_date > today
  end

  def today
    zone = ActiveSupport::TimeZone[@profile_time_zone]
    Embulk.logger.warn "Coudn't find timezone support for #{@profile_time_zone}" if zone.nil?
    @today ||= (zone.nil? ? Date.today : zone.today)
  end
end
