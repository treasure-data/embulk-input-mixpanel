class DateUtil
  attr_reader :from_date_str, :fetch_days, :timezone

  def initialize(from_date_str, fetch_days, timezone)
    @from_date_str = from_date_str
    @fetch_days = fetch_days
    @timezone = timezone
  end

  def from_date
    Date.parse(from_date_str)
  end

  def validate
    begin
      from_date
    rescue ArgumentError # invalid date
      raise Embulk::ConfigError, "from_date '#{from_date_str}' is invalid date"
    end

    if fetch_days && fetch_days < 1
      # `days` only allowed nil or positive number
      raise Embulk::ConfigError, "fetch_days '#{fetch_days}' is invalid. Please specify bigger number than 0."
    end

    begin
      # raises exception if timezone is invalid string
      TZInfo::Timezone.get(timezone)
    rescue => e
      Embulk.logger.error "'#{timezone}' is invalid timezone"
      raise Embulk::ConfigError, e.message
    end
  end

  def range
    validate

    if from_date_too_early?
      return []
    end

    if fetch_days
      from_date..(from_date + fetch_days - 1)
    else
      from_date..yesterday
    end
  end

  def range_only_past
    range.find_all{|date| date < today}
  end

  def overdays?
    ! overdays.empty?
  end

  def overdays
    range.to_a - range_only_past.to_a
  end

  def from_date_too_early?
    from_date > yesterday
  end

  def yesterday
    today - 1
  end

  def today
    @today ||= Date.today
  end
end
