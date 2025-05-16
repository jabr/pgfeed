module PG::LSN
  def self.parse(from : String) : UInt64
    hi, lo = from.split('/')
    return hi.to_u64(16) << 32 | lo.to_u64(16)
  end

  def self.format(value : UInt64) : String
    hi = (value >> 32).to_s(16, upcase: true)
    lo = (value & 0xFFFFFFFF).to_s(16, upcase: true)
    "#{hi}/#{lo}"
  end

  def self.validate(from : String) : String
    format(parse(from))
  end
end

# Add microseconds since unix epoch method to Time
struct Time
  def to_unix_us
    to_unix * 1_000_000 + nanosecond // 1_000
  end
end

module PG::Timestamp
  private EPOCH = Time.utc(2000, 1, 1)

  def self.now : Int64
    (Time.utc - EPOCH).total_microseconds.to_i64
  end

  def self.from(microseconds_since_pg_epoch : Int64) : Time
    span = Time::Span.new(
       seconds: (microseconds_since_pg_epoch / 1_000_000).floor.to_i,
       nanoseconds: (microseconds_since_pg_epoch % 1_000_000) * 1000
    )
    return EPOCH + span
  end

  def self.parse(postgres_datetime : String) : Time
    (Time::Format::YAML_DATE.parse?(postgres_datetime) || raise "Invalid datetime").to_utc
  end
end
