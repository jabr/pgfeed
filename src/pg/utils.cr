struct PG::LSN
  getter value
  @value = 0_u64

  def initialize(from : String)
    hi, lo = from.split('/')
    @value = hi.to_u64 << 32 | lo.to_u64
  end

  def initialize(@value : UInt64)
  end

  def to_s : String
    "#{@value >> 32}/#{@value & 0xFFFFFFFF}"
  end

  def max(other : UInt64)
    @value = Math.max(@value, other)
    self
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
