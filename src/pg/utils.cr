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

module PG
  private EPOCH = Time.utc(2000, 1, 1)

  def self.now : Int64
    (Time.utc - EPOCH).total_microseconds.to_i64
  end
end
