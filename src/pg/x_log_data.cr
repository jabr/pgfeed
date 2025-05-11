class PG::XLogData
  def initialize(buffer : Pointer(UInt8), length : Int32)
    @data = IO::Memory.new(Slice.new(buffer, length))
  end

  def skip(bytes : Int32)
    @data.skip(bytes)
  end

  def read_byte
    @data.read_byte || 0
  end

  def read_char : Char
    read_byte.chr
  end

  def read_int32
    @data.read_bytes(Int32, IO::ByteFormat::BigEndian)
  end

  def read_int64
    @data.read_bytes(Int64, IO::ByteFormat::BigEndian)
  end

  def read_uint64
    @data.read_bytes(UInt64, IO::ByteFormat::BigEndian)
  end

  def read_string
    @data.gets(0.chr, Int32::MAX) || ""
  end
end
