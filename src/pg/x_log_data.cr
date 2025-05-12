class PG::XLogData < IO::Memory
  def initialize(buffer : Pointer(LibC::Char), length : Int32)
    super(Slice.new(buffer.as(Pointer(UInt8)), length))
  end

  def to_bytes
    to_slice + pos
  end

  def read_uint8
    read_byte || 0_u8
  end

  def read_char : Char
    read_uint8.chr
  end

  def read_int16
    read_bytes(Int16, IO::ByteFormat::BigEndian)
  end

  def read_int32
    read_bytes(Int32, IO::ByteFormat::BigEndian)
  end

  def read_int64
    read_bytes(Int64, IO::ByteFormat::BigEndian)
  end

  def read_uint64
    read_bytes(UInt64, IO::ByteFormat::BigEndian)
  end

  def read_string
    gets(0.chr, true) || ""
  end

  def read_slice(len : Int32)
    slice = to_slice[@pos, len]
    skip(len)
    slice
  end
end
