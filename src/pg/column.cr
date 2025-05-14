# https://jdbc.postgresql.org/documentation/publicapi/constant-values.html
# https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L32

struct PG::Column
  enum Type : Int32
    # integers
    INT2 = 21
    INT4 = 23
    INT8 = 20
    # floats
    FLOAT4 = 700
    FLOAT8 = 701
    # strings
    TEXT = 25
    # datetime
    DATE = 1082
    TIME = 1083
    TIMESTAMP = 1114
    TIMESTAMPZ = 1184
    # json
    JSON = 114
    JSONB = 3802
    # other
    BOOL = 16
    UUID = 2950
    NUMERIC = 1700
  end

  getter name
  @type : Type

  def initialize(@name : String, type : Int32)
    @type = Type[type]
  end

  def type
    @type.to_s
  end

  def decode_text(data : Bytes)
    str = String.new(data)
    case @type
    when Type::BOOL
      return str == "t"
    when Type::INT2, Type::INT4, Type::INT8
      return str.to_i64
    when Type::FLOAT4, Type::FLOAT8
      return str.to_f64
    when Type::DATE, Type::TIMESTAMP, Type::TIMESTAMPZ
      return PG::Timestamp.parse(str).to_unix_us
    when Type::JSON, Type::JSONB
      return JSON.parse(str).raw
    when Type::TIME, Type::UUID, Type::NUMERIC
      # @todo: anything better to do with these?
      return str
    when Type::TEXT
      return str
    else
      # just return the string as-is
      return str
    end
  end

  def decode_binary(data : Bytes)
    Log.debug { "decode binary #{@type} : #{data}}" }
    case @type
    when Type::TIMESTAMPZ
      return PG::Timestamp.from(
        IO::ByteFormat::BigEndian.decode(Int64, data)
      ).to_unix_us
    when Type::INT8
      return IO::ByteFormat::BigEndian.decode(Int64, data)
    when Type::TEXT
      return String.new(data)
    when Type::JSONB
      version = data[0]
      unless version == 1
        raise "Unsupported binary JSONB version: #{version}"
      end
      return JSON.parse(String.new(data + 1)).raw
    else
      raise "Unsupported binary type: #{@type.to_s}"
    end
  end
end
