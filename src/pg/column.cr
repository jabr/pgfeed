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
    when Type::TEXT
      return str
    else
      return str
    end
  end
end
