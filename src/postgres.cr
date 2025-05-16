require "json"

require "./pg/lib_pq"
require "./pg/utils"
require "./pg/x_log_data"
require "./pg/column"

class Postgres
  @connection : Pointer(LibPQ::PGconn)
  @slot : String
  @publication : String
  @stream : Stream(Record)
  @binary : Bool

  @relations = Hash(Int32, Array(PG::Column)).new
  @records : Array(Record) = [] of Record

  def initialize(config : Config, @stream : Stream)
    @slot = config.slot
    @publication = config.publication
    @binary = config.binary

    dsn = [
      config.host.presence && "host=#{config.host}",
      config.port.presence && "port=#{config.port}",
      config.user.presence && "user=#{config.user}",
      config.password.presence && "password=#{config.password}",
      config.database.presence && "dbname=#{config.database}",
      "replication=database",
      "sslmode=verify-full",
    ].compact.join(" ")

    @connection = LibPQ.connect(dsn)
    if LibPQ.status(@connection) != LibPQ::ConnStatusType::CONNECTION_OK
      error_msg = String.new(LibPQ.error_message(@connection))
      close
      raise "Failed to connect to PostgreSQL for replication: #{error_msg}"
    end
  end

  def close
    LibPQ.finish(@connection) if @connection
    @connection = Pointer(LibPQ::PGconn).null
  end

  def start
    position = @stream.position
    lsn = PG::LSN.format(position)
    Log.info { "Starting LSN: #{position} (#{lsn})" }

    start_command = "
      START_REPLICATION SLOT #{@slot} LOGICAL #{lsn}
      (proto_version '2', publication_names '#{@publication}', binary '#{@binary}')
    "

    result = LibPQ.exec(@connection, start_command)
    status = LibPQ.result_status(result)

    if status != LibPQ::ExecStatusType::PGRES_COPY_BOTH
      error_msg = String.new(LibPQ.result_error_message(result))
      LibPQ.clear(result)
      raise "Failed to start replication: #{error_msg}"
    end

    LibPQ.clear(result)

    process
  ensure
    close
  end

  def process
    buffer = Pointer(LibC::Char).null

    # @todo: wrap this in another loop with a PQconsumeInput before inner loop below?
    loop do
      bytes_read = LibPQ.get_copy_data(@connection, pointerof(buffer), 0)

      if bytes_read > 0
        # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA
        data = PG::XLogData.new(buffer, bytes_read)
        message_type = data.read_char
        case message_type
        when 'w' # WAL data
          #      [0] Byte ('w')
          Log.debug { "'w' - WAL data" }
          #   [1..8] Int64 (msg WAL start)
          data.skip(sizeof(Int64))
          #  [9..16] Int64 (WAL end)
          lsn = data.read_uint64
          Log.debug { "LSN #{lsn} (#{PG::LSN.format(lsn)})" }
          # [17..24] Int64 (server time)
          data.skip(sizeof(Int64))
          #   [25..] Byte* (message)
          process_message(data)
          feedback(lsn)

        when 'k' # Keepalive message
          #     [0] Byte ('k')
          Log.debug { "k - keepalive" }
          #  [1..8] Int64 (WAL end)
          lsn = data.read_uint64
          # [9..16] Int64 (server time)
          data.skip(sizeof(Int64))
          #    [17] Byte (reply request)
          repreq = data.read_uint8
          # @todo: only send feedback if repreq is set?
          feedback(lsn)
        end
        LibPQ.freemem(buffer)

      elsif bytes_read < 0
        error_msg = String.new(LibPQ.error_message(@connection))
        raise "Error reading from replication stream: #{error_msg}"
      end
    end
  end

  private def process_message(data)
    # https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html
    Log.trace { data.to_bytes }
    type = data.read_char
    case type
    when 'R'
      #    [0] Byte ('R')
      Log.debug { "'R' - relation" }
      # [1..4] Int32 (relation OID)
      oid = data.read_int32
      # [5...] CStr (namespace)
      _ = data.read_string
      # [ ...] CStr (relation)
      relation = data.read_string
      Log.debug { "relation: #{oid} -> #{relation}" }
      #    [x] Int8 (replica identity setting)
      data.skip(sizeof(UInt8))
      #  [..2] Int16 (column count)
      count = data.read_int16

      columns = count.times.map do
        # [0] Int8 (flags)
        flags = data.read_uint8
        # [1...] CStr (name)
        name = data.read_string
        #  [..4] Int32 (column type OID)
        coid = data.read_int32
        #  [..4] Int32 (type modifier)
        modifier = data.read_int32
        Log.debug { {name, coid, flags, modifier} }
        PG::Column.new(name, coid)
      end.to_a
      @relations[oid] = columns

    when 'I'
      # [0] Byte ('I')
      Log.debug { "'I' - insert" }
      # [1..4] Int32 (relation OID)
      oid = data.read_int32
      Log.debug { "relation: #{oid}" }
      # [5] Byte ('N')
      data.skip(sizeof(UInt8))

      # [6..] Data tuple
      # https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TUPLEDATA

      columns = @relations[oid]
      record = {} of String => JSON::Any::Type

      # [0..1] Int16 (column count)
      count = data.read_int16
      count.times do |index|
        column = columns[index]
        # [0] Byte (value type)
        value_type = data.read_char
        Log.debug { "type '#{value_type}' for #{{column.name, column.type}}" }

        case value_type
        when 'n'
          record[column.name] = nil
        when 'u'
          record[column.name] = nil # @todo: better value for this?
        when 't'
          # [1..4] Int32 (length)
          len = data.read_int32
          # [5..] Byte* (data)
          value = column.decode_text(data.read_slice(len))
          Log.debug { {value} }
          record[column.name] = value
        when 'b'
          # [1..4] Int32 (length)
          len = data.read_int32
          # [5..] Byte* (data)
          value = column.decode_binary(data.read_slice(len))
          Log.debug { {value} }
          record[column.name] = value
        else
          raise "Unknown value type: #{value_type}"
        end
      end
      @records << record

    when 'B'
      # [0] Byte ('B')
      Log.debug { "'B' - begin" }
      # [1..8] Int64 (txn final LSN)
      data.skip(sizeof(Int64))
      # [9..16] Int64 (commit timestamp)
      data.skip(sizeof(Int64))
      # [17..20] Int32 (txid)
      data.skip(sizeof(Int32))
      @records = [] of Record

    when 'C'
      # [0] Byte ('C')
      Log.debug { "'C' - commit" }
      # [1] Int8 (flags)
      data.skip(sizeof(UInt8))
      # [2..9] Int64 (commit LSN)
      data.skip(sizeof(Int64))
      # [10..17] Int64 (txn end LSN)
      txn_lsn = data.read_uint64
      Log.debug { "commit LSN #{txn_lsn}" }
      # [18..25] Int64 (commit timestamp)
      data.skip(sizeof(Int64))

      @records.each do |record|
        @stream.push(txn_lsn, record)
      end
    end
  end

  private def feedback(lsn : UInt64)
    # relation or some other information message that did
    # not advanced the received LSN position
    return if lsn.zero?

    Log.info { "Received LSN: #{lsn}" }
    replicated_lsn = @stream.position
    Log.info { "Replicated LSN: #{replicated_lsn}" }

    # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
    feedback = IO::Memory.new(34)
    feedback.write_byte('r'.ord.to_u8) # Standby status update (r)
    feedback.write_bytes(lsn, IO::ByteFormat::BigEndian) # received
    feedback.write_bytes(replicated_lsn, IO::ByteFormat::BigEndian) # flushed
    feedback.write_bytes(replicated_lsn, IO::ByteFormat::BigEndian) # applied
    feedback.write_bytes(PG::Timestamp.now, IO::ByteFormat::BigEndian)
    feedback.write_byte(0_u8) # no reply

    data = feedback.to_slice
    result = LibPQ.put_copy_data(
      @connection,
      data.to_unsafe.as(LibC::Char*),
      data.size
    )

    if result != 1
      error_msg = String.new(LibPQ.error_message(@connection))
      raise "Failed to send feedback: #{error_msg}"
    end

    if LibPQ.flush(@connection) != 0
      error_msg = String.new(LibPQ.error_message(@connection))
      raise "Failed to flush connection: #{error_msg}"
    end
  end
end
