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
    lsn = PG::LSN.format(@stream.position)
    Log.info { "Starting LSN: #{lsn}" }

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
          #   [1..8] Int64 (msg WAL start)
          #  [9..16] Int64 (WAL end)
          # [17..24] Int64 (server time)
          #   [25..] Byte* (message)
          Log.debug { "w" }
          msg_lsn = data.read_uint64
          Log.debug { "ws #{msg_lsn}" }
          lsn = data.read_uint64
          Log.debug { "we #{lsn}" }
          data.skip(sizeof(Int64))
          process_message(data)
          feedback(lsn)

        when 'k' # Keepalive message
          #     [0] Byte ('k')
          #  [1..8] Int64 (WAL end)
          # [9..16] Int64 (server time)
          #    [17] Byte (reply request)
          Log.debug { "k" }
          # @todo: send this as "received" in reply?
          lsn = data.read_uint64
          data.skip(sizeof(Int64))
          repreq = data.read_uint8
          Log.debug { "ke #{lsn} #{repreq}" }
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
    Log.debug { data.to_bytes }
    type = data.read_char
    Log.debug { type }
    case type
    when 'R'
      #    [0] Byte ('R')
      # [1..4] Int32 (txid) - stream txn only
      # [5..8] Int32 (relation OID)
      # [9...] CStr (namespace)
      # [ ...] CStr (relation)
      #    [x] Int8 (replica identity setting)
      #  [..2] Int16 (column count)
      # ** each column:
      # [0] Int8 (flags)
      # [1...] CStr (name)
      #  [..4] Int32 (column type OID)
      #  [..4] Int32 (type modifier)
      oid = data.read_int32
      Log.debug { oid }
      namespace = data.read_string
      Log.debug { namespace }
      relation = data.read_string
      Log.debug { "relation: #{relation}" }
      data.skip(sizeof(UInt8))

      count = data.read_int16
      columns = count.times.map do
        flags = data.read_uint8
        name = data.read_string
        coid = data.read_int32
        modifier = data.read_int32
        Log.debug { {flags, name, coid, modifier} }
        PG::Column.new(name, coid)
      end.to_a
      @relations[oid] = columns

    when 'I'
      # [0] Byte ('I')
      # [1..4] Int32 (txid) - streamed txn only
      # [5..8] Int32 (relation OID)
      # [9] Byte ('N')
      # [10..] Data tuple
      oid = data.read_int32
      Log.debug { oid }
      data.skip(sizeof(UInt8))

      columns = @relations[oid]
      record = {} of String => JSON::Any::Type
      count = data.read_int16
      count.times do |index|
        column = columns[index]
        Log.debug { {column.name, column.type} }
        value_type = data.read_char
        Log.debug { value_type }
        case value_type
        when 'n'
          Log.debug { "null" }
          record[column.name] = nil
        when 'u'
          Log.debug { "unchanged" }
          record[column.name] = nil # @todo: better value for this?
        when 't'
          Log.debug { "text" }
          len = data.read_int32
          Log.debug { len }
          value = column.decode_text(data.read_slice(len))
          Log.debug { {value} }
          record[column.name] = value
        when 'b'
          Log.debug { "binary" }
          len = data.read_int32
          Log.debug { len }
          value = column.decode_binary(data.read_slice(len))
          Log.debug { {value} }
          record[column.name] = value
        else
          # @todo: raise exception
          Log.debug { "unknown" }
        end
      end
      @records << record

    when 'B'
      # [0] Byte ('B')
      # [1..8] Int64 (txn final LSN)
      # [9..16] Int64 (commit timestamp)
      # [17..20] Int32 (txid)
      txn_lsn = data.read_uint64
      Log.debug { txn_lsn }
      @records = [] of Record

    when 'C'
      # [0] Byte ('C')
      # [1] Int8 (flags)
      data.skip(sizeof(UInt8))
      # [2..9] Int64 (commit LSN)
      commit_lsn = data.read_uint64
      # [10..17] Int64 (txn end LSN)
      txn_lsn = data.read_uint64
      # [18..25] Int64 (commit timestamp)
      ci_ts = data.read_int64
      Log.debug { ci_ts }
      Log.debug { {commit_lsn, txn_lsn} }

      @records.each do |record|
        @stream.push(txn_lsn, record)
      end
    end
  end

  private def feedback(lsn : UInt64)
    Log.info { "Received LSN: #{PG::LSN.format(lsn)} (#{lsn})" }
    replicated_lsn = @stream.position
    Log.info { "Replicated LSN: #{PG::LSN.format(replicated_lsn)} (#{replicated_lsn})" }

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
