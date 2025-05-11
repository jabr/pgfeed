require "json"

require "./pg/lib_pq"
require "./pg/utils"
require "./pg/x_log_data"

require "log"
# @todo: switch back to async default once we're running
# in an isolated exec context.
Log.setup do
  Log::IOBackend.new dispatcher: :sync
end

class PG::PQ
  @connection : Pointer(LibPQ::PGconn)
  @slot : String
  @publication : String

  struct Config
    getter host, port, user, password, database, slot, publication

    def initialize(
      *,
      @host = "",
      @port = "",
      @user = "",
      @password = "",
      @database = "",
      @slot : String,
      @publication : String,
    )
    end
  end

  def initialize(config : Config, sink : Channel)
    @slot = config.slot
    @publication = config.publication
    @lsn = LSN.new("0/0")

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

  def start(lsn : String)
    @lsn.max(LSN.new(lsn).value)
    puts("Starting LSN: #{@lsn.to_s}")

    start_command = "
      START_REPLICATION SLOT #{@slot} LOGICAL #{@lsn.to_s}
      (proto_version '2', publication_names '#{@publication}', binary 'true')
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

    loop do
      bytes_read = LibPQ.get_copy_data(@connection, pointerof(buffer), 0)

      if bytes_read > 0
        # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA
        data = XLogData.new(buffer, bytes_read)
        message_type = data.read_char
        case message_type
        when 'w' # WAL data
          #      [0] Byte ('w')
          #   [1..8] Int64 (msg WAL start)
          #  [9..16] Int64 (WAL end)
          # [17..24] Int64 (server time)
          #   [25..] Byte* (message)
          puts("w")
          # @todo: for "received" reply, do we use msg WAL start or WAL end?
          # data.skip(sizeof(Int64))
          msg_lsn = data.read_uint64
          puts("ws", msg_lsn)
          lsn = data.read_uint64
          puts("we", lsn)
          data.skip(sizeof(Int64))
          process_message(data)
          # @todo: move this into process message when we actually send data to sink
          # @lsn.max(lsn)
          feedback(lsn)

        when 'k' # Keepalive message
          #     [0] Byte ('k')
          #  [1..8] Int64 (WAL end)
          # [9..16] Int64 (server time)
          #    [17] Byte (reply request)
          puts("k")
          # @todo: send this as "received" in reply?
          lsn = data.read_uint64
          data.skip(sizeof(Int64))
          repreq = data.read_uint8
          puts("ke", lsn, repreq)
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
    puts(data.to_bytes)
    type = data.read_char
    puts(type)
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
      puts(oid)
      puts(data.read_string)
      relation = data.read_string
      puts("relation: #{relation}")
      data.skip(sizeof(UInt8))

      columns = data.read_int16
      while columns > 0
        columns -= 1
        flags = data.read_uint8
        name = data.read_string
        coid = data.read_int32
        modifier = data.read_int32
        puts({flags, name, coid, modifier})
      end

    when 'I'
      # [0] Byte ('I')
      # [1..4] Int32 (txid) - streamed txn only
      # [5..8] Int32 (relation OID)
      # [9] Byte ('N')
      # [10..] Data tuple
      oid = data.read_int32
      puts(oid)
      data.skip(sizeof(UInt8))

      columns = data.read_int16
      while columns > 0
        columns -= 1
        value_type = data.read_char
        puts(value_type)
        case value_type
        when 'n'
          puts("null")
        when 'u'
          puts("unchanged")
        when 't'
          puts("text")
        when 'b'
          puts("binary")
        else
          puts("unknown")
        end


      end

    when 'B'
      # [0] Byte ('B')
      # [1..8] Int64 (txn final LSN)
      # [9..16] Int64 (commit timestamp)
      # [17..20] Int32 (txid)
      txn_lsn = data.read_uint64
      puts(txn_lsn)

    when 'C'
      # [0] Byte ('C')
      # [1] Int8 (flags)
      data.skip(sizeof(UInt8))
      # [2..9] Int64 (commit LSN)
      commit_lsn = data.read_uint64
      # [10..17] Int64 (txn end LSN)
      txn_lsn = data.read_uint64
      # [18..25] Int64 (commit timestamp)
      puts(data.read_int64)
      puts(commit_lsn, txn_lsn)
    end
  end

  private def feedback(lsn : UInt64)
    puts("Received LSN: #{lsn}")
    puts("Replicated LSN: #{@lsn.to_s}")

    # https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
    feedback = IO::Memory.new(34)
    feedback.write_byte('r'.ord.to_u8) # Standby status update (r)
    feedback.write_bytes(lsn, IO::ByteFormat::BigEndian) # received
    feedback.write_bytes(@lsn.value, IO::ByteFormat::BigEndian) # flushed
    feedback.write_bytes(@lsn.value, IO::ByteFormat::BigEndian) # applied
    feedback.write_bytes(PG.now, IO::ByteFormat::BigEndian)
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

config = PG::PQ::Config.new(slot: "slot_events", publication: "pub_events")

sink = Channel(JSON::Any).new
client = PG::PQ.new(config, sink)

puts(client)
puts(client.@connection)

client.start("0/0")
# client.start("0/490401608")
puts(client)

client.close()
puts(client)
puts(client.@connection)
