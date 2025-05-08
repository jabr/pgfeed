require "./lib_pq"

class PQ
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

  def initialize(config : Config)
    @slot = config.slot
    @publication = config.publication

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
    start_command = "
      START_REPLICATION SLOT #{@slot} LOGICAL #{lsn}
      (proto_version '2', publication_names '#{@publication}')
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
        # @todo
        puts(buffer)
        data = Slice.new(buffer.as(Pointer(UInt8)), bytes_read)
        type = data[0].chr
        puts(type)
        lsn = lsn_to_string(IO::ByteFormat::BigEndian.decode(UInt64, data[1..8]))
        puts(lsn)

        feedback(lsn)
        LibPQ.freemem(buffer)

      elsif bytes_read < 0
        error_msg = String.new(LibPQ.error_message(@connection))
        raise "Error reading from replication stream: #{error_msg}"
      end
    end
  end

  private def lsn_to_string(lsn : UInt64) : String
    "#{lsn >> 32}/#{lsn & 0xFFFFFFFF}"
  end

  private def feedback(lsn : String)
    # @todo:
    # r, wal+1, wal+1, wal+1, time in pg epoch, 0
    result = LibPQ.put_copy_data(
      @connection,
      lsn.to_unsafe.as(LibC::Char*),
      lsn.size
    )
    puts("feedback", result)
    if result != 1
      error_msg = String.new(LibPQ.error_message(@connection))
      raise "Failed to send feedback: #{error_msg}"
    end

    # puts("end")
    # result = LibPQ.put_copy_end(@connection, Pointer(LibC::Char).null)
    # if result != 1
    #   error_msg = String.new(LibPQ.error_message(@connection))
    #   raise "Failed to flush feedback: #{error_msg}"
    # end
  end
end

config = PQ::Config.new(slot: "slot_events", publication: "pub_events")

client = PQ.new(config)

puts(client)
puts(client.@connection)

client.start("0/0")
# client.start("0/490401608")
puts(client)

client.close()
puts(client)
puts(client.@connection)
