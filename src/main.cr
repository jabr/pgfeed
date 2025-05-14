require "json"
require "log"

require "./config"
require "./stream"
require "./postgres"

Log.setup_from_env

config = Config.new(
  slot: "slot_events",
  publication: "pub_events",
  binary: true,
)

stream = Stream(JSON::Any).new

# Run Postgres logical replication logic in its own
# dedicated thread so it can use blocking IO.
source = Fiber::ExecutionContext::Isolated.new("Postgres") do
  client = Postgres.new(config, stream)
  client.start(config.lsn)
end

# @todo: run this in a separate (single or multithreaded?)
# execution context so that it won't block by the main loop
# position writer below or any logging output.
sink = spawn do
  loop do
    pos, entry = stream.take
    Log.info { "replicating @ #{pos} : #{entry}" }
    # @todo: send to sink
    stream.replicated(1) # @todo: send pos, not 1
  end
end

loop do
  sleep 10.seconds
  lsn = PG::LSN.format(stream.position)
  # @todo: write to position status file
  Log.debug { "WRITE #{lsn}" }
end
