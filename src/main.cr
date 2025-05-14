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

source = Fiber::ExecutionContext::Isolated.new("Postgres") do
  client = Postgres.new(config, stream)
  client.start(config.lsn)
end

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
  Log.debug { "WRITE #{lsn}" }
end
