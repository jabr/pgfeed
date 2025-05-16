require "json"
require "log"

require "./config"
require "./stream"
require "./postgres"
require "./sinks/stdout"

Log.setup_from_env(backend: Log::IOBackend.new(STDERR))

config = Config.new(
  slot: "slot_events",
  publication: "pub_events",
  binary: true,
)

alias Record = Hash(String, JSON::Any::Type)
stream = Stream(Record).new(config.lsn)
previous_position = stream.position

# Run Postgres logical replication logic in its own
# dedicated thread so it can use blocking IO.
source = Fiber::ExecutionContext::Isolated.new("Postgres") do
  Postgres.new(config, stream).start
end

# Run sink in its own thread so it won't be blocked by the
# main loop position writer below or any logging output.
sink = Fiber::ExecutionContext::SingleThreaded.new("Sink:Stdout").spawn do
  Sink::Stdout.new(stream).start
end

loop do
  sleep 1.seconds
  position = stream.position
  if position != previous_position
    Log.info { "writing replication position: #{position} (previous #{previous_position})" }
    File.write(config.replicated_file, PG::LSN.format(position))
    previous_position = position
  end
end
