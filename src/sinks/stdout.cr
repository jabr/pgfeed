class Sink::Stdout
  def initialize(@stream : Stream(Record))
  end

  def start
    loop do
      position, record = @stream.take
      STDOUT.puts(record.to_json)
      Log.info { "replicated to sink @ #{position}"}
      @stream.replicated(position)
    end
  end
end
