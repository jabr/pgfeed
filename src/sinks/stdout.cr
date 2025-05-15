class Sink::Stdout
  def initialize(@stream : Stream(Record))
  end

  def start
    loop do
      position, record = @stream.take
      STDOUT.puts(record.to_json)
      @stream.replicated(position)
    end
  end
end
