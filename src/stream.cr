class Stream(T)
  def initialize
    @pending = Channel({UInt64, T}).new(100)
    @position = Atomic(UInt64).new(0_u64)
  end

  def push(position : UInt64, entry : T)
    @pending.send({ position, entry })
  end

  def take
    @pending.receive
  end

  def replicated(position : UInt64)
    @position.max(position)
  end

  def position
    @position.get
  end
end
