struct Config
  getter host, port, user, password, database
  getter slot, publication
  getter lsn, binary

  def initialize(
    *,
    @host = "",
    @port = "",
    @user = "",
    @password = "",
    @database = "",
    @slot : String,
    @publication : String,
    @lsn = "0/0",
    @binary = false,
  )
  end
end
