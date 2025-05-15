struct Config
  getter host, port, user, password, database
  getter slot, publication
  getter binary, replicated_file

  def initialize(
    *,
    @host = "",
    @port = "",
    @user = "",
    @password = "",
    @database = "",
    @slot : String,
    @publication : String,
    @binary = false,
    @replicated_file = "replicated.lsn"
  )
  end

  def lsn
    return File.read(replicated_file).chomp if File.exists?(replicated_file)
    return "0/0"
  rescue
    return "0/0"
  end
end
