@[Link("pq")]
lib LibPQ
  type PGconn = Void
  type PGresult = Void*

  enum ConnStatusType
    CONNECTION_OK = 0
  end

  enum ExecStatusType
    PGRES_COPY_BOTH = 8
  end

  fun connect = PQconnectdb(conninfo : LibC::Char*) : PGconn*
  fun finish = PQfinish(conn : PGconn*) : Void
  fun status = PQstatus(conn : PGconn*) : ConnStatusType
  fun clear = PQclear(res : PGresult) : Void
  fun error_message = PQerrorMessage(conn : PGconn*) : LibC::Char*
  fun exec = PQexec(conn : PGconn*, query : LibC::Char*) : PGresult
  fun result_status = PQresultStatus(res : PGresult) : ExecStatusType
  fun result_error_message = PQresultErrorMessage(
    res : PGresult
  ) : LibC::Char*

  fun get_copy_data = PQgetCopyData(
    conn : PGconn*,
    buffer : LibC::Char**,
    async : LibC::Int
  ) : LibC::Int
  fun freemem = PQfreemem(ptr : Void*) : Void

  fun put_copy_data = PQputCopyData(
    conn : PGconn*,
    buffer : LibC::Char*,
    nbytes : LibC::Int
  ) : LibC::Int
  fun flush = PQflush(conn : PGconn*) : LibC::Int

  fun get_result = PQgetResult(conn : PGconn*) : PGresult
  fun is_busy = PQisBusy(conn : PGconn*) : LibC::Int
  fun consume_input = PQconsumeInput(conn : PGconn*) : LibC::Int
end
