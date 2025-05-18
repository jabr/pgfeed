# pgfeed

This utility reads a standard PostgreSQL `pgoutput` logical replication slot (in text or binary mode) and sends INSERT records to the configured upstream sink.

## Supported sinks
* stdout
* AMQP (todo)

## LSN save point

The replicated LSN position is periodically written to file (e.g. `replicated.lsn`) and used as the starting point sent to Postgres on subsequent starts.

## Architecture

* PostgreSQL replication runs in an isolated thread, using the `libpq` library with blocking I/O.
* Writes to upstream sink use an asynchronous I/O runtime in its own, dedicated thread.
* Logging output and periodic replicated LSN position checkpoint writes occur in a third (parent) thread.

### Crystal

This currently requires the `-Dpreview_mt -Dexecution_context` options to enable execution contexts in [Crystal](https://crystal-lang.org/).

## Replication setup in Postgres

```
-- Enable logical replication in postgresql.conf:
-- wal_level = logical
-- max_replication_slots = 10
-- max_wal_senders = 10

-- Create a replication role
CREATE ROLE repuser WITH REPLICATION LOGIN PASSWORD 'reppass';

-- Create a publication
CREATE PUBLICATION my_pub FOR ALL TABLES;

-- Create a logical replication slot
SELECT * FROM pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

## References
- [PostgreSQL Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html)
- [PostgreSQL Logical Replication Message Formats](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html)

## License

This project is licensed under the terms of the [MIT license](LICENSE).
