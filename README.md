# pgfeed

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
