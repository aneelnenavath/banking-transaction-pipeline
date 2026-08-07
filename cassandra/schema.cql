-- Banking Transaction Pipeline — Cassandra Schema
-- Keyspace + table used as a streaming sink for parsed Kafka transactions

CREATE KEYSPACE IF NOT EXISTS banking
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE banking;

CREATE TABLE IF NOT EXISTS transactions (
    account_id        TEXT,
    transaction_time  TIMESTAMP,
    transaction_id    TEXT,
    amount            DECIMAL,
    location          TEXT,
    merchant          TEXT,
    transaction_type  TEXT,
    PRIMARY KEY (account_id, transaction_time, transaction_id)
) WITH CLUSTERING ORDER BY (transaction_time DESC, transaction_id ASC);
