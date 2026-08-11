# Banking Transaction Pipeline — Runbook

Operational notes for running this project locally in WSL2 Ubuntu.
Written after a real memory-constrained-environment incident (see
"Known Issue" section below) — these exact settings are required for
a stable run on an 8GB RAM machine.

## Environment

- WSL2 Ubuntu, capped via `.wslconfig` (see below)
- Java 17 (`JAVA_HOME` set permanently in `~/.bashrc`)
- Kafka 3.7.2 + Zookeeper — `~/bigdata/kafka`
- Spark 3.5.5 (Hadoop3 build) — `~/bigdata/spark`
- Cassandra 5.0.8 — Docker container `cassandra-banking`, port 9042

## `.wslconfig` (Windows side)

Location: `C:\Users\<username>\.wslconfig`

```ini
[wsl2]
memory=4.5GB
processors=2
swap=2GB
```

This caps WSL2's resource usage so it doesn't try to compete with
Windows itself for all available RAM. See "Known Issue" below for why
this was necessary.

## Startup sequence (run in this order, every fresh WSL session)

Kafka's topic data lives under `/tmp/kafka-logs`, and Cassandra's
container state does NOT survive a `wsl --shutdown` restart of the
WSL VM itself (only a plain `docker stop`/`start` preserves data) —
so after any WSL restart, the topic needs recreating.

**1. Start Zookeeper** (background, survives closing the terminal):
```bash
cd ~/bigdata/kafka
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/bigdata/zookeeper.log 2>&1 &
```

**2. Start Kafka:**
```bash
nohup bin/kafka-server-start.sh config/server.properties > ~/bigdata/kafka.log 2>&1 &
```

**3. Recreate the topic:**
```bash
bin/kafka-topics.sh --create --topic bank-transactions \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**4. Start Cassandra:**
```bash
docker start cassandra-banking
```
Wait 20-30 seconds before connecting — Cassandra's JVM startup is slow.

**5. Start the producer** (separate terminal tab):
```bash
cd ~/projects/banking-transaction-pipeline
python3 producer/transaction_generator.py
```

**6. Start the Spark streaming consumer** (separate terminal tab):
```bash
cd ~/projects/banking-transaction-pipeline

spark-submit \
  --master "local[1]" \
  --driver-memory 1g \
  --executor-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  spark_jobs/streaming_consumer.py
```

## Verifying data landed in Cassandra

```bash
docker exec -it cassandra-banking cqlsh
```
```sql
USE banking;
SELECT * FROM transactions LIMIT 10;
```

## Known Issue: WSL2 resource exhaustion under multi-service load (Aug 2026)

**Symptom:** Running Zookeeper + Kafka + Spark (with `local[*]` default
core usage and no memory limits) + Cassandra simultaneously caused:
- New WSL terminal tabs failing to open at all
- Existing `wsl` connections failing with `Error code: Wsl/Service/0x8007274c`
- Spark's own internal heartbeat/RPC layer timing out
  (`WARN NettyRpcEnv: Ignored failure: java.util.concurrent.TimeoutException`)
- The streaming job stalling entirely (batches stopped incrementing,
  not just slowing down)

**Root cause:** the dev machine has 8GB total RAM. Running four
separate JVM processes (Zookeeper, Kafka, Spark driver+executor,
Cassandra) simultaneously, with Spark defaulting to `local[*]`
(all CPU cores) and no explicit memory caps, exhausted available
memory — confirmed via Task Manager showing 89% system memory usage
and WSL's own VM process (`VmmemWSL`) pegged at high, sustained CPU.

**Diagnosis steps taken:**
1. Confirmed producer was still sending (proving Kafka was healthy)
2. Confirmed Spark's terminal had stopped incrementing batch counts
   (not just slowed — genuinely stalled)
3. Checked Task Manager memory/CPU for `VmmemWSL`
4. Attempted `wsl --shutdown` / reconnect — service itself was hung
   and needed a forced reset

**Fix applied:**
1. Added `.wslconfig` capping WSL2 to 4.5GB RAM / 2 processors / 2GB swap,
   preventing WSL from over-committing against Windows itself
2. Changed `spark-submit` from default `local[*]` to explicit
   `--master "local[1]" --driver-memory 1g --executor-memory 1g`,
   preventing Spark from claiming all available cores/memory
3. Closed unnecessary background apps (browser, chat apps) to free
   additional headroom before running the pipeline

## Snowflake sink debugging (11 Aug 2026)

**Bug:** Snowflake connector writes positionally, not by column name.
`transaction_time` was appended last via `withColumn()`, but the
Snowflake table has it as the 4th column — caused merchant values
to be written into the timestamp column, failing with:
`SQLException: Timestamp 'Sainsburys' is not recognized`

**Fix:** explicitly reorder columns via `.select()` in
`write_to_snowflake` before writing, matching the target table's
column order exactly.

## Cassandra memory optimization (11 Aug 2026)

Cassandra's default JVM heap was consuming ~2.25GB (64% of the
3.5GB WSL2 memory cap) at idle — leaving too little headroom for
Spark. Recreated the container with explicit heap limits:

    docker stop cassandra-banking
    docker rm cassandra-banking
    docker run -d \
      --name cassandra-banking \
      -p 9042:9042 \
      -v 44a5212dd4f93be97201353569a237d4302d3955def7aadb4583af9772275263:/var/lib/cassandra \
      -e MAX_HEAP_SIZE=512M \
      -e HEAP_NEWSIZE=100M \
      cassandra:latest

Result: memory usage dropped to ~870MB. Existing data preserved
via the named volume (do NOT delete the volume when recreating
the container).

## Full spark-submit command (all 4 sinks: console, Cassandra, S3, Snowflake)

    spark-submit \
      --master local[1] \
      --driver-memory 1g \
      --executor-memory 1g \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.30 \
      spark_jobs/streaming_consumer.py > ~/spark_run.log 2>&1

**IMPORTANT:** the following environment variables MUST be exported
in the SAME terminal session, before running spark-submit above —
they do not persist across terminal windows/tabs:

    export AWS_ACCESS_KEY_ID=$(grep aws_access_key_id ~/.aws/credentials | cut -d'=' -f2 | tr -d ' ')
    export AWS_SECRET_ACCESS_KEY=$(grep aws_secret_access_key ~/.aws/credentials | cut -d'=' -f2 | tr -d ' ')
    export SNOWFLAKE_USER=<your_username>
    export SNOWFLAKE_PASSWORD=<your_password>

Verify all four are set (non-zero) before running:

    echo ${#AWS_ACCESS_KEY_ID}
    echo ${#AWS_SECRET_ACCESS_KEY}
    echo ${#SNOWFLAKE_USER}
    echo ${#SNOWFLAKE_PASSWORD}

## Known open issue: memory growth over long runs

**Update (11 Aug 2026, later session):** found that Cassandra and S3
sinks had NO trigger set at all (only Snowflake had
`.trigger(processingTime="15 seconds")`), meaning they were firing
continuously back-to-back. Added matching 15s triggers to all three
foreachBatch/native sinks.

**Result:** memory decline was noticeably slower with all three
sinks throttled (e.g. dropped ~460Mi -> ~422Mi available over 5
batches, vs. dropping to ~36Mi over ~21 batches previously) - but
still trends downward over time, not a complete fix.

**Conclusion:** unthrottled sinks were a significant contributor,
but not the sole cause. Remaining candidates: GC lag under tight
1g driver/executor heap limits, broadcast variable/state not being
released between foreachBatch calls, or checkpoint metadata growth.

**Next steps to try:**
- Increase trigger interval further (e.g. 30s) to isolate whether
  remaining growth is purely frequency-related
- Reduce Kafka producer throughput as an alternative lever
- Investigate explicit cleanup/unpersist calls within foreachBatch
  functions
- Alternatively: document a known stable operating window (e.g.
  "runs cleanly for N batches / M minutes before intervention
  needed") as an acceptable, honest conclusion for a portfolio
  project rather than chasing full elimination
