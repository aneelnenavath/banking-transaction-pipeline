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

## 2026-08-13 — Session findings: Kafka checkpoint bug, memory plateau, EC2↔WSL2 tunnel

### 1. OffsetOutOfRangeException — root cause and fix

**Symptom:** Spark streaming consumer threw `OffsetOutOfRangeException` on startup.

**Root cause:** The Kafka topic `bank-transactions` was recreated mid-session. Kafka's
on-disk data lives in `/tmp/kafka-logs`, which does not survive a WSL2 restart — so a
freshly recreated topic starts at offset 0. But the S3 sink's Spark Structured Streaming
checkpoint (`s3a://anil-banking-transactions-raw/checkpoints/transactions/`) still
referenced the old offsets from before the topic was recreated. Spark refused to reconcile
the mismatch.

**Fix:**
```bash
aws s3 rm --recursive s3a://anil-banking-transactions-raw/checkpoints/transactions/
```
Then restart the consumer so it re-initializes the checkpoint against the current topic.

**Takeaway / rule of thumb:** any time the Kafka topic is recreated (deliberately or as a
side effect of a WSL2 restart wiping `/tmp/kafka-logs`), all Spark checkpoint directories
tied to that topic must be cleared before the next run, or every sink reading from that
topic will fail with the same class of error.

**Also cleaned up:** a stale comment in `spark_jobs/streaming_consumer.py`
(`# --- TEMPORARILY DISABLED: S3 sink ---`) that no longer matched the code — the S3 sink
was actually active. Comment removed to avoid future confusion.

### 2. S3 sink "zero input rows" — resolved, not a real bug

Spent time diagnosing why the S3 streaming query showed 0 input rows in the Spark UI
(`localhost:4040/StreamingQuery/`) while Cassandra and Snowflake sinks worked fine.
Checked Kafka consumer groups, offset files, and query definitions — found no structural
issue in the code. Conclusion: this was leftover confusion from repeated crash/restart
cycles (stale checkpoints, partial batches accumulating across restarts), not a genuine
bug in the S3 sink logic.

**Confirmed resolved:** on a fully fresh restart (topic recreated, checkpoint cleared,
Zookeeper/Kafka/Cassandra all restarted clean), S3 wrote real Parquet files successfully
alongside Cassandra and Snowflake. No code change was needed for this one — it was an
environment/state issue, not a logic bug.

### 3. Consumer memory behaviour — new evidence

Set up a background monitor logging `free -m` every 15s to `~/memory_watch.log` to get
real data on the repeated `Py4JNetworkException` crashes (previously occurring roughly
every 2.5–30 minutes, inconsistently).

**Finding — warm-up then plateau:** on a clean run, available memory drops sharply for
the first ~3.5 minutes (782MB → ~610MB, roughly 50MB/min decline), then plateaus and
stabilizes in the 625–645MB range for at least several more minutes of observation.

**Working conclusion:** there is a known, stable operating window after an initial
warm-up period. This should be treated as documented, expected behaviour rather than an
open bug, pending longer-duration observation.

**Finding — backlog correlates with crash speed:** restarting the consumer after the
producer had been running unattended for a while (so the consumer has a large backlog to
catch up on via `startingOffsets: earliest`) caused a much faster crash (~2.5 minutes)
than a normal clean-backlog restart. Larger backlog at startup appears to correlate with
faster time-to-crash — likely because catch-up processing front-loads memory pressure
into the same window that's already tightest (the first ~3.5 minutes before the plateau).

**Open follow-up:** not yet root-caused to a specific line of code or config; this is
observational evidence to guide future tuning (e.g. Spark executor memory settings, or
throttling catch-up batch size after long producer-only periods).

### 4. Reverse SSH tunnel — EC2 → WSL2 orchestration link (confirmed working)

**Why:** Airflow runs on EC2 (`banking-pipeline-airflow`, eu-west-2). WSL2 has no public
IP, so EC2 can't initiate a connection into it directly. A reverse SSH tunnel lets WSL2
initiate an outbound connection to EC2 and hold it open, so EC2 can then connect back
into WSL2 through that tunnel.

**Setup:**
1. Installed and started OpenSSH server on WSL2 (`sudo apt install openssh-server -y`,
   `sudo service ssh start`).
2. Generated a dedicated ed25519 key pair on WSL2 for this purpose (no passphrase):
   `~/.ssh/ec2_to_wsl2_key` (private) and `~/.ssh/ec2_to_wsl2_key.pub` (public).
3. Added the public key to WSL2's own `~/.ssh/authorized_keys` (this is the key EC2 will
   present when it connects back in).
4. Copied the private key to EC2 at `~/.ssh/ec2_to_wsl2_key`, permissions `600`.
5. From WSL2, opened the reverse tunnel:
```bash
   ssh -i ~/banking-pipeline-key.pem -R 2222:localhost:22 -N -f ubuntu@<EC2_PUBLIC_IP>
```
   This forwards EC2's local port 2222 back to WSL2's port 22 (the `-N -f` flags mean
   "no remote command, run in background").
6. Verified on EC2 that the tunnel was listening:
```bash
   sudo ss -tlnp | grep 2222
```
   confirmed `127.0.0.1:2222` in `LISTEN` state owned by `sshd`.
7. Verified the full round trip from EC2 back into WSL2:
```bash
   ssh -i ~/.ssh/ec2_to_wsl2_key -p 2222 anil_rathod@localhost
```
   This landed in an actual WSL2 shell (prompt `anil_rathod@ANIL`, WSL2 kernel banner) —
   confirmed the reverse tunnel works end-to-end.

**Caveat:** EC2's public IP changes on stop/start (no Elastic IP attached), so the
`-R` tunnel command's target IP needs re-checking each time the instance is restarted.
The tunnel itself (`ssh -R ... -N -f`) also needs to be re-established each time WSL2
restarts, since it's a live process, not a persistent service — consider a `systemd`
unit or `autossh` for this later if orchestration becomes a daily thing.

**Next:** build a basic Airflow DAG on EC2 that uses this tunnel (via `SSHOperator` or
a bash `ssh -p 2222 ...` call) to start/stop pipeline components on WSL2.
