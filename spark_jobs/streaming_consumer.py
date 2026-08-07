from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session - the entry point for any Spark program
spark = SparkSession.builder \
    .appName("BankTransactionStreamConsumer") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# Reduce log noise so we can actually see our own output clearly
spark.sparkContext.setLogLevel("WARN")

# Define the exact structure of our transaction JSON
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("location", StringType(), True),
    StructField("transaction_type", StringType(), True)
])

print("Spark session started. Schema defined. Connecting to Kafka...")

# Read the raw stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the raw JSON bytes into structured columns
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

# Cassandra needs a real TIMESTAMP column named transaction_time
cassandra_ready_stream = parsed_stream \
    .withColumn("transaction_time", to_timestamp(col("timestamp"))) \
    .drop("timestamp")

# Console output, for visual debugging
console_query = parsed_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()


# --- Cassandra sink (needs foreachBatch, not a native streaming sink) ---
def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="transactions", keyspace="banking") \
        .save()
    print(f"Batch {batch_id} written to Cassandra.")


cassandra_query = cassandra_ready_stream.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .start()


# --- S3 sink ---
# Unlike Cassandra, S3 is a natively supported streaming sink (it's
# file-based), so we can write directly with .format("parquet") instead
# of needing foreachBatch. Structured Streaming requires a checkpoint
# location for every streaming write, so it can track exactly which
# data has already been written if the job restarts.
s3_query = parsed_stream.writeStream \
    .format("parquet") \
    .option("path", "s3a://anil-banking-transactions-raw/transactions/") \
    .option("checkpointLocation", "s3a://anil-banking-transactions-raw/checkpoints/transactions/") \
    .outputMode("append") \
    .start()

# Wait for all three streaming queries to keep running
spark.streams.awaitAnyTermination()
