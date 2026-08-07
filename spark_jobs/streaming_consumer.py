from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session - the entry point for any Spark program
# We also set the Cassandra connection host here, so the Cassandra
# connector knows which cluster to talk to when we write to it later
spark = SparkSession.builder \
    .appName("BankTransactionStreamConsumer") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# Reduce log noise so we can actually see our own output clearly
spark.sparkContext.setLogLevel("WARN")

# Define the exact structure of our transaction JSON
# This tells Spark what fields to expect and what type each one is
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

# Kafka gives us raw bytes in the 'value' column.
# Step 1: cast those bytes to a string (so it's readable text/JSON)
# Step 2: parse that JSON string into actual structured columns using our schema
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

# Cassandra's transactions table expects a proper TIMESTAMP column called
# transaction_time, but Kafka is giving us a plain string column called
# timestamp. We rename and cast it here, right before writing to Cassandra,
# without touching the original parsed_stream used for the console output.
cassandra_ready_stream = parsed_stream \
    .withColumn("transaction_time", to_timestamp(col("timestamp"))) \
    .drop("timestamp")

# Write the parsed, structured data to the console so we can see it clearly
console_query = parsed_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()


# Structured Streaming doesn't support writing directly to Cassandra with
# .format(...) the way it does for console/kafka/file sinks. Instead we use
# foreachBatch: Spark hands us each micro-batch as a normal (non-streaming)
# DataFrame, and we write ordinary batch-write code inside this function.
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

# Wait for both streaming queries to keep running
spark.streams.awaitAnyTermination()
