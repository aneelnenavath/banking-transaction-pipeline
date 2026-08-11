import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session - the entry point for any Spark program
spark = SparkSession.builder \
    .appName("BankTransactionStreamConsumer") \
    .getOrCreate()

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

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

cassandra_ready_stream = parsed_stream \
    .withColumn("transaction_time", to_timestamp(col("timestamp"))) \
    .drop("timestamp")

# Console output, for visual debugging
console_query = parsed_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()


# --- TEMPORARILY DISABLED: Cassandra sink ---
# Commented out while we isolate and debug the Snowflake sink on its own.
# Will re-enable once Snowflake is confirmed stable.
#
# def write_to_cassandra(batch_df, batch_id):
#     batch_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .mode("append") \
#         .options(table="transactions", keyspace="banking") \
#         .save()
#     print(f"Batch {batch_id} written to Cassandra.")
#
# cassandra_query = cassandra_ready_stream.writeStream \
#     .foreachBatch(write_to_cassandra) \
#     .outputMode("append") \
#     .start()


# --- TEMPORARILY DISABLED: S3 sink ---
# s3_query = parsed_stream.writeStream \
#     .format("parquet") \
#     .option("path", "s3a://anil-banking-transactions-raw/transactions/") \
#     .option("checkpointLocation", "s3a://anil-banking-transactions-raw/checkpoints/transactions/") \
#     .outputMode("append") \
#     .start()


# --- Snowflake sink (the one we're isolating and testing today) ---
snowflake_options = {
    "sfURL": "QCGJNBF-RG43938.snowflakecomputing.com",
    "sfUser": "ANILNENAVATH162",
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfRole": "BANKING_PIPELINE_ROLE",
    "sfDatabase": "BANKING_DB",
    "sfSchema": "BANKING_SCHEMA",
    "sfWarehouse": "BANKING_WH"
}

def write_to_snowflake(batch_df, batch_id):
    ordered_df = batch_df.select(
        "transaction_id", "account_id", "amount",
        "transaction_time", "merchant", "location", "transaction_type"
    )
    ordered_df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "TRANSACTIONS") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} written to Snowflake.")


snowflake_query = cassandra_ready_stream.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .trigger(processingTime="15 seconds") \
    .start()

# Wait for both remaining streaming queries to keep running
spark.streams.awaitAnyTermination()
