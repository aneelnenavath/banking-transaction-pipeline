from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session - the entry point for any Spark program
spark = SparkSession.builder \
    .appName("BankTransactionStreamConsumer") \
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

# Write the parsed, structured data to the console so we can see it clearly
query = parsed_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
