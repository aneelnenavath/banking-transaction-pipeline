from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import uuid

# Initialize Faker - this generates realistic fake data
fake = Faker()

# Connect to our Kafka broker as a producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Connected to Kafka. Starting to generate transactions...")
print("Press Ctrl+C to stop.\n")

# Lists to randomly choose from - makes our fake data feel more realistic
merchants = ['Tesco', 'Amazon UK', 'Sainsburys', 'Shell', 'Costa Coffee', 'ASOS', 'Uber', 'Netflix', 'Boots', 'Argos']
locations = ['London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow', 'Liverpool', 'Bristol', 'Edinburgh']
transaction_types = ['purchase', 'withdrawal', 'transfer', 'refund']

def generate_transaction():
    """Creates one fake bank transaction as a Python dictionary."""
    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'account_id': f"ACC{random.randint(10000, 99999)}",
        'amount': round(random.uniform(1.00, 2000.00), 2),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'merchant': random.choice(merchants),
        'location': random.choice(locations),
        'transaction_type': random.choice(transaction_types)
    }
    return transaction

# Main loop - keep generating and sending transactions
try:
    while True:
        txn = generate_transaction()
        producer.send('bank-transactions', value=txn)
        print(f"Sent: {txn}")
        time.sleep(2)
except KeyboardInterrupt:
    print("\nStopped by user.")
    producer.close()
