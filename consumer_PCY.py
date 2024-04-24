from kafka import KafkaConsumer
import json
import hashlib
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(_name_)

# Kafka consumer setup
consumer = KafkaConsumer(
    'recommendation_engine_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else {}  # Deserialize JSON messages
)

# Parameters for PCY
support_threshold = 1000  # Example threshold
bucket_size = 10000       # Size of the hash table

# Hash function implementation
def hash_pair(pair):
    return hashlib.sha256(json.dumps(pair, sort_keys=True).encode()).hexdigest() % bucket_size

# Streaming data processing
for message in consumer:
    logger.info("Received message: %s", message)
    
    if not message.value:
        logger.warning("Received empty message, skipping...")
        continue
    
    basket = message.value.get('products', [])
    
    # Initialize count structures for each message
    item_counts = {}
    bucket_counts = [0] * bucket_size
    
    # Single pass to count individual items and hash pairs
    for i in range(len(basket)):
        item = basket[i]
        item_counts[item] = item_counts.get(item, 0) + 1
        
        for j in range(i+1, len(basket)):
            pair = tuple(sorted((item, basket[j])))
            bucket_index = hash_pair(pair)
            bucket_counts[bucket_index] += 1
    
    # Create bitmap
    bitmap = [1 if count >= support_threshold else 0 for count in bucket_counts]
    
    # Second pass: use bitmap to find frequent pairs
    frequent_pairs = {}
    for i in range(len(basket)):
        for j in range(i+1, len(basket)):
            pair = tuple(sorted((basket[i], basket[j])))
            bucket_index = hash_pair(pair)
            if bitmap[bucket_index]:
                frequent_pairs[pair] = frequent_pairs.get(pair, 0) + 1
    
    # Print insights or store results in a database
    frequent_pairs = {k: v for k, v in frequent_pairs.items() if v >= support_threshold}
    logger.info("Current Frequent Pairs: %s", frequent_pairs)

# Close the consumer
consumer.close()