from kafka import KafkaConsumer
import json
import hashlib
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  

# kafka consumer setup
consumer = KafkaConsumer(
    'recommendation_engine_topic',  # kafka topic to subscribe to
    bootstrap_servers=['localhost:9092'],  # list of kafka broker addresses
    auto_offset_reset='earliest', 
    enable_auto_commit=True,  
    group_id='my_consumer_group',  # consumer group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else {}  
)

# parameters for PCY
support_threshold = 1000  # example threshold for frequent itemset
bucket_size = 10000       # size of the hash table for hash-based counting

# hash function implementation
def hash_pair(pair):
    #hash of a pair of items 
    return int(hashlib.sha256(json.dumps(pair, sort_keys=True).encode()).hexdigest(), 16) % bucket_size

# streaming data processing
for message in consumer:
    logger.info("Received message: %s", message)
    
    if not message.value:
        logger.warning("Received empty message, skipping...") 
        continue
    
    basket = message.value.get('products', []) 
    
    # initialize count structures for each message
    item_counts = {}
    bucket_counts = [0] * bucket_size
    
    # single pass to count individual items and hash pairs
    for i in range(len(basket)):
        item = basket[i]
        item_counts[item] = item_counts.get(item, 0) + 1
        
        for j in range(i+1, len(basket)):
            pair = tuple(sorted((item, basket[j])))  # sort to ensure consistent pair order
            bucket_index = hash_pair(pair)
            bucket_counts[bucket_index] += 1
    
    # create bitmap from bucket counts
    bitmap = [1 if count >= support_threshold else 0 for count in bucket_counts]
    
    # second pass: use bitmap to find frequent pairs
    frequent_pairs = {}
    for i in range(len(basket)):
        for j in range(i+1, len(basket)):
            pair = tuple(sorted((basket[i], basket[j])))
            bucket_index = hash_pair(pair)
            if bitmap[bucket_index]:
                frequent_pairs[pair] = frequent_pairs.get(pair, 0) + 1
    
    #store results in a database
    frequent_pairs = {k: v for k, v in frequent_pairs.items() if v >= support_threshold}
    logger.info("Current Frequent Pairs: %s", frequent_pairs)

# close the consumer
consumer.close()
