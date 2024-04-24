from kafka import KafkaConsumer
import json
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
import pandas as pd
from collections import deque

consumer = KafkaConsumer(
    'recommendation_engine_topic',
    bootstrap_servers='localhost:9092',
    group_id='my_consumer_group',
    auto_offset_reset='earliest'
)

#utilizing sliding window approach
window_size = 10

window_queue = deque(maxlen=window_size)

#finding jaccard similarity for each message being receivd by the producer and then identifying frequent itemsets
def find_frequent_itemsets(window_items, min_support=0.01):
    """Find frequent itemsets using the FP-Growth algorithm."""
    te = TransactionEncoder()
    te_ary = te.fit(window_items).transform(window_items)
    df = pd.DataFrame(te_ary, columns=te.columns_)
    frequent_itemsets = fpgrowth(df, min_support=min_support, use_colnames=True)
    return frequent_itemsets

def jaccard_similarity(set1, set2):
    """Calculate Jaccard Similarity between two sets."""
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

consumer.subscribe(['recommendation_engine_topic'])

for message in consumer:
    try:
        data = json.loads(message.value.decode('utf-8'))
        print("Consumer received:", data)
        
        if 'also_buy' in data and isinstance(data['also_buy'], str):
            items = set(data['also_buy'].split())
            
            window_queue.append(items)
            
            window_items = [item for window in window_queue for item in window]
            
            frequent_itemsets = find_frequent_itemsets(window_items, min_support=0.01)
            print("Frequent Itemsets Found:", frequent_itemsets)
        
        else:
            print("No 'also_buy' data available or incorrect format.")
        
    except Exception as e:
        print("Error processing message:", e)
