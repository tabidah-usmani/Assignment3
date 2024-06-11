from collections import defaultdict
from itertools import combinations
from sklearn.metrics.pairwise import cosine_similarity
from kafka import KafkaConsumer
import json
from pymongo import MongoClient

bootstrap_servers = ['localhost:9092']
topic = 'recommendation_engine_topic'

products_list = []

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


def similarity1(categories1, categories2):
    categories1_set = set(categories1)
    categories2_set = set(categories2)
    intersection = len(categories1_set.intersection(categories2_set))
    union = len(categories1_set.union(categories2_set))
    return intersection / union if union != 0 else 0

def hash_function(item1, item2, table_size):
    # Use a combination of titles and categories to generate hash
    title_hash = hash(item1["title"] + item2["title"])
    category_hash = hash(tuple(item1["category"] + item2["category"]))
    return (title_hash + category_hash) % table_size


def bitmap_show(dataset, hash_table_size, support_threshold):
    bitmap = [0] * hash_table_size
    hash_table = defaultdict(int)

    for i in range(len(dataset)):
        for j in range(i + 1, len(dataset)):
            hash_val = hash_function(dataset[i], dataset[j], hash_table_size)
            hash_table[hash_val] += 1

    for hash_val, count in hash_table.items():
        if count >= support_threshold:
            bitmap[hash_val] = 1

    return bitmap

def pruning(dataset, bitmap, hash_table_size, support_threshold):
    candidate_pairs = defaultdict(int)

    for i in range(len(dataset)):
        for j in range(i + 1, len(dataset)):
            hash_val = hash_function(dataset[i], dataset[j], hash_table_size)
            if bitmap[hash_val]:
                similarity = similarity1(dataset[i]["category"], dataset[j]["category"])
                candidate_pairs[(dataset[i]["title"], dataset[j]["title"])] = similarity

    frequent_pairs = [(pair, similarity) for pair, similarity in candidate_pairs.items() if similarity >= support_threshold]

    return frequent_pairs


def count_itemsets(dataset, frequent_pairs):
    itemsets = defaultdict(int)

    for item in dataset:
        itemsets[(item["title"],)] += 1

    for pair, similarity in frequent_pairs:
        itemsets[pair] = similarity

    return itemsets

# MongoDB setup
client = MongoClient('localhost', 27017)
db = client['frequent_itemsets_pcy']
collection = db['frequent_itemsets_collection2']

#applying pcy algorithm
hashing = 1000  
threshold = 0.6  

for message in consumer:
    product_info = message.value
    products_list.append(product_info)

    if len(products_list) == 10:
        bitmap = bitmap_show(products_list, hashing, threshold)
        frequent_pairs = pruning(products_list, bitmap, hashing, threshold)
        itemsets = count_itemsets(products_list, frequent_pairs)

        print("frequent item pairs and their similarities")
        for itemset, similarity in itemsets.items():
            if len(itemset) > 1:
                print(itemset, ":", similarity)
                print('\n')

        # Saving to MongoDB
        frequent_itemsets_dict = [{"itemset": list(itemset), "similarity": similarity} for itemset, similarity in itemsets.items() if len(itemset) > 1]
        collection.insert_many(frequent_itemsets_dict)

        products_list = []  
