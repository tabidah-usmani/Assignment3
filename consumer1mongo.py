import json
from kafka import KafkaConsumer
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
import pandas as pd
from pymongo import MongoClient

bootstrap_servers = ['localhost:9092']
topic = 'recommendation_engine_topic'

products_list = []

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
count = 0

def category_tuples(item):
    converted_item = item.copy()
    converted_item['category'] = tuple(item['category'])
    converted_item['also_buy'] = tuple(item['also_buy'])
    return converted_item

def calculate_similarity(product1, product2):
    common_attributes = set(product1.items()) & set(product2.items())
    similarity = len(common_attributes) / len(set(product1.items()) | set(product2.items()))
    return similarity

def apriori_similarity(items, min_support):
    frequent_itemsets = []
    for i, item1 in enumerate(items):
        for j, item2 in enumerate(items):
            if i != j:
                similarity = calculate_similarity(item1, item2)
                if similarity >= min_support:
                    frequent_itemsets.append((item1['title'], item2['title'], similarity))
    return frequent_itemsets

min_support = 0.09

# MongoDB setup
client = MongoClient('localhost', 27017)
db = client['frequent_itemsets_db']
collection = db['frequent_itemsets_collection']

for message in consumer:
    product_info = message.value
    products_list.append(product_info)

    count += 1

    if len(products_list) == 10:
        # Convert items to tuples
        products_list = [category_tuples(item) for item in products_list]
        # Find frequent itemsets based on similarity
        frequent_itemsets = apriori_similarity(products_list, min_support)
        print("These are the frequent itemsets for the products:")
        for title1, title2, similarity in frequent_itemsets:
            print("Itemset:", title1, ',', title2)
            print("Similarity:", similarity)

        # Saving to MongoDB
        frequent_itemsets_dict = [{"item1": title1, "item2": title2, "similarity": similarity} for title1, title2, similarity in frequent_itemsets]
        collection.insert_many(frequent_itemsets_dict)

        products_list = []  
        count = 0
