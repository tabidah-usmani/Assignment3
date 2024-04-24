from collections import defaultdict
from kafka import KafkaConsumer
import json
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from mlxtend.preprocessing import TransactionEncoder

class AprioriStreaming:
    def __init__(self, min_support, window_size):
        self.min_support = min_support
        self.window_size = window_size
        self.window = []
        self.transactions = []

    def process_transaction(self, transaction):
        
        self.window.append(transaction)
        
        # Remove oldest transactions if window size exceeds the specified size
        if len(self.window) > self.window_size:
            oldest_transaction = self.window.pop(0)
            self.transactions.remove(oldest_transaction)

        # Add new transaction to transactions list
        self.transactions.append(transaction)
        
        # Apply Apriori to discover frequent itemsets
        if len(self.window) == self.window_size:
            self.discover_associations()

    def discover_associations(self):
        # Extract relevant information from transactions
        also_buy_list = []
        for transaction in self.transactions:
            if 'also_buy' in transaction:
                also_buy_list.append(transaction['also_buy'])
            else:
                print("Transaction does not contain 'also_buy' field:", transaction)
        
        # Print transaction list for debugging
        print("Transaction List:", also_buy_list)
        
        # Convert transactions to list of lists format
        transaction_list = [str(also_buy).split() for also_buy in also_buy_list]
        
        # Print transaction list after conversion
        print("Transaction List after Conversion:", transaction_list)
        
        # Initialize TransactionEncoder
        te = TransactionEncoder()
        
        # Fit and transform transaction data
        te_ary = te.fit(transaction_list).transform(transaction_list)
        
        # Convert transformed data to DataFrame
        df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
        
        
        print("Encoded DataFrame:")
        print(df_encoded)
        
        # Apply Apriori algorithm
        frequent_itemsets = apriori(df_encoded, min_support=self.min_support, use_colnames=True)
        
        # Print frequent itemsets for debugging
        print("Frequent Itemsets:")
        print(frequent_itemsets)
        
        # Generate association rules
        rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1)
        
        
        print("\nAssociation Rules:")
        print(rules)

# Initialize Kafka consumer, setting the kafka topic, localhost id, and the id of consumer
consumer = KafkaConsumer('recommendation_engine_topic', bootstrap_servers='localhost:9092', group_id='my_consumer_group')

# Connect with the topic of producer
consumer.subscribe(topics=['recommendation_engine_topic'])

# Initialize AprioriStreaming with a window size and minimum support
window_size = 100
min_support = 0.3
apriori_streaming = AprioriStreaming(min_support, window_size)


for message in consumer:
    try:
        data = json.loads(message.value.decode('utf-8'))
        apriori_streaming.process_transaction(data)
    except Exception as e:
        print("Error processing message:", e)

