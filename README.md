# Assignment 3

# Preprocessing

The preprocessing code is divided into several steps:
1. Clean data: Removes duplicates, based on the asin id.
2. Extract relevant info: Extract relevant fields from the JSON data, which are further needed in finding frequent itemsets mining.
3. Handle missing values: Removing data with missing fields.
4. Convert data types: Convert data types as needed, such as converting 'price' to float data type.
5. Normalize text: Normalize text by lowercasing, removing punctuation, and extra spaces.
6. Tokenize text: Tokenize texts into words
7. Handle categorical data.

This preprocessing prepares the data set for finding frequent itemsets.


# Kafka Consumer and Producer 

# Description
This repository contains Python code examples for a Kafka consumer and producer. These scripts demonstrate how to implement a simple Kafka consumer and producer using the kafka-python library.

# Features
Kafka producer sends data from a JSON file to a Kafka topic.
Kafka consumer processes messages from the Kafka topic to find frequent pairs using the PCY algorithm.


# Usage

## Producer
The kafka_producer.py script reads preprocessed data from a JSON file line by line and sends it to a Kafka topic named recommendation_engine_topic.

## Consumer
The kafka_consumer_pcy.py script consumes messages from the recommendation_engine_topic Kafka topic. It processes the data to find frequent pairs using the PCY (Park-Chen-Yu) algorithm.
## Implementation Details
The consumer script initializes a Kafka consumer to subscribe to the recommendation_engine_topic topic.
Each message consumed from Kafka represents a basket of items.
The script processes each basket using the PCY algorithm to find frequent pairs.
Frequent pairs are printed as insights or can be stored in a database for further analysis.

## Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, feel free to open an issue or submit a pull request.

Amna Javaid,
Maryam Khalid,
Tabidah Usmani
