# Assignment3

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


# Kafka Consumer and Producer Example

# Description
This repository contains Python code examples for a Kafka consumer and producer. These scripts demonstrate how to implement a simple Kafka consumer and producer using the kafka-python library.

# Features
Kafka producer sends data from a JSON file to a Kafka topic.
Kafka consumer processes messages from the Kafka topic to find frequent pairs using the PCY algorithm.
