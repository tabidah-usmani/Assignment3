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






Here's a template for your README file on GitHub:

Kafka Consumer and Producer Example
This repository contains Python code examples for a Kafka consumer and producer. These scripts demonstrate how to implement a simple Kafka consumer and producer using the kafka-python library.

Getting Started
To use these examples, you'll need to have Kafka installed and running on your local machine. You'll also need Python installed, along with the kafka-python library.

Prerequisites
Apache Kafka
Python 3.x
kafka-python library (pip install kafka-python)
Installation
Clone this repository:
bash
Copy code
git clone https://github.com/your-username/kafka-consumer-producer.git
Install dependencies:
bash
Copy code
pip install -r requirements.txt
Usage
Producer
The kafka_producer.py script reads preprocessed data from a JSON file line by line and sends it to a Kafka topic named recommendation_engine_topic.

To run the producer:

bash
Copy code
python kafka_producer.py
Consumer
The kafka_consumer_pcy.py script consumes messages from the recommendation_engine_topic Kafka topic. It processes the data to find frequent pairs using the PCY (Park-Chen-Yu) algorithm.

To run the consumer:

bash
Copy code
python kafka_consumer_pcy.py
Configuration
You may need to adjust the Kafka broker configuration in the scripts to match your Kafka setup. Update the bootstrap_servers parameter in both the consumer and producer scripts accordingly.

Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, feel free to open an issue or submit a pull request.

License
This project is licensed under the MIT License - see the LICENSE file for details.
