from kafka import KafkaProducer
import json
import time

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Function to send data to Kafka
def send_data(topic, data):
    producer.send(topic, json.dumps(data).encode('utf-8'))

# Read and parse preprocessed data from file line by line
with open('/home/tabidah/kafka/assignment/preprocessed_data.json', 'r') as file:
    for line in file:
        try:
            data = json.loads(line.strip())  # Strip any leading/trailing whitespace
            send_data("recommendation_engine_topic", data)
            print("Sent data to Kafka:", data)  # Print sent data for debugging
            time.sleep(1)  # Simulate real-time streaming with a 1-second delay
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
        except Exception as e:
            print(f"Error sending data to Kafka: {e}")

# Close the Kafka producer
producer.close()

