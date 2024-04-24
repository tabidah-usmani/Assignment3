#!/bin/bash

# Start Zookeeper server
echo "Starting Zookeeper server..."
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Start Kafka server
echo "Starting Kafka server..."
bin/kafka-server-start.sh config/server.properties &

# Wait for servers to start up
sleep 5

# Run Python producers and consumers
echo "Running Python producers and consumers..."
python3 producer.py &
python3 consumer1.py &
: 'python3 consumer2.py &
python3 consumer3.py &'

