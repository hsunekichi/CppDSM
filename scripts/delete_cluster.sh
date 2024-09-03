#!/bin/bash

# IP address and starting port number
IP_ADDRESS="127.0.0.1"
START_PORT=7000

# Check args
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <num_masters>"
    exit 1
fi

# Number of master nodes
NUM_MASTERS=$1

# Function to stop and remove Redis instances
delete_redis_cluster() {
    local port=$1

    # Stop Redis instance
    redis-cli -h $IP_ADDRESS -p $port shutdown

    # Remove data directory
    rm -rf ~/redis-cluster/$port
}

# Delete Redis master nodes
for ((i = 0; i < NUM_MASTERS; i++)); do
    port=$((START_PORT + i))
    delete_redis_cluster $port

    echo "Deleted Redis master node on port $port"
done

