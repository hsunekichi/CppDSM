#!/bin/bash

# IP address and starting port number
IP_ADDRESS="192.168.1.50"
START_PORT=7000

# Check args
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <num_masters>"
    exit 1
fi

# Check args > 1
if [ "$1" -le 2 ]; then
    echo "Number of masters must be greater than 2"
    exit 1
fi

# Number of master nodes
NUM_MASTERS=$1

# Function to create a Redis cluster
create_redis_cluster() {
    local port=$1
    local replica_port=$((port + 1000))

    cd ~/redis-cluster
    mkdir $port
    rm -rf $port/*
    cd $port

    # Create Redis master node
    redis-server --port $port --cluster-enabled yes --cluster-node-timeout 5000 --appendonly yes --daemonize yes --bind $IP_ADDRESS --protected-mode no > /dev/null

    cd ~/redis-cluster
}

cluster_data=""

# Create master nodes
for ((i = 0; i < NUM_MASTERS; i++)); do
    port=$((START_PORT + i))
    create_redis_cluster $port
    cluster_data="$cluster_data $IP_ADDRESS:$port"
done

sleep 1

# Initialize Redis cluster
redis-cli --cluster create $cluster_data
