#!/bin/bash

# Check args
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <ip> <nNodes>"
    exit 1
fi

ip=$1
nNodes=$2

endPort=$((nNodes - 1))

# Send to all ports from 7000 to 7000 + nNodes - 1
for i in $(seq 0 $endPort); do
    port=$((7000 + i))
    redis-cli -h $ip -p $port flushall
done
