#! /bin/bash

# First arg is the document path, second is the number of processes
if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters"
    echo "Usage: ./count_docs.sh <path_to_docs> <num_processes>"
    exit 1
fi

# Init the database
./bin/count_docs_dist $1 0 0 1

# Get init time in ns
INIT_TIME=$(date -u +%s.%N)

for i in $(seq 0 $2)
do
    # Process docs
    ./bin/count_docs_dist $1 $i $2 0 &
done


# Wait all processes to finish
wait

# Get end time
END_TIME=$(date -u +%s.%N)

elapsed="$(bc <<<"$END_TIME-$INIT_TIME")"

# Compute ms.us
elapsed="$(bc <<<"scale=3; $elapsed*1000")"

# Print total time
echo "Total time: $elapsed ms"