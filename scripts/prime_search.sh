#!/bin/bash

primeHosts="./prime_hosts"
command=$1

# Parse prime hosts, one ip per line. Discard lines starting with #
primeHostsArray=()
while IFS= read -r line
do
    if [[ $line != \#* ]]; then
        primeHostsArray+=($line)
    fi
done < "$primeHosts"

# Ssh to each host and launch command asynchronously. Each command has as parameters rank id and total number of ranks
for i in "${!primeHostsArray[@]}"; 
do
    host=${primeHostsArray[$i]}

    scp "bin/$command" "$host:~/"
    ssh -o StrictHostKeyChecking=no $host "~/$command $i ${#primeHostsArray[@]}" &
done