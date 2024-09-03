#!/bin/bash

# Get all args except exec name

nWorkers=$1

shift

args="${@}"


#mpirun --map-by node:pe=1 -n $nWorkers --hostfile "./mpi_hosts_clinic" $args
mpirun --map-by node --rank-by slot -n $nWorkers --hostfile "./mpi_hosts" $args