# Description

This is a DSM system that allows to use shared memory variables accross many machines seamlessly, with the same interface as C++ concurrent variables (mutex, atomics, std::vectors...).

You may take any correct C++ concurrent code, and deploy it across independant machines by only changing the data types.

## Interface

The system offers low level instructions similar to Redis and ARM. 
It also offers a higher level library that mimics C++ STL data structures, using the low level interface.

There are many read and write operations that work exactly like Redis interface. 
The system implements hash maps, sets, key-value and subscription queues, all for strings.

Synchronization operations follow ARMv8 interface, and provide consistency barriers, exclusive read-write, event propagation and an execution barrier to synchronize the running nodes.

It uses Lazy Release Consistency, which basically delays synchronization to critical section entrance and exit. The implications of it are discussed more in-depth in the main documentation, which unfortunately right now is only written in spanish.

Operations that are not clones of C++ STL or Redis have comments explaining its usage, but more detailed documentation would be helpful. 

## Structure

The core code for the project is in /src. It contains the base library: DB_cache, which provides read-write and sync operations in an ARM assembly fashion. 

In Distributed_libs there are high level libraries that mimic C++ STL interface, allowing to deploy programs written for concurrent C++ in remote machines by only changing the data types.

The test and bench directories contain several programs used to measure performance and test the system.

In the root directory there is an example bash script to compile with this library, and another one to compile an MPI program.

There is a detailed documentation of the architecture and protocols of all the system, but right now it is only written in spanish.

## Architecture

Each instance of this library has a buffer and a cache, which work similar to a CPU memory hierarchy. The main difference is it uses Lazy Release Consistency to reduce network communication to the minimum, while CPUs usually use stronger models. In most use cases, the programmer won't even notice the difference. 

Each instance of the library connects to a Redis Cluster (and detects automatically if it's a cluster or not), and uses it as its main data storage (like the RAM). There is a virtual interface that manages communication with Redis, so migration to other DB systems will be easier. 

## Performance

This project is built arround the hash-table data structure due to its original use case, although it has other less optimized data structures. 

The high level libraries have been designed arround the hash table to obtain the maximum performance the system allows, but there is room for improvement on basic data types like integer arrays due to the hash table memory indirections and the mutex locks in the cache and the buffer. 

It has high performance, close to MPI in bandwidth use on the basic scenarios tested. 
The average latency per access is only arround 3 times higher than using directly the local RAM, despite having all the data distributed across remote machines on a ~30ms latency network.

NOTE: Currently there is no eviction policy. 
Cache grows indefinitely until it is cleared, so some eviction policy to free spaces automatically would be interesting

## Correctness

This project is experimental, the test cases are basic so more in-depth tests should be made to warrantee its correction. 

Performance has also been tested in basic scenarios, so there may be room for improvement in less tested cases like more complex synchronization scenarios.

## Dependencies

All the system uses standard C++ except for the communication layer, which needs Redis++ and a running Redis instance.
Implementing another communication layer would change those requirements.
