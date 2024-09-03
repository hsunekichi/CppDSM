#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/Hash_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"

#include <chrono>

#include "DB_cache.cpp"

using namespace std;


const int vSize = 1000*1000;
const int nThreads = 20;
const int nReads = 1000*1000;

int main(int argc, char *argv[])
{
    // Initialize the redis connection
    sw::redis::ConnectionOptions options;
    options.host = "127.0.0.1";
    //options.host = "127.0.0.1";
    //options.host = "192.168.1.37";

    options.port = 7000;

    sw::redis::Redis redis(options);
    redis.flushall();
    // Initialize the distributed data
    auto pcache = std::make_shared<DB_cache>(options);

    std::vector<Distributed_variable<int>> v(nReads, Distributed_variable<int>("v", pcache));
    
    for (int i = 0; i < nReads; i++)
    {
        v[i].set_var_id("v" + std::to_string(i));
        v[i] = 0;
    }


    auto init = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < nReads; i++)
    {
        v[i].get();
    }

    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed_seconds = end - init;

    std::cout << "Latency: " << elapsed_seconds.count()*1000 / nReads << " ms " << std::endl;
    std::cout << "Throughput: " << nReads / elapsed_seconds.count() << " reads/s" << std::endl;

    
    return 0;
}


