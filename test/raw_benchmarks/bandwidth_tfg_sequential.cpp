#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/Hash_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"

#include <chrono>

#include "DB_cache.cpp"

using namespace std;


const long long vSize = 1000*1000;
const long long nThreads = 32;
const long long string_size = 1000;



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
    auto pcache1 = std::make_shared<DB_cache>(options);
    auto pcache2 = std::make_shared<DB_cache>(options);

    Distributed_vector<std::string> v1("v1", pcache1);
    Distributed_vector<std::string> v2("v1", pcache2);

    v1.resize(vSize);

    for (int i = 0; i < vSize; i++)
    {
        std::string content = std::string(string_size, 'a');
        
        for (int j = 0; j < string_size; j++)
        {
            content[j] = 'a' + (i + j) % 26;
        }

        v1[i] = content;
    }

    pcache1->release_barrier();

    pcache2->acquire_barrier();

    auto init = std::chrono::high_resolution_clock::now();
    v2.preload();
    auto end = std::chrono::high_resolution_clock::now();





    long long data_loaded = vSize * string_size;
    std::chrono::duration<double> elapsed_seconds = end - init;

    std::cout << "Time spent: " << elapsed_seconds.count() << " s" << std::endl;
    std::cout << "Bandwidth: " << data_loaded / elapsed_seconds.count() / 1024 / 1024 / 1024 << " GB/s" << std::endl;

    return 0;
}


