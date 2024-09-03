#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/Hash_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"

#include <chrono>

#include "DB_cache.cpp"

using namespace std;


const long long vSize = 100*1000;
const long long nThreads = 10;
const long long string_size = 1000;

const int port = 7000;


void thread_preloader(int id, std::vector<std::pair<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>> &times, std::mutex &g_mutex)
{
    auto pcache = std::make_shared<DB_cache>("localhost", port);

    Distributed_vector<std::string> v1("v1", pcache);
    Distributed_atomic<int> nFinished("nFinished", pcache);
    Distributed_atomic<int> nReady("nReady", pcache);

    pcache->acquire_barrier();


    auto init = std::chrono::high_resolution_clock::now();

    v1.preload();

    auto end = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < vSize; i++)
    {
        std::string content = v1[i];
        
        if (content.size() != string_size)
        {
            std::cout << "Error in content size" << std::endl;
        }
    }

    g_mutex.lock();
    times[id] = std::make_pair(init, end);
    g_mutex.unlock();

    nFinished++;
}


int main(int argc, char *argv[])
{
    // Initialize the redis connection
    sw::redis::ConnectionOptions options;
    options.host = "127.0.0.1";
    //options.host = "127.0.0.1";
    //options.host = "192.168.1.37";

    options.port = port;

    sw::redis::Redis redis(options);

    // Initialize the distributed data
    auto pcache = std::make_shared<DB_cache>(options);

    Distributed_vector<std::string> v1("v1", pcache);
    Distributed_atomic<int> nFinished("nFinished", pcache);
    Distributed_atomic<int> nReady("nReady", pcache);
    std::thread threads[nThreads];

    std::mutex g_mutex;
    std::vector<std::pair<std::chrono::high_resolution_clock::time_point, std::chrono::high_resolution_clock::time_point>> times(nThreads);
  
    redis.flushall();
    nFinished = 0;
    nReady = 0;
    v1.resize(vSize);

    for (int i = 0; i < vSize; i++)
    {
        std::string content = "";

        for (int j = 0; j < string_size; j++)
        {
            content += 'a' + (rand() % 26);
        }

        v1[i] = content;
    }

    pcache->release_barrier();

    for (int i = 0; i < nThreads; i++)
    {
        threads[i] = std::thread(thread_preloader, i, std::ref(times), std::ref(g_mutex));
    }

    for (int i = 0; i < nThreads; i++)
    {
        threads[i].join();
    }

    
    g_mutex.lock();
    // Mean of times
    double mean = 0;
    for (int i = 0; i < nThreads; i++)
    {
        mean += std::chrono::duration_cast<std::chrono::seconds>(times[i].second - times[i].first).count();
    }
    mean /= nThreads;
    g_mutex.unlock();


    long long data_per_node = vSize * string_size;
    long long data_sent = data_per_node * nThreads;
    double bandwidth = data_sent / mean;

    std::cout << data_sent << " bytes sent" << std::endl;
    std::cout << "Total bandwidth: " << bandwidth / 1024/1024/1024 << " GB/s" << std::endl;
    
    return 0;
}


