#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/Hash_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"

#include <chrono>

#include "DB_cache.cpp"

using namespace std;


const int vSize = 1000*1000;
const int nThreads = 20;


bool check_result(int iMin, int iMax, Distributed_vector<int> v, int &incorrect)
{
    for (int i = iMin; i < iMax && i < vSize; i++)
    {
        if (v[i] != i * (vSize - i))
        {
            incorrect = i;
            return false;
        }
    }

    return true;
}

void init_multiply(
        sw::redis::ConnectionOptions options,
        int iMin, int iMax,
        std::shared_ptr<DB_cache> pcache)
{
    int block_size = vSize / nThreads;

    Hash_vector<int> v1(iMax - iMin);
    Hash_vector<int> v2(iMax - iMin);
    //std::vector<int> v3(iMax - iMin);

    Distributed_vector<int> g_v3("v3", pcache);
    Distributed_atomic<int> ready("ready", pcache);
    Distributed_atomic<int> correct("correct", pcache);

    // Initialize both vectors
    for(int i = iMin; i < iMax; i++)
    {
        v1[i-iMin] = i;
        v2[i-iMin] = vSize - i;
    }

    // Multiply both vectors
    for(int i = iMin; i < iMax; i++)
    {
        g_v3[i] = v1[i-iMin] * v2[i-iMin];
    }

    //pcache->release_barrier();
    int arrived_id = ready++;

    while (ready != nThreads)
    {
        // Sleep
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    
    pcache->acquire_barrier(true);

    int incorrect; 

    // Check result
    if (!check_result(block_size*arrived_id, block_size*(arrived_id+1), g_v3, incorrect))
    {
        correct = -(nThreads*2);
        std::cout << "Test failed on i = " << incorrect << std::endl;
        return;
    }
    else
    {
        int nodes_correct = correct++;

        if (nodes_correct == nThreads-1)
        {
            std::cout << "Test correct" << std::endl;
        }
    }
}

void th_init_multiply(
        sw::redis::ConnectionOptions options,
        int iMin, int iMax,
        std::shared_ptr<DB_cache> g_pcache)
{
    // Initialize the distributed data
    auto pcache = std::make_shared<DB_cache>(options);

    init_multiply(options, iMin, iMax, pcache);
}

int main(int argc, char *argv[])
{
    // Initialize the redis connection
    sw::redis::ConnectionOptions options;
    options.host = "127.0.0.1";
    //options.host = "127.0.0.1";
    //options.host = "192.168.1.37";

    options.port = 6379;

    sw::redis::Redis redis(options);

    // Initialize the distributed data
    auto pcache = std::make_shared<DB_cache>(options);

    std::vector <std::shared_ptr<DB_cache>> caches;

    for (int i = 0; i < nThreads; i++)
    {
        caches.push_back(std::make_shared<DB_cache>(options));
    }

    //Distributed_vector<int> v1("v1", pcache);
    //Distributed_vector<int> v2("v2", pcache);
    Distributed_vector<int> v3("v3", pcache);
    Distributed_atomic<int> ready("ready", pcache);
    Distributed_atomic<int> correct("correct", pcache);

    //std::vector<int> v1;
    //std::vector<int> v2;
    //std::vector<int> v3;

    //v1.clear();
    //v2.clear();
    v3.clear();
    ready = 0;
    correct = 0;

    // Initialize both vectors
    //v1.resize(vSize);
    //v2.resize(vSize);
    v3.resize(vSize);


    // Multiply both vectors

    // Create threads
    std::thread threads[nThreads];

    std::cout << "Launching " << nThreads << " threads" << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    for(int i = 0; i < nThreads; i++)
    {
        threads[i] = std::thread(init_multiply, 
                options,
                i * vSize / nThreads, 
                (i+1) * vSize / nThreads,
                caches[i]);
    }

    // Join threads
    for(int i = 0; i < nThreads; i++)
    {
        threads[i].join();
    }

    auto end = std::chrono::high_resolution_clock::now();

    cout << "********************************" << endl;

    cout << "Test executed in " << 
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << 
        "ms" << endl;

    return 0;
}


