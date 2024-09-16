#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"
#include "distributed_libs/Hash_vector.cpp"

#include "DB_cache.cpp"

using namespace std;


int vSize = 1000*1000;
//int vSize = 100;

int main(int argc, char *argv[])
{
    // Initialize the redis connection
    sw::redis::ConnectionOptions options;
    options.host = "127.0.0.1";
    //options.host = "192.168.1.37";
    options.port = 6379;


    sw::redis::Redis redis(options);

    redis.flushdb();

    // Initialize the distributed data
    auto pcache = std::make_shared<DB_cache>(options);

    std::cout << pcache->ping() << std::endl;

    Distributed_vector<int> v1("v1", pcache);
    Distributed_vector<int> v2("v2", pcache);
    Distributed_vector<int> v3("v3", pcache);

    //std::vector<int> v1;
    //std::vector<int> v2;
    //std::vector<int> v3;

    //Hash_vector<int> v1;
    //Hash_vector<int> v2;
    //Hash_vector<int> v3;

    

    v1.clear();
    v2.clear();
    v3.clear();

    // Initialize both vectors
    v3.resize(vSize);

    // Get time
    auto start = std::chrono::high_resolution_clock::now();

    for(int i = 0; i < vSize; i++)
    {
        v1.push_back(i);
        v2.push_back(vSize-i);
    }

    //pcache->full_sync(true);

    auto mult = std::chrono::high_resolution_clock::now();

    // Multiply both vectors
    for(int i = 0; i < vSize; i++)
    {
        v3[i] = v1[i] * v2[i];
    }

    auto check = std::chrono::high_resolution_clock::now();

    // Check result
    for(int i = 0; i < vSize; i++)
    {
        if(v3[i] != i * (vSize-i))
        {
            cout << "Error at index " << i << endl;
            cout << "Expected: " << i * (vSize-i) << 
                    ", found: " << v3[i] << endl;
            return 1;
        }
    }

    // Get time
    auto finish = std::chrono::high_resolution_clock::now();

    std::cout << "********************************\n";

    std::cout << "Time to initialize: " << 

        std::chrono::duration_cast<std::chrono::milliseconds>(mult - start).count() << " ms\n";
    std::cout << "Time to multiply: " <<
        std::chrono::duration_cast<std::chrono::milliseconds>(check - mult).count() << " ms\n";

    std::cout << "Time to check: " <<
        std::chrono::duration_cast<std::chrono::milliseconds>(finish - check).count() << " ms\n";
    
    std::cout << "Mean latency per access: " <<
        std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count() / (vSize*6) << " ns\n";

    std::cout << "Total time: " <<
        std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << " ms\n";

    std::cout << "********************************\n";

    return 0;
}


