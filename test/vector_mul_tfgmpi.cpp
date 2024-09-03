#include <mpi.h>
#include <iostream>
#include <vector>
#include <chrono>
#include "../src/distributed_libs/Hash_vector.cpp"
#include "../src/distributed_libs/Distributed_vector.cpp"
#include "../src/distributed_libs/distributed_atomics.cpp"


using namespace std;

int vSize = 1000*1000;

void init_multiply(Distributed_vector<int>& v3, int init, int chunk_size) 
{
    Hash_vector<int> v1(chunk_size);
    Hash_vector<int> v2(chunk_size);

    // Initialize the distributed data
    for (int i = 0; i < chunk_size; i++) 
    {
        //int id = init + i;
        //v1[i] = id;
        //v2[i] = vSize - id;
    }

    // Multiply vectors
    for (int i = 0; i < chunk_size; i++) {

        //for (int i = 0; i < 100; i++) {
        //    v3[init + i] = v1[i] * v2[i];
        //}
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
}


int check_result(Distributed_vector<int>& v, int init, int chunk_size) 
{
    for (int i = 0; i < chunk_size; i++) 
    {
        int id = init + i;

        if (v[id] != id * (vSize - id)) {
            return id;
        }
    }

    return -1;
}



int main(int argc, char* argv[]) 
{
    MPI_Init(&argc, &argv);

    bool verbose = true;

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int chunkSize = vSize / size;

    sw::redis::ConnectionOptions options;
    //options.host = "192.168.1.1";
    options.host = "127.0.0.1";
    //options.host = "192.168.1.37";

    options.port = 6379;

    // Initialize the distributed data
    auto pcache = std::make_shared<DB_cache>(options);

    Distributed_vector<int> g_v3("v3", pcache);
    Distributed_atomic<int> ready("ready", pcache);
    Distributed_atomic<int> correct("correct", pcache);

    ready = 0;
    correct = 0;

    auto init = std::chrono::high_resolution_clock::now();
    

    // Initialize vectors in parallel
    //initialize(v1, v2, startIdx, endIdx);

    // Multiply vectors in parallel
    init_multiply(g_v3, rank * chunkSize, chunkSize);

    pcache->release_barrier();

    auto end_mult = std::chrono::high_resolution_clock::now();

    int arrived_id = ready++;
    pcache->clear_cache();

    auto wait = std::chrono::high_resolution_clock::now();

    //while (ready != size)
    //{
    //    // Sleep
    //    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    //}

    if (arrived_id < size-1)
        pcache->wait_event("rev");
    else
        pcache->send_event("rev", "1");

    //std::cout << "All nodes arrived" << std::endl;

    auto check = std::chrono::high_resolution_clock::now();
    
    // Check the next nodes data
    //rank = rank+1 % size;
    int incorrect = check_result(g_v3, rank * chunkSize, chunkSize); 

    // Check result
    if (incorrect != -1)
    {
        correct = -(size*2);
        std::cout << "Test failed on i = " << incorrect << std::endl;
    }
    else
    {
        int nodes_correct = correct++;

        if (nodes_correct == size-1)
        {
            auto end = std::chrono::high_resolution_clock::now();

            if (verbose)
            {
                std::cout << "Time to multiply: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_mult - init).count() << " ms" << std::endl;
                std::cout << "Time to increment: " << std::chrono::duration_cast<std::chrono::milliseconds>(wait - end_mult).count() << " ms" << std::endl;
                std::cout << "Time to wait: " << std::chrono::duration_cast<std::chrono::milliseconds>(check - wait).count() << " ms" << std::endl;
                std::cout << "Time to check: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - check).count() << " ms" << std::endl;
                std::cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - init).count() << " ms" << std::endl;
            }
            else
            {
                std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - init).count() << std::endl;
            }
        }
    }


    MPI_Finalize();

    return 0;
}
