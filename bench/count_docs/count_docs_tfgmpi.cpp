#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <atomic>
#include <filesystem>
#include <chrono>
#include <thread>
#include <mpi.h>


#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"
#include "distributed_libs/Hash_vector.cpp"
#include "DB_cache.cpp"

using String = std::string;
using Counter_type = Distributed_atomic<int>;


std::vector<std::string> loadDocs(std::string directory_path)
{
    std::vector<std::string> docs;

    for (const auto &entry : std::filesystem::directory_iterator(directory_path))
    {
        if (entry.is_directory())
        {
            continue;
        }

        std::ifstream file(entry.path());
        std::string str((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
        docs.push_back(str);
    }
    return docs;
}

int countWords(std::string str)
{
    int count = 0;
    int size = str.size();

    for (int i = 0; i < size; i++)
    {
        if (str[i] == ' ')
        {
            count++;
        }
    }

    return count;
}

long long wordsInCollection(Distributed_vector<String> &collection, int start, int nElems)
{
    long long local_count = 0;
    int size = collection.size();

    for (int i = start; i < start+nElems && i < size; i++)
    {
        local_count += countWords(collection[i]);
    }

    return local_count;
}

long long count_mb(std::vector<std::string> &docs)
{
    long long count = 0;
    for (auto &doc : docs)
    {
        count += doc.size();
    }

    return count / 1000000;
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    std::string files_directory = "/home/hsunekichi/Escritorio/zaguan_big";
    //std::string files_directory = "/tmp/a816678/zaguan_big";

    // Initialize the redis connection
    sw::redis::ConnectionOptions options;
    options.host = "192.168.1.49";
    options.port = 7000;

    auto pcache = std::make_shared<DB_cache>(options);
    sw::redis::Redis redis (options);
    redis.flushall();

    Distributed_vector<std::string> collection("collection", pcache);
    Counter_type count("count", pcache);
    count = 0;

    auto start_load = std::chrono::high_resolution_clock::now();

    if (rank == 0)
    {
        std::cout << "Loading docs..." << std::endl;
        auto docs = loadDocs(files_directory);
        std::cout << "Loaded " << docs.size() << " docs" << std::endl;

        auto start_send = std::chrono::high_resolution_clock::now();
        collection = docs;
        pcache->release_sync();

        auto end_load = std::chrono::high_resolution_clock::now();

        auto mb = count_mb(docs);

        std::cout << "Number of docs: " << docs.size() << std::endl;
        std::cout << "Send time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_load - start_send).count() << "ms" << std::endl;
        std::cout << "Bandwidth: " << ((double)mb / std::chrono::duration_cast<std::chrono::milliseconds>(end_load - start_send).count()) * 1000 << "MB/s" << std::endl;
        std::cout << "Load time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_load - start_load).count() << "ms" << std::endl;
        std::cout << "************************\n\n";
    }
    
    pcache->clear_cache();
    pcache->barrier_synchronization("load", size);
    
    if (rank == 0)
        std::cout << "Rank " << rank << " loaded" << std::endl;
    
    int docs_per_process = collection.size() / size;
    int remainder = collection.size() % size;

    

    // Synchronize nodes
    //pcache->barrier_synchronization("init", size, false);
    auto start_comp = std::chrono::high_resolution_clock::now();
    

    /********** Computation ***********/
    long long local_count = 0;
    if (rank < size-1)
        local_count = wordsInCollection(collection, docs_per_process*rank, docs_per_process);
    else
        local_count = wordsInCollection(collection, docs_per_process*rank, docs_per_process+remainder);
    
    count += local_count;

    auto start_wait = std::chrono::high_resolution_clock::now();

    // Synchronize nodes
    pcache->barrier_synchronization("finish", size, false);
    auto end_comp = std::chrono::high_resolution_clock::now();


    if (rank == 0)
    {
        std::cout << "Number of words: " << count.get() / 1000 << "k" << std::endl;
        std::cout << "Waiting time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_comp - start_wait).count() << "ms" << std::endl;
        std::cout << "Computation time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_comp - start_comp).count() << "ms" << std::endl;

        std::cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_comp - start_load).count() << "ms" << std::endl;
    }

    MPI_Finalize();
    return 0;
}