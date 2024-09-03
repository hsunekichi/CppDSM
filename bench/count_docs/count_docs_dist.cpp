
#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <string>

#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"
#include "distributed_libs/Hash_vector.cpp"
#include "DB_cache.cpp"

using Counter_type = Distributed_atomic<int>;

// Loads all docs in a directory as strings
std::vector<std::string> loadDocs(std::string directory_path)
{
    std::vector<std::string> docs;

    for (const auto & entry : std::filesystem::directory_iterator(directory_path))
    {
        // If it is a directory
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


// Counts the number of words in a string
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

    //std::this_thread::sleep_for(std::chrono::microseconds(100));

    return count;
}

Counter_type wordsInCollection(Distributed_vector<std::string> &collection,
        Counter_type count,
        int start, int end)
{
    int size = collection.size();
    long long local_count = 0;

    for (int i = start; i < end && i < size; i++)
    {
        local_count += countWords(collection[i]);
    }

    count += local_count;

    return count;
}

int main(int argc, char *argv[])
{
    if (argc != 5)
    {
        std::cout << "Invoke as \"count_docs <path_to_docs> <id> <max_workers> <is_loader>\"" << std::endl;
        return 1;
    }

    std::string path = argv[1];

    bool isLoader = false;

    try {
        isLoader = std::stoi(argv[4]);
    }
    catch (std::exception e)
    {
        std::cout << "Invoke as \"count_docs <path_to_docs> <id> <max_workers> <0/1>\"" << std::endl;
        return 1;
    }

    // Initialize the redis connection
    sw::redis::ConnectionOptions options;
    options.host = "127.0.0.1";
    options.port = 6379;

    auto pcache = std::make_shared<DB_cache>(options);

    Distributed_vector<std::string> collection("collection", pcache);
    Counter_type count("count", pcache);

    // Get id
    int id = 0;
    try {
        id = std::stoi(argv[2]);
    }
    catch (std::exception e)
    {
        std::cout << "Invoke as \"count_docs <path_to_docs> <id> <max_workers> <0/1>\"" << std::endl;
        return 1;
    }

    // Get max_workers
    int max_workers = 0;
    try {
        max_workers = std::stoi(argv[3]);
    }
    catch (std::exception e)
    {
        std::cout << "Invoke as \"count_docs <path_to_docs> <id> <max_workers> <0/1>\"" << std::endl;
        return 1;
    }

    if (isLoader)
    {
        auto start_load = std::chrono::high_resolution_clock::now();

        auto loaded_docs = loadDocs(argv[1]);
        collection = loaded_docs;
        count = 0;

        auto end_load = std::chrono::high_resolution_clock::now();

        std::cout << "Load time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_load - start_load).count() << "ms" << std::endl;
    }
    else
    {
        // Get index range
        int size = collection.size();
        int i_start = (size / max_workers) * id;
        int i_end = (size / max_workers) * (id + 1);

        auto start = std::chrono::high_resolution_clock::now();

        std::cout << "Working processing docs from " << i_start << " to " << i_end << std::endl;
        auto words = wordsInCollection(collection, count, i_start, i_end);

        auto end = std::chrono::high_resolution_clock::now();

        std::cout << "Number of docs: " << collection.size() << std::endl;
        std::cout << "Number of words: " << words/1000 << "k" << std::endl;
        std::cout << "Computation time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
        

        std::cout << std::endl;
    }
};