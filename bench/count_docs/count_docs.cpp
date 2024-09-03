
#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <string>

#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/distributed_atomics.cpp"
#include "distributed_libs/Hash_vector.cpp"


using String_vector = std::vector<std::string>;

// Loads all docs in a directory as strings
String_vector loadDocs(std::string directory_path)
{
    String_vector docs;

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
    
    return count;
}

void wordsInCollection(
    String_vector &collection,
    std::atomic<long long> &count,
    int start, int end)
{
    int size = collection.size();
    long long local_count = 0;

    for (int i = start; i < end && i < size; i++)
    {
        local_count += countWords(collection[i]);
    }

    count += local_count;
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        std::cout << "Invoke as \"count_docs <path_to_docs>\"" << std::endl;
        return 1;
    }

    auto start_load = std::chrono::high_resolution_clock::now();

    auto docs = loadDocs(argv[1]);

    auto start_comp = std::chrono::high_resolution_clock::now();

    std::atomic<long long> count(0);

    wordsInCollection(docs, count, 0, docs.size());

    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Number of docs: " << docs.size() << std::endl;
    std::cout << "Number of words: " << count/1000 << "k" << std::endl << std::endl;

    std::cout << "Load time: " << std::chrono::duration_cast<std::chrono::milliseconds>(start_comp - start_load).count() << "ms" << std::endl;

    std::cout << "Computation time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_comp).count() << "ms" << std::endl;

    std::cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_load).count() << "ms" << std::endl;
};