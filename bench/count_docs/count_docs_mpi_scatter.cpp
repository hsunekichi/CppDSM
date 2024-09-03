#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <atomic>
#include <filesystem>
#include <chrono>
#include <thread>
#include <mpi.h>

// To get ip
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <cstring>

using String_vector = std::vector<std::string>;

String_vector loadDocs(std::string directory_path)
{
    String_vector docs;

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

void wordsInCollection(String_vector &collection, long long &local_count, int start, int end)
{
    int size = collection.size();
    local_count = 0;

    for (int i = start; i < end && i < size; i++)
    {
        local_count += countWords(collection[i]);
    }
}

void recieve_docs (String_vector &collection)
{
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    for (int i = 0; i < size; i++)
    {
        int count;
        MPI_Bcast(&count, 1, MPI_INT, i, MPI_COMM_WORLD);

        std::string doc;
        doc.resize(count);
        MPI_Bcast(&doc[0], count, MPI_CHAR, i, MPI_COMM_WORLD);

        collection.push_back(doc);
    }
}

void scatter_docs (String_vector &collection)
{
    std::string collection_str;
    for (auto &doc : collection)
    {
        collection_str += (doc + "\0");
    }

    MPI_Scatter(collection_str.c_str(), collection_str.size(), MPI_CHAR, MPI_IN_PLACE, 0, MPI_CHAR, 0, MPI_COMM_WORLD);
}

long long count_mb(String_vector &docs)
{
    long long count = 0;
    for (auto &doc : docs)
    {
        count += doc.size();
    }

    return count / 1000000;
}

// Rank 0 recieves no documents, but sends them to the other ranks asynchronously
void scatter_data(std::string &full_docs, std::string &local_docs, long long docs_per_process, int rank, int size)
{
    if (rank == 0)
    {
        std::vector<MPI_Request> requests(size);
        std::vector<int> sendcounts(size, docs_per_process);
        std::vector<int> displs(size, 0);
        for (int i = 1; i < size; i++)
        {
            displs[i] = (i-1) * docs_per_process;
            MPI_Isend(&full_docs[displs[i]], sendcounts[i], MPI_CHAR, i, 0, MPI_COMM_WORLD, &requests[i]);
        }

        MPI_Waitall(size-1, &requests[1], MPI_STATUSES_IGNORE);
    }
    else
    {
        MPI_Recv(&local_docs[0], docs_per_process, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
}

// Rank 0 recieves no documents, but sends them to the other ranks using scatterv
void scatterv_data(std::string &full_docs, std::string &local_docs, long long docs_per_process, int rank, int size)
{
    if (rank == 0)
    {
        std::vector<int> sendcounts(size, docs_per_process);
        std::vector<int> displs(size, 0);
        for (int i = 1; i < size; i++)
        {
            displs[i] = i * docs_per_process;
        }

        MPI_Scatterv(&full_docs[0], &sendcounts[0], &displs[0], MPI_CHAR, &local_docs[0], docs_per_process, MPI_CHAR, 0, MPI_COMM_WORLD);
    }
    else
    {
        MPI_Scatterv(&full_docs[0], NULL, NULL, MPI_CHAR, &local_docs[0], docs_per_process, MPI_CHAR, 0, MPI_COMM_WORLD);
    }
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    std::string files_directory = "/home/hsunekichi/zaguan_big";
    //std::string files_directory = "/tmp/a816678/zaguan_medium";
    std::string full_docs = "";
    std::string local_docs = "";

    long long docs_size = 0;

    auto start_load = std::chrono::high_resolution_clock::now();

    // Get local ip
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    std::string local_ip = std::string(hostname);

    long long mb = 0;

    if (rank == 0)
    {
        std::cout << "Loading docs..." << std::endl;
        
        auto docs = loadDocs(files_directory);
        for (auto &doc : docs)
        {
            full_docs += (doc);
        }

        // Add padding to make the string divisible by the number of processes
        int padding = size - (full_docs.size() % size);
        full_docs += std::string(padding, '\0');

        auto end_load = std::chrono::high_resolution_clock::now();

        docs_size = full_docs.size();

        for (int i = 0; i < size; i++)
        {
            MPI_Send(&docs_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        }

        mb = full_docs.size() / 1000000;

        std::cout << "Number of docs: " << docs.size() << std::endl;
        std::cout << "Load time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_load - start_load).count() << "ms" << std::endl;
    }
    else
    {
        int size;
        MPI_Recv(&docs_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    auto start_comp = std::chrono::high_resolution_clock::now();

    long long count = 0;
    long long docs_per_process = docs_size / (size-1);

    // Scatter the documents

    local_docs.resize(docs_per_process);
    //MPI_Scatter(&full_docs[0], docs_per_process, MPI_CHAR, &local_docs[0], docs_per_process, MPI_CHAR, 0, MPI_COMM_WORLD);
    scatter_data(full_docs, local_docs, docs_per_process, rank, size);
 
    
    //local_docs.resize(docs_per_process);
    //MPI_Scatter(&full_docs[0], docs_per_process, MPI_CHAR, &local_docs[0], docs_per_process, MPI_CHAR, 0, MPI_COMM_WORLD);

    auto end_scatter = std::chrono::high_resolution_clock::now();

    long long local_count = 0;
    // Count the words
    local_count = countWords(local_docs);

    // Gather the counts
    MPI_Reduce(&local_count, &count, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);


    auto end_comp = std::chrono::high_resolution_clock::now();


    if (rank == 0)
    {
        std::cout << "Number of words: " << count / 1000 << "k" << std::endl << std::endl;
        std::cout << "Send time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_scatter - start_comp).count() << "ms" << std::endl;
        std::cout << "Bandwidth: " << ((double)mb / std::chrono::duration_cast<std::chrono::milliseconds>(end_scatter - start_comp).count()) * 1000 << "MB/s" << std::endl;
        std::cout << "Computation time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_comp - start_comp).count() << "ms" << std::endl;

        auto end = std::chrono::high_resolution_clock::now();

        std::cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_load).count() << "ms" << std::endl;
    }
    
    MPI_Finalize();
    return 0;
}