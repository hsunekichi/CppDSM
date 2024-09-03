#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <atomic>
#include <filesystem>
#include <chrono>
#include <thread>
#include <mpi.h>

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

void mpi_send_string_vector(const String_vector &v, int id, int iMin, int iMax)
{
    int size = iMax - iMin;

    MPI_Send(&size, 1, MPI_INT, id, 0, MPI_COMM_WORLD);

    for (int i = iMin; i < iMax; i++)
    {
        int str_size = v[i].size();
        MPI_Send(&str_size, 1, MPI_INT, id, 0, MPI_COMM_WORLD);
        MPI_Send(v[i].c_str(), str_size, MPI_CHAR, id, 0, MPI_COMM_WORLD);
    }
}

// Like previous function but using MPI_Isend
void mpi_send_string_vector_async(const String_vector &v, int id, int iMin, int iMax)
{
    int size = iMax - iMin;

    MPI_Send(&size, 1, MPI_INT, id, 0, MPI_COMM_WORLD);

    // Create a vector of requests to wait for all of them
    std::vector<MPI_Request> requests(size * 2);
    std::vector<int> str_sizes(size);

    // Send doc sizes
    for (int i = 0; i < size; i++)
    {
        str_sizes[i] = v[i+iMin].size();
        MPI_Isend(&str_sizes[i], 1, MPI_INT, id, 0, MPI_COMM_WORLD, &requests[i * 2]);
    }

    // Send docs
    for (int i = 0; i < size; i++)
    {
        MPI_Isend(v[i+iMin].c_str(), str_sizes[i], MPI_CHAR, id, 0, MPI_COMM_WORLD, &requests[i * 2 + 1]);
    }

    // Wait for all send operations to complete
    MPI_Waitall(size * 2, requests.data(), MPI_STATUSES_IGNORE);
}

// Like previous function but using MPI_Isend
MPI_Request mpi_send_string_vector_blocked(const String_vector &v, int id, int iMin, int iMax)
{
    // Concatenate all strings
    std::string all_strs;
    for (int i = iMin; i < iMax; i++)
    {
        all_strs += v[i];
        all_strs += '\0';
    }

    int size = all_strs.size();

    MPI_Send(&size, 1, MPI_INT, id, 0, MPI_COMM_WORLD);

    MPI_Request status;
    // Send docs async
    MPI_Isend(&all_strs[0], size, MPI_CHAR, id, 0, MPI_COMM_WORLD, &status);

    return status;
}


void mpi_recv_string_vector(String_vector &v)
{
    int size;
    MPI_Recv(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    v.resize(size);

    for (int i = 0; i < size; i++)
    {
        int str_size;
        MPI_Recv(&str_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        v[i].resize(str_size);

        MPI_Recv(&v[i][0], str_size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
}

void mpi_recv_string_vector_async(String_vector &v)
{
    int size;
    MPI_Recv(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    v.resize(size);

    // Create a vector of requests to wait for all of them
    std::vector<MPI_Request> requests(size);
    std::vector<int> str_sizes(size);

    for (int i = 0; i < size; i++)
    {
        MPI_Irecv(&str_sizes[i], 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &requests[i]);
    }

    // Wait for all send operations to complete
    MPI_Waitall(size, requests.data(), MPI_STATUSES_IGNORE);

    for (int i = 0; i < size; i++)
    {
        v[i].resize(str_sizes[i]);
        MPI_Irecv(&v[i][0], str_sizes[i], MPI_CHAR, 0, 0, MPI_COMM_WORLD, &requests[i]);
    }

    // Wait for all send operations to complete
    MPI_Waitall(size, requests.data(), MPI_STATUSES_IGNORE);
}

void mpi_recv_string_vector_blocked(String_vector &v)
{
    // Recieve string size
    int size;
    MPI_Recv(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // Recieve string
    std::string all_strs(size, '\0');
    MPI_Recv(&all_strs[0], size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // Split string
    int i = 0;
    int j = 0;
    while (i < size)
    {
        while (all_strs[i] != '\0')
        {
            i++;
        }

        v.push_back(all_strs.substr(j, i - j));
        i++;
        j = i;
    }
}

long long  mpi_recv_count(String_vector &v)
{
    int size;
    MPI_Recv(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // Recieve string
    std::string all_strs(size, '\0');
    MPI_Recv(&all_strs[0], size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // Count number of spaces
    int count = 0;
    for (int i = 0; i < size; i++)
    {
        if (all_strs[i] == ' ')
        {
            count++;
        }
    }

    return count;
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    std::string files_directory = "/home/hsunekichi/Escritorio/zaguan_big";

    auto start_load = std::chrono::high_resolution_clock::now();

    if (rank == 0)
    {
        auto docs = loadDocs(files_directory);
        auto end_load = std::chrono::high_resolution_clock::now();

        std::cout << "Number of docs: " << docs.size() << std::endl;
        std::cout << "Load time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_load - start_load).count() << "ms" << std::endl;

        auto start_comp = std::chrono::high_resolution_clock::now();

        long long count = 0;
        long long docs_per_process = docs.size() / size;
        long long remainder = docs.size() % size;

        std::vector<MPI_Request> requests(size - 1);

        for (int i = 1; i < size; i++)
        {
            int start = i * docs_per_process;
            int end = start + docs_per_process;

            if (i == size - 1)
            {
                end += remainder;
            }

            auto req = mpi_send_string_vector_blocked(docs, i, start, end);
            requests[i - 1] = req;
        }

        for (int i = 1; i < size; i++)
        {
            MPI_Wait(&requests[i - 1], MPI_STATUS_IGNORE);
        }


        wordsInCollection(docs, count, 0, docs_per_process);

        for (int i = 1; i < size; i++)
        {
            long long local_count;
            MPI_Recv(&local_count, 1, MPI_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            count += local_count;
        }

        auto end_comp = std::chrono::high_resolution_clock::now();

        std::cout << "Number of words: " << count / 1000 << "k" << std::endl << std::endl;
        std::cout << "Computation time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end_comp - start_comp).count() << "ms" << std::endl;

        auto end = std::chrono::high_resolution_clock::now();

        std::cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_load).count() << "ms" << std::endl;
    }
    else
    {
        String_vector local_docs;
        long long local_count = mpi_recv_count(local_docs);

        //long long local_count;
        //wordsInCollection(local_docs, local_count, 0, local_docs.size());

        MPI_Send(&local_count, 1, MPI_LONG_LONG, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}