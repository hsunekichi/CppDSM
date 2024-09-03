#include <mpi.h>
#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include "../src/distributed_libs/Hash_vector.cpp"

using namespace std;

int vSize = 1000*1000;

void initialize(Hash_vector<int>& v1, Hash_vector<int>& v2, int iMin, int iMax) {
    // Initialize the distributed data
    for (int i = iMin; i < iMax; i++) {
        v1[i] = i;
        v2[i] = vSize - i;
    }
}

void send_array(Hash_vector<int>& v, int size, int chunkSize) 
{
    std::vector<int> local_chunk(chunkSize);

    for (int i = 1; i < size; i++) 
    {
        for (int j = 0; j < chunkSize; j++) {
            local_chunk[j] = v[i * chunkSize + j];
        }

        MPI_Send(&local_chunk[0], chunkSize, MPI_INT, i, 0, MPI_COMM_WORLD);
    }
}

void send_array(std::vector<int>& v, int size, int chunckSize)
{
    for (int i = 1; i < size; i++) 
    {
        MPI_Send(&v[i * chunckSize], chunckSize, MPI_INT, i, 0, MPI_COMM_WORLD);
    }
}

void send_chunk(int nodeId, Hash_vector<int>& v, int chunkSize) 
{
    std::vector<int> local_chunk(chunkSize);

    for (int i = 0; i < chunkSize; i++) {
        local_chunk[i] = v[i];
    }

    MPI_Send(&local_chunk[0], chunkSize, MPI_INT, nodeId, 0, MPI_COMM_WORLD);
}

void recieve_chunk (int nodeId, Hash_vector<int>& v, int chunkSize) 
{
    std::vector<int> local_chunk(chunkSize);
    MPI_Recv(&local_chunk[0], chunkSize, MPI_INT, nodeId, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    for (int i = 0; i < chunkSize; i++) {
        v[i] = local_chunk[i];
    }
}

void recieve_vector(Hash_vector<int>& v, int size, int chunkSize) 
{
    for (int i = 1; i < size; i++) 
    {
        std::vector<int> local_chunk(chunkSize);
        MPI_Recv(&local_chunk[0], chunkSize, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int j = 0; j < chunkSize; j++) {
            v[i * chunkSize + j] = local_chunk[j];
        }
    }
}

void multiply(Hash_vector<int>& v1, Hash_vector<int>& v2, Hash_vector<int>& v3, int iMin, int iMax) {
    // Multiply vectors
    for (int i = iMin; i < iMax; i++) {
        v3[i] = v1[i] * v2[i];
    }
}

void init_multiply(Hash_vector<int>& v3, int init, int chunk_size) 
{
    Hash_vector<int> v1(chunk_size);
    Hash_vector<int> v2(chunk_size);

    // Initialize the distributed data
    for (int i = 0; i < chunk_size; i++) 
    {
        int id = init + i;
        v1[i] = id;
        v2[i] = vSize - id;
    }

    // Multiply vectors
    for (int i = 0; i < chunk_size; i++) {
        v3[i] = v1[i] * v2[i];
        //std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
}


int check_result(Hash_vector<int>& v, int init, int chunk_size) 
{
    for (int i = 0; i < chunk_size; i++) 
    {
        int id = init + i;

        if (v[i] != id * (vSize - id)) {
            return id;
        }
    }

    return -1;
}


void recieve_vector(std::vector<int>& v, int size, int chunkSize) 
{
    for (int i = 1; i < size; i++) 
    {
        MPI_Recv(&v[i * chunkSize], chunkSize, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
}


int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    bool verbose = true;

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int chunkSize = vSize / size;

    Hash_vector<int> local_chunk(chunkSize);

    auto init = std::chrono::high_resolution_clock::now();


    // Initialize vectors in parallel
    //initialize(v1, v2, startIdx, endIdx);

    // Multiply vectors in parallel
    init_multiply(local_chunk, rank * chunkSize, chunkSize);

    // Check result
    if (rank == 0) 
    {
        Hash_vector<int> complete_vector(vSize);

        // Copy the local chunk to the complete vector
        for (int i = 0; i < chunkSize; i++) {
            complete_vector[i] = local_chunk[i];
        }

        // Recieve the rest of vector slices
        recieve_vector(complete_vector, size, chunkSize);

        auto check = std::chrono::high_resolution_clock::now();

        // Send chunks to check
        send_array(complete_vector, size, chunkSize);

        int incorrect = check_result(complete_vector, 0, chunkSize);

        // Recieve boolean confirmations
        for (int i = 1; i < size; i++) 
        {
            int incorrectIdx;
            MPI_Recv(&incorrectIdx, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (incorrectIdx != -1) {
                incorrect = incorrectIdx;
                std::cout << "Error at index " << incorrect << std::endl;
            }
        }

        if (incorrect != -1) {
            cout << "Error at index " << incorrect << endl;
            cout << "Expected: " << incorrect * (vSize - incorrect) << ", found: " << complete_vector[incorrect] << endl;
        }
        else if (verbose)
        {
            cout << "Test correct" << endl;
        }

        auto end = std::chrono::high_resolution_clock::now();

        if (verbose)
        {
            cout << "Multiplication time: " << std::chrono::duration_cast<std::chrono::milliseconds>(check - init).count() << " ms" << endl;
            cout << "Check time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - check).count() << " ms" << endl;
            cout << "Total test took " << std::chrono::duration_cast<std::chrono::milliseconds>(end - init).count() << " ms" << endl;
        }
        else
        {
            cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - init).count() << endl;
        }
    }
    else {
        // Send the result to the root process
        send_chunk(0, local_chunk, chunkSize);

        // Recieve new block
        recieve_chunk(0, local_chunk, chunkSize);

        // Check result
        int incorrect = check_result(local_chunk, rank * chunkSize, chunkSize);

        MPI_Send(&incorrect, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }


    MPI_Finalize();

    return 0;
}
