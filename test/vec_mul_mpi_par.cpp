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

    //MPI_Send(&local_chunk[0], chunkSize, MPI_INT, nodeId, 0, MPI_COMM_WORLD);
    
    // Send asynchronously
    MPI_Request request;
    MPI_Isend(&local_chunk[0], chunkSize, MPI_INT, nodeId, 0, MPI_COMM_WORLD, &request);
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
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
}


int check_result(Hash_vector<int>& v, int init, int chunk_size) 
{
    for (int i = 0; i < chunk_size; i++) 
    {
        int id = init + i;

        if (v[i] != id * (vSize - id)) {
            std::cout << "Error, expected: " << id * (vSize - id) << " got: " << v[i] << "\n";
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

    bool verbose = false;

    int real_rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &real_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int rank = real_rank;

    int chunkSize = vSize / size;

    Hash_vector<int> local_chunk(chunkSize);

    auto init = std::chrono::high_resolution_clock::now();


    // Initialize vectors in parallel
    //initialize(v1, v2, startIdx, endIdx);

    // Multiply vectors in parallel
    init_multiply(local_chunk, rank * chunkSize, chunkSize);

    int send_rank = (rank + 1) % size;

    // Send vector to the next id
    send_chunk(send_rank, local_chunk, chunkSize);

    rank = (rank - 1) % size;

    if (rank < 0)
    {
        rank = size + rank;
    }

    // Receive vector from the previous id
    recieve_chunk(rank, local_chunk, chunkSize);

    // Check result
    int error = check_result(local_chunk, rank*chunkSize, chunkSize);
    rank = real_rank;

    if (rank == 0)
    {
        // Recieve confirmations
        std::vector<int> confirmations(size, 0);
        confirmations[0] = error;
        
        for (int i = 1; i < size; i++)
        {
            MPI_Recv(&confirmations[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        // Check if there was an error
        for (int i = 0; i < size; i++)
        {
            if (confirmations[i] != -1)
            {
                error = confirmations[i];
                break;
            }
        }

        if (error != -1)
        {
            std::cout << "Error en la multiplicación de vectores en la posición " << error << "\n";
        }

        std::cout << "Executed in " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - init).count() << " ms\n";
    }
    else
    {
        // Send confirmation
        MPI_Send(&error, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }


    MPI_Finalize();

    return 0;
}
