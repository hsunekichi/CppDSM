#include <mpi.h>
#include <iostream>
#include <vector>
#include <chrono>

using namespace std;

const int vSize = 200*1000*1000;
const int nBatches = 1;



int main(int argc, char* argv[]) 
{
    MPI_Init(&argc, &argv);

    int rank, cluster_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &cluster_size);
    int nWorkers = cluster_size - 1;



    // Check result
    if (rank == 0) 
    {
        std::vector<int> chunk(vSize);

        auto init = std::chrono::high_resolution_clock::now();

        MPI_Request requests[nWorkers*nBatches];

        for (int i = 0; i < nBatches; i++)
        {
            // Send async
            for (int i_node = 1; i_node < cluster_size; i_node++)
            {
                MPI_Isend(&chunk[0], vSize, MPI_INT, i_node, 0, MPI_COMM_WORLD, &requests[i*nWorkers + i_node]);       
            }
        }


        for (int i = 1; i < cluster_size; i++)
        {
            bool finished;
            MPI_Recv(&finished, 1, MPI_C_BOOL, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> elapsed_seconds = end - init;

        long long data_sent = vSize * sizeof(int) * nWorkers * nBatches;
        double bandwidth = data_sent / elapsed_seconds.count();

        std::cout << "Bandwidth: " << bandwidth / 1024 / 1024 / 1024 << " GB/s" << std::endl;
    }
    else 
    {
        std::vector<int> chunks [nBatches];

        for (int i = 0; i < nBatches; i++)
        {
            chunks[i].resize(vSize);
        }
        

        MPI_Request requests[nBatches];

        for (int i = 0; i < nBatches; i++)
        {
            // Recv async
            MPI_Irecv(&chunks[i][0], vSize, MPI_INT, 0, 0, MPI_COMM_WORLD, &requests[i]);
        }

        MPI_Waitall(nBatches, requests, MPI_STATUSES_IGNORE);

        bool finished = true;
        MPI_Send(&finished, 1, MPI_C_BOOL, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();

    return 0;
}
