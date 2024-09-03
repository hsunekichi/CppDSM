#include <mpi.h>
#include <iostream>
#include <vector>
#include <chrono>

using namespace std;

int vSize = 1000 * 1000;
const int nReads = 1000 * 1000;

int main(int argc, char* argv[]) 
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);


    // Check result
    if (rank == 0) 
    {
        int value = 0;
        
        auto init = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < nReads; i++)
        {
            MPI_Send(&value, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
        }
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> elapsed_seconds = end - init;
        std::cout << "Latency: " << elapsed_seconds.count()*1000 / nReads << " ms " << std::endl;
        std::cout << "Throughput: " << nReads / elapsed_seconds.count() << " reads/s" << std::endl;
    }
    else if (rank == 1)
    {
        for (int i = 0; i < nReads; i++)
        {
            int value;
            MPI_Recv(&value, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }


    MPI_Finalize();

    return 0;
}
