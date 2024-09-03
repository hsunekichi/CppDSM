#include <iostream>
#include <vector>
#include <thread>
#include <algorithm>

#include "distributed_libs/Distributed_vector.cpp"
#include "distributed_libs/Distributed_variables.cpp"
#include "distributed_libs/Hash_vector.cpp"
#include "DB_cache.cpp"

#define Vector Distributed_vector
//#define Vector Distributed_vector

std::mutex mtx;

// Partition function for quicksort
template<typename T>
int partition(Vector<T>& arr, int low, int high) 
{
    using std::swap;

    T pivot = arr[high];
    int i = (low - 1);
    for (int j = low; j <= high - 1; j++) {
        if (arr[j] < pivot) {
            i++;
            swap(arr[i], arr[j]);
        }
    }

    swap(arr[i + 1], arr[high]);
    return (i + 1);
}

// Quicksort function
template<typename T>
void quicksort(Vector<T>& arr, int low, int high) {
    if (low < high) {
        int pi = partition(arr, low, high);
        std::thread left_thread(quicksort<T>, std::ref(arr), low, pi - 1);
        std::thread right_thread(quicksort<T>, std::ref(arr), pi + 1, high);

        left_thread.join();
        right_thread.join();
    }
}

// Function to print the array
template<typename T>
void printArray(Vector<T>& arr) {
    for (int i = 0; i < arr.size(); i++) {
        std::cout << arr[i] << " ";
    }

    std::cout << std::endl;
}

template <typename T>
void checkArray(Vector<T>& arr) {
    for (int i = 0; i < arr.size() - 1; i++) {
        if (arr[i] > arr[i + 1] || arr[i] != i+1) {
            std::cout << "Array is not sorted" << std::endl;
            return;
        }
    }
    std::cout << "Array is sorted" << std::endl;
}

int main() {

    int nWrites = 5000;

    auto cache = std::make_shared<DB_cache>();
    Distributed_vector<int> dv("test_vector", cache);
    dv.resize(nWrites);
    Hash_vector<int> hv(nWrites);

    auto init = std::chrono::high_resolution_clock::now(); 

    for (int i = 0; i < nWrites; i++) 
    {
        dv[i] = nWrites - i;
        hv[i] = nWrites - i;
    }

    auto sort = std::chrono::high_resolution_clock::now();


    // Starting quicksort
    quicksort(dv, 0, nWrites - 1);

    auto check = std::chrono::high_resolution_clock::now();

    checkArray(dv);

    //printArray(dv);

    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Initialization time: " << std::chrono::duration_cast<std::chrono::milliseconds>(sort - init).count() << "ms" << std::endl;
    std::cout << "Sort time: " << std::chrono::duration_cast<std::chrono::milliseconds>(check - sort).count() << "ms" << std::endl;
    std::cout << "Check time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - check).count() << "ms" << std::endl;

    return 0;
}