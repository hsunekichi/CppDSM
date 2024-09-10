#include "distributed_libs/Distributed_queue.cpp"
#include "distributed_libs/distributed_atomics.cpp"
#include "distributed_libs/Hash_queue.cpp"
#include <random>
#include <queue>

#include "DB_cache.cpp"

using namespace std;

//#define COUNTER_TYPE long long
//#define MUTEX std::mutex
//#define QUEUE Hash_queue

using COUNTER_TYPE = std::atomic<long long>;
using MUTEX = std::mutex;
using QUEUE = std::queue<long long>;



bool is_prime(long long N)
{
    //std::this_thread::sleep_for(std::chrono::microseconds(1));

    // If the number is less than or equal to 1,
    // it is not prime
    if (N <= 1) {
        return false;
    }

    // If the number is 2 or 3, it is prime
    if (N <= 3) {
        return true;
    }

    // If the number is divisible by 2 or 3,
    // it is not prime
    if (N % 2 == 0 || N % 3 == 0) {
        return false;
    }

    // Check for divisors from 5
    // to the square root of N
    for (long long i = 5; i * i <= N; i += 6) 
    {
        // If N is divisible by i or (i + 2),
        // it is not prime
        if (N % i == 0 || N % (i + 2) == 0) {
            return false;
        }
    }

    // If no divisors are found, it is prime
    return true;
}

long long EXEC_TIME = 10;
long long BATCH_SIZE = 10000;
long long N_THREADS = 16;


void prime_worker (QUEUE &data, std::vector<long long> &primes, MUTEX &mtx_data, MUTEX &mtx_primes, COUNTER_TYPE &counter, std::atomic<bool> &finish)
{
    std::vector <long long> local_primes;
    auto t1 = std::chrono::high_resolution_clock::now();

    while (!finish)
    {
        // Get batch of data
        std::vector <long long> local_data(BATCH_SIZE);
        

        mtx_data.lock();
        
        for (long long i = 0; i < BATCH_SIZE; i++)
        {
            if (data.empty())
                break;

            local_data[i] = data.front();
            data.pop();
        }

        mtx_data.unlock();


        if (local_data.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Process the data
        for (auto &number : local_data)
        {            
            if (is_prime(number))
            {
                local_primes.push_back(number);
            }
        }

        counter += local_data.size();
    }

    auto t2 = std::chrono::high_resolution_clock::now();
    //std::cout << "Processed: " << counter << " in " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << " ms" << std::endl;
    

    mtx_primes.lock();
    primes.insert(primes.end(), local_primes.begin(), local_primes.end());
    mtx_primes.unlock();
}

// Returns a number between 100000000 and MAX
long long generate_number()
{
    long long MAX = std::numeric_limits<int>::max();
    MAX = MAX;

    long long seed = 123456789;
    static std::mt19937 gen(seed);
    static std::uniform_int_distribution<long long> dis(100000ll, MAX);

    return dis(gen);
}



int main(int argc, char *argv[])
{
    COUNTER_TYPE counter;
    QUEUE data;
    std::vector<long long> primes;
    MUTEX mtx_data;
    MUTEX mtx_primes;

    counter = 0;
    long long s_to_execute = EXEC_TIME;

    primes.clear();

    long long n_threads = 2; // std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    std::cout << "Threads: " << n_threads << std::endl;
    std::atomic<bool> finish = false;

    for (long long i = 0; i < n_threads; i++)
    {
        threads.push_back(std::thread(prime_worker, std::ref(data), std::ref(primes), std::ref(mtx_data), std::ref(mtx_primes), std::ref(counter), std::ref(finish)));
    }

    // Get time
    auto start = std::chrono::high_resolution_clock::now();

    // Insert data
    while (!finish)
    {
        std::vector<long long> batch;

        for (long long j = 0; j < BATCH_SIZE; j++)
        {
            batch.push_back(generate_number());
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        mtx_data.lock();
        for (auto &number : batch)
        {
            data.push(number);
        }
        mtx_data.unlock();
        auto t2 = std::chrono::high_resolution_clock::now();

        if (data.size() > 1000000)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

        //std::cout << "Data size: " << data.size() << std::endl;

        auto now = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
        
        if (duration.count() > s_to_execute * 1000)
        {
            finish = true;
        }
    }

    for (auto &t : threads)
    {
        t.join();
    }


    long long processed_primes = counter;
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << counter << " numbers processed in " << double(duration.count() / 1000) << " seconds" << std::endl;

    
    std::cout << "Processed per second: " << (double)processed_primes / double(duration.count() / 1000) << std::endl;
    std::cout << "Primes found: " << primes.size() << std::endl;


    return 0;
}


