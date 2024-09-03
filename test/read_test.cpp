#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <functional>
#include <chrono>
#include <stdio.h>
#include "DB_cache.cpp"
#include "redis++.h"
//#include <Windows.h>
#include <stdint.h>
#include <memory>
#include <thread>
#include <mutex>
#include <atomic>

using namespace std;

int MAX_BUFFER_SIZE = 0;
int BUFFER_SLEEP_TIME = 50;
bool ASYNC_PUBLISH = true;
bool ASYNC_CACHE = true;
int PIPE_SIZE = 1000;
int UPDATER_SLEEP_TIME = 100;
int SUBSCRIBER_SLEEP_TIME = 100;
bool LOAD_ASSET = true;
bool USE_BUFFER = true;
bool USE_CACHE = true;
sw::redis::ConnectionOptions options;

atomic<int> nFallos(0);
atomic<int> nItEsperando(0);

struct redis_mtx
{
    string id, key;
};

redis_mtx mtx1 = { "mtx1", "value" };

/*
int local_gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}
*/


// Increments a shared variable in redis,
// non atomic, spends a lot of time in the race conditions zone
int increment(string variable, string value_name, DB_cache *pcache)
{
    int val = -1;

    OptionalString value = pcache->hget(variable, value_name);

    std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Favors race conditions

    if (value)
    {
        val = stoi(value.value());
        pcache->hset(variable, value_name, to_string(val + 1));
    }

    return val;
}

bool redis_mutex_lock(redis_mtx mtx, DB_cache *pcache)
{
    bool acquired = false;

    while (!acquired) // Until the key has been obtained atomically
    {
        bool locked = true;

        while (locked)  // Waits until the key is unlocked
        {
            OptionalString opt_mtx_value = pcache->hget_exclusive_acquire(mtx.id, mtx.key);    // Gets mtx key exclusively
            string mtx_value = opt_mtx_value.value_or("0");             // If the key doesn't exist, it's locked
            locked = !bool(stoi(mtx_value));

            if (locked)
            {
                nItEsperando++;
                this_thread::yield();
            }
        }

        if (pcache->hset_exclusive_release(mtx.id, mtx.key, "0"))  // Removes the key
            acquired = true;
        else
            nFallos++;
    }

    return true;
}

bool redis_mutex_unlock(redis_mtx mtx, DB_cache *pcache)
{
    pcache->hset(mtx.id, mtx.key, "1");      // Writes the key
    return true;
}


bool redis_mutex_lock_sequential(redis_mtx mtx, DB_cache *pcache)
{
    bool acquired = false;

    while (!acquired) // Until the key has been obtained atomically
    {
        bool locked = true;

        while (locked)  // Waits until the key is unlocked
        {
            OptionalString opt_mtx_value = pcache->hget_exclusive_acquire(mtx.id, mtx.key);    // Gets mtx key exclusively
            string mtx_value = opt_mtx_value.value_or("0");             // If the key doesn't exist, it's locked
            locked = !bool(stoi(mtx_value));

            if (locked)
            {
                nItEsperando++;
                this_thread::yield();
            }
        }

        if (pcache->hset_exclusive_release(mtx.id, mtx.key, "0"))  // Removes the key
            acquired = true;
        else
            nFallos++;
    }

    return true;
}

bool redis_mutex_unlock_sequential(redis_mtx mtx, DB_cache *pcache)
{
    pcache->hset(mtx.id, mtx.key, "1");      // Writes the key
    return true;
}

bool redis_mutex_init(redis_mtx mtx, DB_cache *pcache)
{
    pcache->hset(mtx.id, mtx.key, "1");      // Writes the key
    return true;
}


// Increments a shared variable non atomically
void test_incrementUpToN(int n, int id)
{
    DB_cache *pcache = new DB_cache(options);

    for (int i = 0; i < n; i++)
    {
        increment("Shared_variable", "value", pcache);

        if ((i+1) % 200 == 0)
        {
            cout << "Thread " + to_string(id) + " ha incrementado " + to_string(i+1) + " veces\n";
        }
    }

    delete pcache;
}

// Increments a shared variable atomically
void test_atomic_incrementUpToN(int n, int id)
{
    DB_cache *pcache = new DB_cache(options);

    for (int i = 0; i < n; i++)
    {
        redis_mutex_lock(mtx1, pcache);
        increment("Shared_variable", "value", pcache);
        redis_mutex_unlock(mtx1, pcache);

        if ((i+1) % 100 == 0)
        {
            cout << "Thread " + to_string(id) + " ha incrementado " + to_string(i+1) + " veces\n";
        }
    }

    delete pcache;
}

// Increments a shared variable atomically
void test_sequential_incrementUpToN(int n, int id)
{
    DB_cache *pcache = new DB_cache(options);

    for (int i = 0; i < n; i++)
    {
        redis_mutex_lock_sequential(mtx1, pcache);
        increment("Shared_variable", "value", pcache);
        redis_mutex_unlock_sequential(mtx1, pcache);

        if ((i+1) % 100 == 0)
        {
            cout << "Thread " + to_string(id) + " ha incrementado " + to_string(i+1) + " veces\n";
        }
    }

    delete pcache;
}



int main()
{
    cout << "iniciando test\n";

    struct timeval begin, end;
	long long seconds, microseconds;
    OptionalString result;

    string nombre = "DISPOSITIVO_";
    int nEscrituras = 100;
    int nLecturas = 100;
    int nRepeticiones = 3;
    int nBlocks = 10000;
    int nVars = 100;

    options.host = "127.0.0.1";
    //options.host = "192.168.1.50";
    options.port = 6379;


    const int nThreads = 6;
    thread threads[nThreads];

    cout << "inicializando redis\n";

    sw::redis::Redis redis (options);
    redis.flushdb();
    
    cout << redis.ping() << endl;


    /******************************************* TEST *******************************************/

    cout << "iniciando test\n";

    
    DB_cache *pcache = new DB_cache(options);
    DB_cache *pcache2 = new DB_cache(options);

    cout << "Cache creada\n";

    unordered_map<string, unordered_map<string, string>> variables;

    pcache->release_barrier();  // Waits for all the writes to be done

    this_thread::sleep_for(chrono::seconds(1)); // Gives time to the cache to update
    

    // CREATE THE VARIABLES
    for (int i = 0; i < nBlocks; i++)
    {
        pcache->hset("variable_"+to_string(i), "value", "iteracion_");
    }

    pcache->release_barrier();  // Waits for all the writes to be done

    gettimeofday(&begin, NULL);

    // FILL THE CACHE
    for (int i = 0; i < nBlocks; i++)
    {
        pcache2->hget("variable_"+to_string(i), "value");
    }

    gettimeofday(&end, NULL);

    seconds = end.tv_sec - begin.tv_sec;
    microseconds = end.tv_usec - begin.tv_usec;
    double elapsed = seconds + microseconds*1e-6;
    std::cout << "Time to fill the cache: " << elapsed << " seconds\n";


    // MODIFY THE VARIABLES
    for (int i = 0; i < nBlocks; i++)
    {
        for (int j = 0; j < nVars; j++)
        {
            pcache->hset("variable_"+to_string(i), "value_"+to_string(j), "iteracion_"+to_string(j));
        }
    }

    pcache->release_barrier();  // Waits for all the writes to be done

    gettimeofday(&begin, NULL);

    pcache2->acquire_barrier();
    gettimeofday(&end, NULL);


    //pcache->printCache();


    seconds = end.tv_sec - begin.tv_sec;
    microseconds = end.tv_usec - begin.tv_usec;
    elapsed = seconds + microseconds*1e-6;

    cout << "------------- Estadísticas de la iteración " << "-------------\n";
    cout << "Tiempo de ejecución: " << elapsed << " segundos\n";
    cout << "Lecturas por segundo: " << nLecturas*10/elapsed << "\n";

    int size = 0;
    for (auto it = variables.begin(); it != variables.end(); it++)
    {
        size += it->second.size();
    }

    cout << "Tamaño leido: " << size << "\n";

    return 0;
}