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
#include "../src/DB_cache.cpp"
#include "redis++.h"
//#include <Windows.h>
#include <stdint.h>
#include <memory>
#include <thread>
#include <mutex>
#include <atomic>
//#include "debugger.cpp"

#include "../src/distributed_libs/distributed_atomics.cpp"

using namespace std;

int BUFFER_SLEEP_TIME = 50000;
int cache_latency = 100000;
int PIPE_SIZE = 1000;
bool USE_BUFFER =  false;
bool USE_CACHE = false;
bool concurrent_cache = false;
bool local_redis = true;
bool cluster_mode = false;

const int nThreads = 8;


string nombre = "DISPOSITIVO_";
int nEscrituras = 100;

sw::redis::ConnectionOptions options;


int local_gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}


// Increments a shared variable in redis,
// non atomic, spends a lot of time in the race conditions zone
int increment(string variable, string value_name, DB_cache *pcache)
{
    int val = -1;
    OptionalString value;

    value = pcache->hget(variable, value_name);
    //std::this_thread::sleep_for(std::chrono::milliseconds(1));  // Favors race conditions

    if (value)
    {   
        val = stoi(value.value());
        pcache->hset(variable, value_name, to_string(val + 1));
    }
    else
    {
        std::cerr << "Incrementing variable does not exist\n";
    }

    return val;
}


// Increments a shared variable non atomically
void test_incrementUpToN(int n, int id)
{
    DB_cache *pcache = new DB_cache(options, 1, 
                BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                USE_BUFFER, USE_CACHE, 
                base_cache_consistency::LRC);

    for (int i = 0; i < n; i++)
    {   
        increment("Shared_variable", "value", pcache);

        if ((i+1) % 100 == 0)
        {
            cout << "Thread " + to_string(id) + " ha incrementado " + to_string(i+1) + " veces\n";
        }
    }

    delete pcache;

    cout << "Finished thread " + to_string(id) + "\n";
}

void test_atomic_incrementUpToN(int n, int id, std::shared_ptr<DB_cache> pcache = nullptr)
{
    bool local_cache = (pcache == nullptr);

    if (local_cache)
    {
        pcache = std::make_shared<DB_cache>(options, 1,
                BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                USE_BUFFER, USE_CACHE, 
                base_cache_consistency::LRC);
    }  

    timeval start, end;

    Distributed_atomic<int> atomic = Distributed_atomic<int>("performance_sync_variable", pcache);

    for (int i = 0; i < n; i++)
    {
        atomic++;
    }
}

// Increments a shared variable atomically
void test_mtx_incrementUpToN(int n, int id, std::shared_ptr<DB_cache> pcache = nullptr)
{
    try {
    bool local_cache = (pcache == nullptr);

    if (local_cache)
    {
        pcache = std::make_shared<DB_cache>(options, 1,
                BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                USE_BUFFER, USE_CACHE, 
                base_cache_consistency::LRC);
    }

    Distributed_mutex mtx = Distributed_mutex("mtx", pcache);

    timeval t1, t2;

    for (int i = 0; i < n; i++)
    {
        mtx.lock();
        int old = increment("Shared_variable", "value", pcache.get());
        mtx.unlock();

        if ((i+1) % 100 == 0)
        {
            cout << "Thread " + to_string(id) + " ha incrementado " + to_string(i+1) + " veces\n";
        }
    }
    
    std::cout << "Thread " << id << " ha terminado\n";

    //pcache->release_sync();

    }
    catch (sw::redis::Error e)
    {
        cout << "Error al incrementar con mutex en thread " << id << endl;
        cout << e.what() << endl;

        throw e;
    }
}

//std::mutex Debugger::cout_mtx;
//std::atomic<bool> Debugger::fast_execution = false;

int main()
{
    cout << "iniciando test\n";

    struct timeval begin, end;
	long long seconds, microseconds;

    if (local_redis)
        options.host = "127.0.0.2";
    else
        options.host = "192.168.1.1";

    if (cluster_mode)
        options.port = 7000;
    else
        options.port = 6379;

    thread threads[nThreads];

    cout << "inicializando redis\n";

    sw::redis::RedisCluster *redisc = nullptr;
    sw::redis::Redis *redis = nullptr;

    if (cluster_mode)
        redisc = new sw::redis::RedisCluster(options);
    else
        redis = new sw::redis::Redis(options);
    
    //redis.flushdb();
    
    if (cluster_mode)
        cout << redisc->redis("").ping() << endl;
    else
        cout << redis->ping() << endl;
    
    if (!cluster_mode) 
        redis->flushdb();

    auto pcache = std::make_shared<DB_cache>(options, nThreads, 
                BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                USE_BUFFER, USE_CACHE, base_cache_consistency::LRC);

    //pcache->hset("mtx", "lck", "u");    // Creates the mutex if it does not exist
    //pcache->release_sync();             // Warrantees consistency

    Distributed_mutex mtx = Distributed_mutex ("mtx", pcache);
    
    pcache->hset("Shared_variable", "value", "0");    // Inicializar variable
    pcache->release_sync();

    /******************************************* TEST *******************************************/

    cout << "iniciando test\n";

    for (int i = 0; i < nThreads; i++)
    {
        if (concurrent_cache)
            threads[i] = thread(test_mtx_incrementUpToN, nEscrituras, i, pcache);
        else
            threads[i] = thread(test_mtx_incrementUpToN, nEscrituras, i, nullptr);
    }

    local_gettimeofday(&begin, NULL);

    for (int i = 0; i < nThreads; i++)
    {
        threads[i].join();
    }

    local_gettimeofday(&end, NULL);

    // ******************************************* RESULTADOS *******************************************
    
    pcache->acquire_sync();

    auto result = pcache->hget("Shared_variable", "value");    // Comprobar resultado
    
    cout << "\n****************************** RESULTADOS ******************************\n";
    if (stoi(result.value()) == nEscrituras*nThreads)
        cout << "Test de concurrencia correcto\n";
    else
        cout << "Test concurrencia incorrecto, valor esperado: " << nEscrituras*nThreads << ", valor obtenido: " << result.value() << "\n";

    seconds = end.tv_sec - begin.tv_sec;
    microseconds = end.tv_usec - begin.tv_usec;
    double elapsed = seconds + microseconds*1e-6;

    cout << "Tiempo transcurrido: " << elapsed << " segundos\n";
    cout << "Escrituras por segundo: " << nEscrituras*nThreads/elapsed << "\n";
    cout << "Tasa de fallos de bloqueo no atómico: " << mtx.getFallos()/(nEscrituras*nThreads) << "\n";
    cout << "Tasa iteraciones en espera: " << mtx.getItEsperando()/(nEscrituras*nThreads) << "\n";
    
    cout << "****************************** FIN RESULTADOS ******************************\n\n";


    /******************************************* TEST 2 *******************************************/

    pcache->clear_cache();

    cout << "Iniciando test 2\n";
    Distributed_atomic<int> atomic("performance_sync_variable", pcache);
    atomic = 0;

    for (int i = 0; i < nThreads; i++)
    {
        if (concurrent_cache)
            threads[i] = thread(test_atomic_incrementUpToN, nEscrituras, i, pcache);    // TSO consistency
        else
            threads[i] = thread(test_atomic_incrementUpToN, nEscrituras, i, nullptr);    // TSO consistency
    }

    local_gettimeofday(&begin, NULL);

    for (int i = 0; i < nThreads; i++)
    {
        threads[i].join();
    }

    local_gettimeofday(&end, NULL);

    seconds = end.tv_sec - begin.tv_sec;
    microseconds = end.tv_usec - begin.tv_usec;
    elapsed = seconds + microseconds*1e-6;


    cout << "****************************** RESULTADOS 2 ******************************\n";
    if (atomic.get() == nEscrituras*nThreads)
        cout << "Test de concurrencia correcto\n";
    else
        cout << "Test concurrencia incorrecto, valor esperado: " << nEscrituras*nThreads << ", valor obtenido: " << atomic.get() << "\n";

    cout << "Tiempo medio por iteración: " << elapsed/(nEscrituras*nThreads) << " segundos\n";
    cout << "Escrituras atómicas por segundo: " << nEscrituras*nThreads/elapsed << "\n";
    //cout << "Tasa de fallos de incremento no atómico: " << nFallos/(nEscrituras*nThreads) << "\n";
    cout << "****************************** FIN RESULTADOS ******************************\n\n";

    return 0;
}