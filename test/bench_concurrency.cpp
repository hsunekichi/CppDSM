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
#include "Redis_cache.cpp"
#include "redis++.h"
//#include <Windows.h>
#include <stdint.h>
#include <memory>
#include <thread>
#include <mutex>
#include <atomic>


using namespace std;

int BUFFER_SLEEP_TIME = 1;
int cache_latency = 300;
int PIPE_SIZE = 1000;
bool local_redis = false;
bool CLUSTER_MODE = true;

int nReps = 1;
int nThreads = 8;
int nVars = 100;
int nBlocks = 1000;
int nConcurrencyCache = nThreads;

string nombre = "DISPOSITIVO_";
int nEscrituras = 2000;

sw::redis::ConnectionOptions options;

atomic<int> g_nFallos(0);
atomic<int> g_nItEsperando(0);

struct redis_mtx
{
    string id, key;
};

redis_mtx mtx1 = { "mtx1", "value" };


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
int increment(string variable, string value_name, Redis_cache *pcache)
{
    int val = -1;

    OptionalString value = pcache->hget(variable, value_name);
   

    if (value)
    {
        val = stoi(value.value());
        pcache->hset(variable, value_name, to_string(val + 1));
    }

    return val;
}

bool redis_mutex_lock(redis_mtx mtx, Redis_cache *pcache, int id)
{
    bool aquired = false;
    string mtx_value = "";

    while (!aquired) // Until the key has been obtained atomically
    {
        bool locked = true;

        while (locked)  // Waits until the key is unlocked
        {
            OptionalString opt_mtx_value = pcache->hget_exclusive_aquire(mtx.id, mtx.key);    // Gets mtx key exclusively

            if (opt_mtx_value)
            {
                mtx_value = opt_mtx_value.value();             // If the key doesn't exist, it's locked
                locked = !bool(stoi(mtx_value));
            }
            else
            {
                cout << "El mutex no existe" << endl;
            }   

            if (locked)
            {
                g_nItEsperando++;
                this_thread::yield();
            }
        }

        if (pcache->hset_exclusive_release(mtx.id, mtx.key, "0"))  // Removes the key
            aquired = true;
        else
            g_nFallos++;
    }

    return true;
}

bool redis_mutex_unlock(redis_mtx mtx, Redis_cache *pcache, int id)
{
    //std::cout << "Thread " << id << " liberando mutex\n";
    pcache->release_barrier();              // Warrantees consistency
    pcache->hset(mtx.id, mtx.key, "1");     // Writes the key
    pcache->release_barrier();              // Forces the release as soon as posible
    
    return true;
}


bool redis_mutex_init(redis_mtx mtx, Redis_cache *pcache)
{
    pcache->hset(mtx.id, mtx.key, "1");      // Writes the key
    return true;
}



void test_atomic_incrementUpToN(int n, int b, int id, Redis_cache *pcache, bool use_cache, bool use_buffer)
{
    bool local_cache = (pcache == nullptr);
    g_nFallos = 0;

    if (local_cache)
    {
        pcache = new Redis_cache(options, 
                BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                use_buffer, use_cache, 1, base_cache_consistency::TSO);
    }

    for (int i = 0; i < n; i++)
    {
        bool performed = false;

        while (!performed)
        {
            OptionalString val = pcache->hget_exclusive_aquire("Shared_variable", "value");
            val = to_string(stoi(val.value()) + 1);            
            performed = pcache->hset_exclusive_release("Shared_variable", "value", val.value());
            
            if (!performed)
                g_nFallos++;
        }
    }

    if (local_cache)
        delete pcache;
}

// Increments a shared variable atomically
void test_mtx_incrementUpToN(int n, int b, int id, Redis_cache *pcache, bool use_cache, bool use_buffer)
{
    bool local_cache = (pcache == nullptr);

    if (local_cache)
    {
        pcache = new Redis_cache(options, 
                            BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                            use_buffer, use_cache, 1, base_cache_consistency::TSO);
    }

    for (int i = 0; i < n; i++)
    {
        redis_mutex_lock(mtx1, pcache, id);
        int old = increment("Shared_variable", "value", pcache);
        redis_mutex_unlock(mtx1, pcache, id);


        if ((i+1) % 100 == 0)
        {
            //cout << "Thread " + to_string(id) + " ha incrementado " + to_string(i+1) + " veces\n";
        }
    }

    if (local_cache)
        delete pcache;
}

void test_write_n(int a, int b, int id, Redis_cache *pcache, bool use_cache, bool use_buffer)
{
    bool local_cache = (pcache == nullptr);

    if (local_cache)
    {
        pcache = new Redis_cache(options, 
                            BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                            use_buffer, use_cache);
    }

    timeval t1, t2;

    local_gettimeofday(&t1, NULL);
    for (int i = 0; i < a; i++)
    {
        string key = "local_variable_id:"+to_string(id)+"_it:"+to_string(i);
        
        for (int j = 0; j < b; j++)
        {
            pcache->hset(key, "value_it:"+to_string(j), to_string(j));
        }
    }
    local_gettimeofday(&t2, NULL);

    std::cout << "Thread " << id << " ha tardado " << (t2.tv_sec - t1.tv_sec) * 1000.0 + (t2.tv_usec - t1.tv_usec) / 1000.0 << " ms en escribir " << a*b << " valores\n";

    //pcache->release_barrier();

    if (local_cache)
        delete pcache;
}


void run_test(void (*test)(int, int, int, Redis_cache *, bool, bool), int a, int b,
                int nThreads, bool concurrent_cache, bool use_buffer, 
                bool use_cache, double &elapsed, int &nFallos)
{
    vector<thread> threads(nThreads);
    struct timeval begin, end;
    Redis_cache *pcache = new Redis_cache(options, 
                            BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                            use_buffer, use_cache,
                            nConcurrencyCache);

    std::vector<Redis_cache*> caches(nThreads);

    pcache->hset("Shared_variable", "value", "0");    // Inicializar variable
    pcache->release_barrier();


    for (int i = 0; i < nThreads; i++)
    {
        if (concurrent_cache)
        {
            threads[i] = thread(test, a, b, i, pcache, use_cache, use_buffer);    // TSO consistency
        }
        else
        {
            caches[i] = new Redis_cache(options, 
                            BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                            use_buffer, use_cache);

            threads[i] = thread(test, a, b, i, caches[i], use_cache, use_buffer);    // TSO consistency
        }
    }
    timeval t1, t2;


    local_gettimeofday(&begin, NULL);

    for (int i = 0; i < nThreads; i++)
    {
        threads[i].join();
    }

    if (concurrent_cache)
    {
        pcache->release_barrier();
        delete pcache;
    }
    else
    {
        for (int i = 0; i < nThreads; i++)
        {
            caches[i]->release_barrier();
            delete caches[i];
        }
    }

    local_gettimeofday(&end, NULL);

    nFallos = g_nFallos;


    //OptionalString result = pcache->hget("Shared_variable", "value");    // Comprobar resultado

    //if (!result)
    //    cout << "No se ha podido obtener el resultado\n";

    //if (stoi(result.value()) != nEscrituras*nThreads)
    //    cout << "Test incorrecto, valor esperado: " << a*b*nThreads << ", valor obtenido: " << result.value() << "\n";

    double seconds = end.tv_sec - begin.tv_sec;
    double microseconds = end.tv_usec - begin.tv_usec;
    elapsed = seconds + microseconds*1e-6;
}


void test_combinatory(void (*test)(int, int, int, Redis_cache *, bool, bool), int a, int b)
{
    bool use_buffer, use_cache, concurrent_cache;
    int nFallos = 0;
    double elapsed = 0;


    /******************************************* Concurrent, buffer and cache *******************************************/

    use_buffer = true;
    use_cache = true;
    concurrent_cache = true;

    double elapsed_cbc = 0;
    int nFallos_cbc = 0;
    for (int i = 0; i < nReps; i++)
    {
        //run_test(test, a, b, nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_cbc += elapsed;
        nFallos_cbc += nFallos;
    }


    elapsed_cbc /= nReps;
    nFallos_cbc /= nReps;

    /******************************************* Concurrent, buffer *******************************************/

    use_buffer = true;
    use_cache = false;
    concurrent_cache = true;

    double elapsed_cb = 0;
    int nFallos_cb = 0;
    for (int i = 0; i < nReps; i++)
    {
        run_test(test, a, b,  nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_cb += elapsed;
        nFallos_cb += nFallos;
    }

    std::cout << "elapsed concurrent: " << elapsed_cb/nReps << endl;

    elapsed_cb /= nReps;
    nFallos_cb /= nReps;

    /******************************************* Concurrent, cache *******************************************/

    use_buffer = false;
    use_cache = true;
    concurrent_cache = true;

    double elapsed_cc = 0;
    int nFallos_cc = 0;
    for (int i = 0; i < nReps; i++)
    {
        //run_test(test, a, b,  nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_cc += elapsed;
        nFallos_cc += nFallos;
    }

    elapsed_cc /= nReps;
    nFallos_cc /= nReps;

    /******************************************* Concurrent *******************************************/

    use_buffer = false;
    use_cache = false;
    concurrent_cache = true;

    double elapsed_c = 0;
    int nFallos_c = 0;
    for (int i = 0; i < nReps; i++)
    {
        ///run_test(test, a, b,  nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_c += elapsed;
        nFallos_c += nFallos;
    }

    elapsed_c /= nReps;
    nFallos_c /= nReps;

    /******************************************* distributed, buffer and cache *******************************************/

    use_buffer = true;
    use_cache = true;
    concurrent_cache = false;

    double elapsed_dbc = 0;
    int nFallos_dbc = 0;
    for (int i = 0; i < nReps; i++)
    {
        //run_test(test, a, b,  nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_dbc += elapsed;
        nFallos_dbc += nFallos;
    }

    elapsed_dbc /= nReps;
    nFallos_dbc /= nReps;

    /******************************************* distributed, buffer *******************************************/

    use_buffer = true;
    use_cache = false;
    concurrent_cache = false;

    double elapsed_db = 0;
    int nFallos_db = 0;
    for (int i = 0; i < nReps; i++)
    {
        run_test(test, a, b,  nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_db += elapsed;
        nFallos_db += nFallos;
    }

    std::cout << "elapsed distributed: " << elapsed_db/nReps << endl;

    elapsed_db /= nReps;
    nFallos_db /= nReps;

    /******************************************* distributed, cache *******************************************/

    use_buffer = false;
    use_cache = true;
    concurrent_cache = false;

    double elapsed_dc = 0;
    int nFallos_dc = 0;
    for (int i = 0; i < nReps; i++)
    {
        //run_test(test, a, b, nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_dc += elapsed;
        nFallos_dc += nFallos;
    }

    elapsed_dc /= nReps;
    nFallos_dc /= nReps;

    /******************************************* distributed *******************************************/

    use_buffer = false;
    use_cache = false;
    concurrent_cache = false;

    double elapsed_d = 0;
    int nFallos_d = 0;
    for (int i = 0; i < nReps; i++)
    {
        //run_test(test, a, b,  nThreads, concurrent_cache, use_buffer, use_cache, elapsed, nFallos);
        elapsed_d += elapsed;
        nFallos_d += nFallos;
    }

    elapsed_d /= nReps;
    nFallos_d /= nReps;


    /******************************************* Results *******************************************/

    int nTotalWrites = nThreads * a * b;
    int nWrites_cbc = nTotalWrites / elapsed_cbc;
    int nWrites_cb = nTotalWrites / elapsed_cb;
    int nWrites_cc = nTotalWrites / elapsed_cc;
    int nWrites_c = nTotalWrites / elapsed_c;

    nFallos_cbc = nFallos_cbc / nTotalWrites;
    nFallos_cb = nFallos_cb / nTotalWrites;
    nFallos_cc = nFallos_cc / nTotalWrites;
    nFallos_c = nFallos_c / nTotalWrites;


    int nWrites_dbc = nTotalWrites / elapsed_dbc;
    int nWrites_db = nTotalWrites / elapsed_db;
    int nWrites_dc = nTotalWrites / elapsed_dc;
    int nWrites_d = nTotalWrites / elapsed_d;

    nFallos_dbc = nFallos_dbc / nTotalWrites;
    nFallos_db = nFallos_db / nTotalWrites;
    nFallos_dc = nFallos_dc / nTotalWrites;
    nFallos_d = nFallos_d / nTotalWrites;



    cout << "                   Concurrent ------------------ Distributed\n";
    cout << "Buffer, cache ---- " << nWrites_cbc/1000 << "kw/s, " << nFallos_cbc << " fallos ---- " << nWrites_dbc/1000 << "kw/s, " << nFallos_dbc << " fallos\n";
    cout << "---------------------------------------------------------------------------------\n";
    cout << "Buffer ----------- " << nWrites_cb/1000 << "kw/s, " << nFallos_cb << " fallos ---- " << nWrites_db/1000 << "kw/s, " << nFallos_db << " fallos\n";
    cout << "---------------------------------------------------------------------------------\n";
    cout << "Cache ------------ " << nWrites_cc/1000 << "kw/s, " << nFallos_cc << " fallos ---- " << nWrites_dc/1000 << "kw/s, " << nFallos_dc << " fallos\n";
    cout << "---------------------------------------------------------------------------------\n";
    cout << "Ninguno ---------- " << nWrites_c/1000 << "kw/s, " << nFallos_c << " fallos ---- " << nWrites_d/1000 << "kw/s, " << nFallos_d << " fallos\n";
}





//std::mutex Debugger::cout_mtx;
//std::atomic<bool> Debugger::fast_execution = false;

int main()
{
    cout << "iniciando test\n";

    int nFallos;
    double elapsed; 
    bool use_buffer, use_cache;

    OptionalString result;

    if (local_redis)
        options.host = "127.0.0.1";
    else
        options.host = "192.168.1.50";

    if (CLUSTER_MODE)
        options.port = 7000;
    else
        options.port = 6379;


    cout << "inicializando redis\n";

    sw::redis::Redis redis (options);
    redis.flushdb();
    
    cout << redis.ping() << endl;

    Redis_cache *pcache = new Redis_cache(options, 
                            BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
                            false, false);


    cout << "\n********************************************\n";
    cout << "************** MTX INCREMENT **********************\n";
    redis_mutex_init(mtx1, pcache);
    //test_combinatory(test_mtx_incrementUpToN, nEscrituras, 1);

    cout << "\n\n********************************************\n";
    cout << "************** ATOMIC INCREMENT **********************\n";
    //test_combinatory(test_atomic_incrementUpToN, nEscrituras, 1);

    cout << "\n\n********************************************\n";
    cout << "************** LOCAL WRITES **********************\n";
    test_combinatory(test_write_n, nBlocks, nVars);

    cout << "\n\n********************************************\n";


    return 0;
}