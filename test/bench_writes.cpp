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

using namespace std;


int local_gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}

int main()
{
    bool local_redis = true;


    cout << "iniciando test\n";

    struct timeval begin, end;
	long long seconds, microseconds;

    int nEscrituras = 1000 * 1000;

    sw::redis::ConnectionOptions options;

    if (local_redis)
        options.host = "127.0.0.1";
    else
        options.host = "192.168.0.33";

    options.port = 6379;

    int BUFFER_SLEEP_TIME = 50;
    int PIPE_SIZE = 1000;
    bool USE_BUFFER = true;
    bool USE_CACHE = true;

    cout << "inicializando redis\n";

    sw::redis::Redis redis (options);
    redis.flushdb();
    
    cout << redis.ping() << endl;

    DB_cache pcache(options);
    
    local_gettimeofday(&begin, 0);

    for (int i = 0; i < nEscrituras; i++)  
    {
        pcache.hset("elemento", "color"+to_string(i), "rojo"+to_string(i));
    }

    local_gettimeofday(&end, 0);
	seconds = end.tv_sec - begin.tv_sec;
	microseconds = end.tv_usec - begin.tv_usec;
	double tiempoEscrituraEfectivo = seconds + microseconds * 1e-6;

    pcache.release_barrier();

    local_gettimeofday(&end, 0);
	seconds = end.tv_sec - begin.tv_sec;
	microseconds = end.tv_usec - begin.tv_usec;
	double tiempoEscrituraReal = seconds + microseconds * 1e-6;

    cout << "tiempo escritura efectivo: " << tiempoEscrituraEfectivo << endl;
    cout << "tiempo de escritura real: " << tiempoEscrituraReal << endl;

    cout << "Escrituras efectivas por segundo: " << nEscrituras/tiempoEscrituraEfectivo << endl;
    cout << "Escrituras reales por segundo: " << nEscrituras/tiempoEscrituraReal << endl;

    return 0;
}