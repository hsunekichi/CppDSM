#include "redis++.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <atomic>

using Attrs = std::unordered_map<std::string, std::string>;
using Item = std::pair<std::string, std::optional<Attrs>>;
using ItemStream = std::vector<Item>;

using namespace std;


std::string makeFixedLength(const int i, const int length)
{
    if (false)
    {
        std::ostringstream ostr;

        ostr << std::setfill('0') << std::setw(length) << i;

        return ostr.str();
    }
    
    return to_string(i);
}

int pipeSize = 1000;
string script_block_size = "1000";

int nBlocks = 10000;
int nVars = 1000;
int nThreads = 16;
int nReps = 1;
int s_len = 4;
// Per hset value size = 7 + 2*slen

struct timeval
{
    long tv_sec;
    long tv_usec;
};



int gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}


/*

    vector<string> keys;
    vector<string> args;

    keys.push_back("lastUpdateId");
    args.push_back("");                     // Padding

    
    for (int i_block = 0; i_block < nBlocks; i_block++)         // For each block
    {
        keys.push_back("hset:"+makeFixedLength(i_block, s_len));
        keys.push_back("stream:hset:"+makeFixedLength(i_block, s_len));
        keys.push_back(makeFixedLength(nVars, s_len)); // Number of keys

        args.push_back("");                 // Padding
        args.push_back("");                 // Padding
        args.push_back("");                 // Padding

        for (int i_var = 0; i_var < nVars; i_var++)             // For each key
        {
            keys.push_back("key:"+makeFixedLength(i_block, s_len)+":"+makeFixedLength(i_var, s_len));
            args.push_back("value:"+makeFixedLength(i_block, s_len)+":"+makeFixedLength(i_var, s_len));
            nWrites++;
        }

        if (nWrites > pipeSize || i_block == nBlocks-1)
        {
            try {
                pipe.eval(script_hset, keys.begin(), keys.end(), args.begin(), args.end());
            }
            catch (const sw::redis::Error &e) {
                std::cout << e.what() << std::endl;
            }


            nWrites = 0;

            keys.clear();
            args.clear();

            keys.push_back("lastUpdateId");
            args.push_back("");                     // Padding
        }
    }

    pipe.exec();
*/


void escrituras(int id, sw::redis::ConnectionOptions options)
{
    sw::redis::RedisCluster redisc(options);

    string s_id = makeFixedLength(id, s_len);
    int local_nBlocks = nBlocks/nThreads;
    int ipagina = 0;

    auto pipe = redisc.pipeline(s_id +makeFixedLength(ipagina, s_len));
    
    int nWrites = 0;

    for (int i = 0; i < local_nBlocks; i++)
    {
        unordered_map<string, string> keys;
        for (int j = 0; j < nVars; j++)
        {
            keys[to_string(i)+":"+to_string(j)] = "0";
        }

        pipe.hset("{"+s_id+"}:"+to_string(i), keys.begin(), keys.end());
    }

    pipe.exec();
}

void escrituras_dir(int id, sw::redis::ConnectionOptions options)
{
    sw::redis::Redis redis(options);

    string s_id = makeFixedLength(id, s_len);
    int local_nBlocks = nBlocks/nThreads;
    auto pipe = redis.pipeline();
    int nWrites = 0;


    for (int i = 0; i < local_nBlocks; i++)
    {
        unordered_map<string, string> keys;
        for (int j = 0; j < nVars; j++)
        {
            keys[to_string(i)+":"+to_string(j)] = "0";
        }

        pipe.hset("{"+s_id+"}:"+to_string(i), keys.begin(), keys.end());
    }
    
    pipe.exec();
}

int main()
{   
    sw::redis::ConnectionOptions options, options_local;
    sw::redis::ConnectionPoolOptions pool_options;

    //options_local.host = "192.168.1.50";
    //options_local.port = 6379;

    //options.host = "192.168.1.50";
    //options.port = 7000;


    
    pool_options.size = nThreads;

    struct timeval begin, begin_general, end, end_general;
	long long seconds, microseconds;
    vector<thread> threads;

    sw::redis::Redis redis(options_local, pool_options);
    //sw::redis::QueuedRedis pipe = redis.pipeline();

    sw::redis::RedisCluster redisc(options, pool_options);

    std::cout << redis.ping() << std::endl;
    std::cout << redisc.redis("").ping() << std::endl;

    
    std::string coherency_hset2 =  
        "local i = 0;"
        "local j = 0;"
        "while i < #(KEYS) do "
            "local newId = redis.call('XADD', KEYS[i+1], '*', KEYS[i+3], ARGV[j+1]);"
            "redis.call('HSET', KEYS[i+2], KEYS[i+3], ARGV[j+1], KEYS[i+4], newId);"
            "i = i + 4;"
            "j = j + 1;"
        "end;"
        "return 1";

    const std::string script_hset =  
        "local lastUpdateId = KEYS[1];"
        "local key_processed = 2;"      // First key is lastUpdateId
        "local block_size = "+script_block_size+";"

        "while key_processed < #KEYS do "
            "local init_key = key_processed;"
            "local nKeys = KEYS[key_processed+2];"
            "local end_key = init_key+2+nKeys;"

            "local hset_key = KEYS[init_key];"
            "local stream_key = KEYS[init_key+1];"

            "for init_block = init_key+3, end_key, block_size do "  // For each block
                "local end_block = math.min(end_key, init_block + block_size);"

                "local j = 0;"
                "local values = {};"
                "values[end_block-init_block] = ' ';" // Preallocates memory
                

                "for i = init_block, end_block do "         // For each key
                    "local ind = j*2+1;"
                    "values[ind] = KEYS[i];"                // Adds the key and value to the block data
                    "values[ind+1] = ARGV[i];"
                    "j = j + 1;"
                "end "

                "local newId = redis.call('XADD', stream_key, '*', unpack(values));"
                "redis.call('HSET', hset_key, lastUpdateId, newId, unpack(values));"
            "end;"

            "key_processed = end_key+1;"
        "end;"
        
        "return 1";
    

    int vectorSize = 1 + nBlocks * 3 + nBlocks * nVars; 

    int index = 0;
    int nWrites = 0;
    double elapsed = 0;


    gettimeofday(&begin_general, NULL);
        
    for (int i = 0; i < nThreads; i++)
    {
        threads.push_back(thread(escrituras, i, options));
    }

    gettimeofday(&begin, NULL);
    
    for (auto& th : threads) th.join();

    gettimeofday(&end, NULL);

    gettimeofday(&end_general, NULL);


    seconds = end.tv_sec - begin.tv_sec;
    microseconds = end.tv_usec - begin.tv_usec;
    elapsed = (seconds + microseconds*1e-6);


    double elapsed_general = (end_general.tv_sec - begin_general.tv_sec) + (end_general.tv_usec - begin_general.tv_usec) / 1E6;

    nWrites = nBlocks * nVars;

    cout << "Tiempo de ejecucion cluster: " << elapsed << " s" << endl;

    cout << "Escrituras por segundo (cluster): " << (nWrites / elapsed)/1000 << "k" << endl;
    cout << "Ancho de banda de escritura (cluster): " << (nWrites*(7+s_len*2) + 12) / (elapsed*1000000) << " Mb/s" << endl;
    cout << "Ancho de banda de red (cluster): " << (nWrites*(7+5+4*s_len) + nBlocks*(5+12+3*s_len) + 12) / (elapsed*1000000) << " Mb/s" << endl;

    cout << "\n---------------------------------------\n\n";
}