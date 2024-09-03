#include "src/DB_cache.cpp"
#include <iostream>
#include <unordered_set>


int BUFFER_SLEEP_TIME = 50000;
int cache_latency = 100;
int PIPE_SIZE = 1000;
bool USE_BUFFER =  true;
bool USE_CACHE = true;
bool concurrent_cache = false;
bool local_redis = false;
bool cluster_mode = false;



bool concurrentSetTest(sw::redis::ConnectionOptions options)
{
    DB_cache db_cache (options, 
            BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
            USE_BUFFER, USE_CACHE, 
            1, base_cache_consistency::TSO);

    // Write 10000 values
    for (int i = 0; i < 10000; i++)
    {
        db_cache.set("testKey" + std::to_string(i), "testValue" + std::to_string(i));
    }

    return true;
}

bool basicTests(sw::redis::ConnectionOptions options)
{    
    DB_cache db_cache (options, 
            BUFFER_SLEEP_TIME, cache_latency, PIPE_SIZE, 
            USE_BUFFER, USE_CACHE, 
            1, base_cache_consistency::LRC);

    bool correct = true;

    // Test sadd
    db_cache.sadd("testSet", "testValue");
    db_cache.full_sync();

    if (!db_cache.sismember("testSet", "testValue")) {
        std::cerr << "sadd test failed\n";
        correct = false;
    }

    // Test srem
    db_cache.srem("testSet", "testValue");
    db_cache.full_sync();

    if (db_cache.sismember("testSet", "testValue")) {
        std::cerr << "srem test failed\n";
        correct = false;
    }

    // Test del
    db_cache.del("testSet");
    db_cache.full_sync();

    if (db_cache.scard("testSet") != 0) {
        std::cerr << "del test failed\n";
        correct = false;
    }

    // Test set and get
    db_cache.set("testKey", "testValue");
    db_cache.full_sync();

    if (db_cache.get("testKey") != "testValue") {
        std::cerr << "set/get test failed\n";
        correct = false;
    }

    // Test sismember
    db_cache.sadd("testSet", "testValue");
    db_cache.full_sync();

    if (!db_cache.sismember("testSet", "testValue")) {
        std::cerr << "sismember test failed\n";
        correct = false;
    }

    // Test smembers
    std::unordered_set<std::string> out;
    db_cache.smembers("testSet", out);

    if (out.find("testValue") == out.end()) {
        std::cerr << "smembers test failed\n";
        correct = false;
    }

    // Test hset and hget and hdel
    db_cache.hset("testHash", "testKey", "testValue");
    db_cache.full_sync();

    if (db_cache.hget("testHash", "testKey") != "testValue") {
        std::cerr << "hset/hget test failed\n";
        correct = false;
    }

    db_cache.hdel("testHash", "testKey");
    db_cache.full_sync();

    auto temp = db_cache.hget("testHash", "testKey");
    if (temp != std::nullopt) {
        std::cerr << "hdel test failed, returned: " << temp.value() << "\n";
        correct = false;
    }

    // Test hsetnx
    db_cache.hsetnx("testHash", "testKey", "testValue");
    db_cache.full_sync();

    if (db_cache.hget("testHash", "testKey") != "testValue") {
        std::cerr << "hsetnx test failed\n";
        correct = false;
    }

    db_cache.hsetnx("testHash", "testKey", "testValue2");
    db_cache.full_sync();

    if (db_cache.hget("testHash", "testKey") != "testValue") {
        std::cerr << "hsetnx test failed\n";
        correct = false;
    }

    return correct;
}


int main() 
{
    sw::redis::ConnectionOptions options;
    options.host = "127.0.0.1";
    options.port = 6379;

    bool passed;

    passed = basicTests(options);
    std::cout << "Basic test passed: " << (passed ? "True" : "False") << std::endl;

    return 0;
}