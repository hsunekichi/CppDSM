#include "redis++.h"
#include <chrono>
#include <iostream>

int main() 
{
    sw::redis::ConnectionOptions connection_options;
    connection_options.host = "127.0.0.1";
    connection_options.port = 6379;

    sw::redis::ConnectionPoolOptions pool_options;
    pool_options.size = 10;

    sw::redis::Redis redis(connection_options, pool_options);

    int nHashes = 10000;
    int nFields = 1000;

    auto pipe = redis.pipeline();

    auto start = std::chrono::high_resolution_clock::now();
    for (int j = 0; j < nHashes; j++)
    {
        for (int i = 0; i < nFields; i++)
        {
            pipe.hset("hash"+std::to_string(j), "field"+std::to_string(i), "value"+std::to_string(i));
        }
    }

    pipe.exec();

    auto end = std::chrono::high_resolution_clock::now();



    std::chrono::duration<double> elapsed = end - start;

    std::cout << "Elapsed time single: " << elapsed.count() << " s\n";
    std::cout << "Writes per second single: " << nHashes*nFields/elapsed.count() << "\n";

    auto pipe2 = redis.pipeline();

    start = std::chrono::high_resolution_clock::now();

    // Repeat using multiwrite in hash
    for (int j = 0; j < nHashes; j++)
    {
        std::vector<std::pair<std::string, std::string>> fields;
        for (int i = 0; i < nFields; i++)
        {
            fields.push_back(std::make_pair("field"+std::to_string(i), "value"+std::to_string(i)));
        }
        
        pipe2.hmset("hash"+std::to_string(j), fields.begin(), fields.end());
    }

    pipe2.exec();

    end = std::chrono::high_resolution_clock::now();

    elapsed = end - start;

    std::cout << "Elapsed time multi: " << elapsed.count() << " s\n";
    std::cout << "Writes per second multi: " << nHashes*nFields/elapsed.count() << "\n";

    return 0;
}