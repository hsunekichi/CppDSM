#pragma once

#include <string>
#include <memory>
#include <thread>


#include "../DB_cache.cpp"



class Distributed_hashmap
{
    std::string map_id;
    std::shared_ptr<DB_cache> pcache;

public:

    Distributed_hashmap(std::string _map_id, std::shared_ptr<DB_cache> _pcache)
    {
        pcache = _pcache;
        map_id = _map_id;
    }

    bool insert(std::string key, std::string value)
    {
        pcache->hsetnx(map_id, key, value);
        return true;
    }

    bool erase(std::string key)
    {
        pcache->hdel(map_id, key);
        return true;
    }

    std::string at(std::string key)
    {
        auto val = pcache->hget(map_id, key);

        if (val)
            return val.value();
        else
            throw std::out_of_range("Key not found");
    }

    bool exists(std::string key)
    {
        auto val = pcache->hget(map_id, key);
        return val.has_value();
    }

    bool set(std::string key, std::string value)
    {
        pcache->hset(map_id, key, value);
        return true;
    }
};


