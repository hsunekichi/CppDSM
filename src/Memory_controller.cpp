#pragma once

#include <memory>
#include <thread>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <shared_mutex>
//#include <Windows.h>
#include <chrono>
#include <optional>
#include <iostream>
#include <string>


#include "DB_connection.hpp"
#include "Redis_connection.cpp"



#pragma once


using Attrs = std::unordered_map<std::string, std::string>;

using Block = std::unordered_map<std::string, std::string>;	// A block contains a map of variable-value pairs
using StringPair = std::pair<std::string, std::string>;
using OptionalString = std::optional<std::string>;

using Item = std::pair<std::string, std::optional<Attrs>>;
using ItemStream = std::vector<Item>;

class Memory_controller
{
private:
    /****************************************** CONSTANTS ****************************************************/

    // Long long warrantees to have at least 64 bits. Redis uses 64 bits for the timestamp
    const unsigned long long MAX_SECONDARY_TIMESTAMP = 18446744073709551615ull; // Max timestamp for secondary indexes
    const int N_MINIMUM_CONNECTIONS = 1;            // Number of additional connections the cache needs internally
                                                    // 1 connection for the updater thread
    const unsigned int CLUSTER_N_SLOTS = 16384;

    std::string sepChar1 = ";";		// Separator between the stream name and the updates
    std::string sepChar2 = ",";		// Separator between the update id and the attributes
    int nConnections;               // Number of connections the DB object needs to hold concurrently. Has to be declared BEFORE the connection

    const std::string streamId = "stream:";				// Identifier for the streams
    const std::string lastUpdateId = "lastUpdateId";    // Identifier for the last update id
    bool USE_CACHE;
    bool CLUSTER_MODE;                      // True if the DB is a cluster
    unsigned int CACHE_REFRESH_RATE;        // Refresh rate for the cache (ms)
    bool ASYNC_UPDATE;                      // True if the cache updater is async
    
    std::unique_ptr<DB_connection> db;      // Connection with the db

    /******************************************** CACHE *****************************************************/

	std::unordered_set<std::string> cachedTypes;    // Set with all the cached block names
	
    std::shared_mutex blocksMtx;					// Mutex that protects all blocks

    //Map with all the blocks in the cache
    std::unordered_map<std::string, Block> blocks;	// Cached blocks
    std::mutex generalUpdaterMtx;                   // Mutex to prevent multiple updates at the same time
    std::mutex performingUpdateMtx;                 // Mutex to prevent updating while the buffer is writing to db and cache

    std::thread th_updater;				            // Thread that updates the blocks
    std::mutex updater_is_running;                  // Locked when the updater is running, prevents the updater from being started twice
    std::atomic<bool> finish_updater;               // Atomic bool to finish the updater thread

	std::mutex performanceMtx;			// Mutex to protect performance variables
	int hitCount, missCount;			// Performance measures

	double totalLoadBlockTime;			// Addition of all the block loading times
	int nBlocksLoaded;					// Number of blocks loaded
	double totalUpdateBlockTime;		// Addition of all the block updating times
	int	nBlocksUpdated;					// Number of blocks updated
    
    std::mutex global_mutex;            // Mutex for debugging purpouses


    int gettimeofday(struct timeval* tp, struct timezone* tzp) 
    {
        namespace sc = std::chrono;
        sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
        sc::seconds s = sc::duration_cast<sc::seconds>(d);
        tp->tv_sec = s.count();
        tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

        return 0;
    }

    
    // Function to add milliseconds to a timeval struct
    void addMillisecondsToTimeval(struct timeval& tv, int milliseconds) 
    {
        // Convert milliseconds to seconds and microseconds
        int secondsToAdd = milliseconds / 1000;
        int microsecondsToAdd = (milliseconds % 1000) * 1000;

        // Add seconds and microseconds to the timeval
        tv.tv_sec += secondsToAdd;
        tv.tv_usec += microsecondsToAdd;

        // Normalize the timeval in case of overflow
        if (tv.tv_usec >= 1000000) {
            tv.tv_sec += tv.tv_usec / 1000000;
            tv.tv_usec %= 1000000;
        }
    }

    // Define the < operator for timeval structs
    bool timevalLesser(const struct timeval& lhs, const struct timeval& rhs) 
    {
        if (lhs.tv_sec < rhs.tv_sec)
            return true;
        else if (lhs.tv_sec == rhs.tv_sec && lhs.tv_usec < rhs.tv_usec)
            return true;
        else
            return false;
    }
      
    
    uint16_t crc16_gpt(const char *buf, int len) 
    {
        uint16_t crc = 0;

        for (int i = 0; i < len; ++i) 
        {
            crc = crc ^ ((uint16_t)buf[i] << 8);

            for (int j = 0; j < 8; ++j) 
            {
                if (crc & 0x8000)
                    crc = (crc << 1) ^ 0x1021;
                else
                    crc <<= 1;
            }
        }

        return crc;
    }

    std::string getBlockFromStream(std::string streamName)
    {
        // Erases the initial "stream:" identifier from the block name
        return streamName.substr(streamId.size());
    }

    void printStreamsMap (std::unordered_map<std::string, ItemStream> streams_map)
    {
        // print streams_map
        std::cout << "streams map: " << std::endl;

        for (auto &stream : streams_map)
        {
            std::cout << stream.first << std::endl;

            for (auto &item : stream.second)
            {
                std::cout << "\t" << item.first << std::endl;

                for (auto &field : item.second.value())
                {
                    std::cout << "\t\t" << field.first << ": " << field.second << std::endl;
                }
            }
        }
    }

    std::string getStreamId (const std::string &streamId, const std::string &blockName)
    {
        return streamId+blockName;
    }

    // Auxiliar function to check whether the threads should finish atomicaly
	int numberOfCachedBlocks() 
	{
		std::shared_lock lock (blocksMtx);
		return blocks.size();
	}

    std::string increment_stream_timestamp(std::string timestamp)
    {
        int separator_position = timestamp.find("-");
        std::string s_first_timestamp = timestamp.substr(0, separator_position+1);
        std::string s_second_timestamp = timestamp.substr(separator_position+1);

        unsigned long long i_second_timestamp = std::stoull(s_second_timestamp);
        
        if (i_second_timestamp < MAX_SECONDARY_TIMESTAMP)
            i_second_timestamp++;
        else
            i_second_timestamp = 0;


        std::string temp = s_first_timestamp + std::to_string(i_second_timestamp);

        return temp;
    }

    std::vector<std::string> getOutdatedKeys(
            std::vector<std::string> keys, 
            std::unique_ptr<DB_pipeline> &pipe)
    {
        std::vector<std::string> outdatedKeys;

        for (auto &blockName : keys)    // Gets the last update id of each block
        {
            pipe->hget(blockName, lastUpdateId);
        }

        auto result = pipe->exec();


        std::shared_lock lock(blocksMtx);

        for (int i = 0; i < result->size(); i++)
        {
            OptionalString newId = result->getString(i);
            std::string v_blockName = keys[i];

            // The block is outdated
            if ((newId.has_value() && newId.value() != blocks[v_blockName][lastUpdateId])
                || !newId.has_value())      // The block does not exist anymore           
            {
                outdatedKeys.push_back(keys[i]);
            }
        }
        
        return outdatedKeys;
    }

    void updateOutdatedBlocks(
            std::vector<std::string> outdated_keys, // Virtual name of each outdated key
            std::unique_ptr<DB_pipeline> &pipe)
    {
        // Get the outdated blocks
        for (auto &key : outdated_keys)
        {
            pipe->hgetall(key);
        }

        // Prevents updating while the buffer is writing to db and cache
        //  To allow local sequential consistency
        performingUpdateMtx.lock();

        auto result = pipe->exec();

        // Updates the outdated blocks atomically

        blocksMtx.lock();
        for (int i = 0; i < result->size(); i++)        
        {
            auto new_block = result->getStringHashMap(i);

            if (new_block.size() > 0)
                blocks[outdated_keys[i]] = new_block;        // Updates the block
            else
                blocks.erase(outdated_keys[i]);          // Deletes the block
        }
        blocksMtx.unlock();

        performingUpdateMtx.unlock();
    }

    // Returns the names of the blocks cached in each node
    std::vector<std::vector<std::string>> groupCacheByNode()
    {
        std::vector<std::vector<std::string>>
                        keys_in_each_node;

        blocksMtx.lock_shared();
        for (auto &block_pair : blocks)             // Gets all cached block names
        {
            const std::string &blockName = block_pair.first;
            int node = db->getNodeOfKey(blockName);    // Gets the node of the block
            
            if (keys_in_each_node.size() <= node)   // If the node does not exist
                keys_in_each_node.resize(node+1);       // Creates it (resizes the vector)

            keys_in_each_node[node].push_back(blockName);
        }
        blocksMtx.unlock_shared();

        return keys_in_each_node;
    }


    OptionalString getCachedVariable (const std::string &blockName, const std::string &variableName)
    {
        OptionalString value;
        blocksMtx.lock_shared();
        auto it_block = blocks.find(blockName);   // Gets the block

        if (it_block != blocks.end())             // If the variable is cached
        {
            hitCount++;
            value = it_block->second[variableName]; // Gets the variable

            blocksMtx.unlock_shared();
        }
        else
        {
            blocksMtx.unlock_shared();

            bool wasLoaded = loadBlock(blockName); // Loads the new block

            if (wasLoaded)           
            {
                missCount++;

                blocksMtx.lock_shared();

                it_block = blocks.find(blockName);      // Gets the block
                value = it_block->second[variableName]; // Gets the variable

                blocksMtx.unlock_shared();
            }
            else {
                value = OptionalString();        // Returns empty optional, variable did not exist
            }
        }

        return value;
    }

    // Updates all the pending changes constantly
	void updaterThread()
	{   
        // Get time
        struct timeval nextUpdate, currentTime;

    	while (!finish_updater && numberOfCachedBlocks() > 0)
		{			
            gettimeofday(&currentTime, NULL);

            if (timevalLesser(nextUpdate, currentTime))
            {
			    updateCache();	// Processes pending updates, blocking some time when there are none

                // Add CACHE_REFRESH_RATE ms to nextUpdate
                gettimeofday(&nextUpdate, NULL);
                addMillisecondsToTimeval(nextUpdate, CACHE_REFRESH_RATE);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(std::min(CACHE_REFRESH_RATE, 1000u)));
        }
        
        updater_is_running.unlock();    // Allows a new updater to start
    }

    // Generates an item from an string, destroying the string in the process
    Item get_item_from_string(std::string &item_string)
    {
        int position1;
        std::string itemId;
        Attrs itemAttrs;

        // Gets the item id
        position1 = item_string.find(sepChar2);
        itemId = item_string.substr(0, position1);      // Gets the item id
        item_string = item_string.substr(position1+1);  // Deletes the item id from the string  

        while(item_string.size() > 0)
        {
            position1 = item_string.find(sepChar2);
            std::string key = item_string.substr(0, position1);      // Gets the key
            item_string = item_string.substr(position1+1);           // Deletes the key from the string

            position1 = item_string.find(sepChar2);
            std::string value = item_string.substr(0, position1);    // Gets the value
            item_string = item_string.substr(position1+1);           // Deletes the value from the string

            itemAttrs[key] = value;
        }

        return std::make_pair(itemId, itemAttrs);
    }

    // Generates an ItemStream map from the script result, deleting the original result
    std::unordered_map<std::string, ItemStream> get_stream_map_from_script (std::vector<std::string> &result)
    {
        std::unordered_map<std::string, ItemStream> streams_map;
        // Syntax: vector("streamName;itemId,key,value,key,value...;itemId,key,value...;...;)" 
        // Each element of the vector is a stream
        // ; is sepChar1 and , is sepChar2

        for (std::string& streamString : result)
        {
            std::string streamName;
            ItemStream itemStream;
            int position1;

            // Gets the stream name
            position1 = streamString.find(sepChar1);
            streamName = streamString.substr(0, position1);   // Gets the stream name
            streamString = streamString.substr(position1+1);        // Deletes the stream name from the string

            // Gets the items
            while (streamString.size() > 0)
            {
                std::string item_string;
                Item item;

                position1 = streamString.find(sepChar1);
                item_string = streamString.substr(0, position1);   // Gets the item string
                streamString = streamString.substr(position1+1);         // Deletes the item string from the string

                item = get_item_from_string(item_string);   // Gets the item attributes
                itemStream.push_back(item);
            }

            streams_map[streamName] = itemStream;
        }

        return streams_map;
    }

    sw::redis::ConnectionPoolOptions getPoolOptions()
    {
        sw::redis::ConnectionPoolOptions poolOptions;

        if (nConnections <= N_MINIMUM_CONNECTIONS)
        {
            std::cerr << "The number of cache connections must be greater or equal to 1\n";
            throw std::invalid_argument("The number of cache connections must be greater or equal to 1\n");
        }

        poolOptions.size = nConnections;

        return poolOptions;
    }

/******************************************************************************************/
/************************************* PUBLIC METHODS *************************************/
/******************************************************************************************/

public:
    Memory_controller(sw::redis::ConnectionOptions options, bool _USE_CACHE, 
                            bool _CLUSTER_MODE, unsigned int _CACHE_REFRESH_RATE, 
                            int nConcurrency=1) :
        nConnections(nConcurrency+N_MINIMUM_CONNECTIONS)
    {
        db = std::make_unique<Redis_connection>(options, getPoolOptions());

        CLUSTER_MODE = _CLUSTER_MODE;
        USE_CACHE = _USE_CACHE;
        CACHE_REFRESH_RATE = _CACHE_REFRESH_RATE;
        ASYNC_UPDATE = false;

        hitCount = 0;
        missCount = 0;
        totalLoadBlockTime = 0;
        nBlocksLoaded = 0;
        totalUpdateBlockTime = 0;
        nBlocksUpdated = 0; 


        finish_updater = false;
    }

    ~Memory_controller()
    {
        finish_updater = true;

        if (ASYNC_UPDATE && th_updater.joinable())  // If there is or there was an updating thread, wait for it to finish
        {
            th_updater.join();
        }
    }

    // Loads the block into cache, and returns whether it was loaded or not
    bool loadBlock(std::string blockName)
    {       
        auto init = std::chrono::high_resolution_clock::now();
        auto block = db->hgetall(blockName);

        if (block.size() > 0)   // If the block did exits
        {
            blocksMtx.lock();
            blocks[blockName].swap(block);  // Adds the block to the cache
            blocksMtx.unlock();

            auto end = std::chrono::high_resolution_clock::now();

            performanceMtx.lock();          // Updates performance variables
            totalLoadBlockTime += std::chrono::duration_cast<std::chrono::microseconds>(end - init).count();
            nBlocksLoaded++;
            performanceMtx.unlock();

            if (ASYNC_UPDATE && updater_is_running.try_lock())   // If the updater thread is not running, starts it
            {
                if (th_updater.joinable())  // Deletes the old thread if it exists
                    th_updater.join();

                th_updater = std::thread(&Memory_controller::updaterThread, this);
            }

            return true;
        }
        else {
            return false;
        }
    }

    bool loadBlocks(const std::vector<std::string> &blockNames)
    {
        auto keys_in_each_node = db->orderByNode(blockNames);
        int nLoaded = 0;

        for (const auto &keys_in_node : keys_in_each_node)  // For each node
        {
            if (keys_in_node.size() == 0)
                continue;

            const std::string &sampleKey = *keys_in_node.begin(); // Gets a sample key
            auto pipe = db->pipeline(sampleKey, false);  

            for (auto &key : keys_in_node)
            {
                pipe->hgetall(key);
            }

            auto result = pipe->exec();

            blocksMtx.lock();
            for (int i = 0; i < result->size(); i++)
            {
                auto block = result->getStringHashMap(i);

                if (block.size() > 0)
                {
                    blocks[keys_in_node[i]].swap(block);  // Adds the block to the cache
                    nLoaded++;
                }
            }
            blocksMtx.unlock();
        }

        return true;
    }

    bool isCached(const std::string& blockName)
    {
        bool isCached = false;

        if (USE_CACHE)
        {
            blocksMtx.lock_shared();
            bool isCached = blocks.find(blockName) != blocks.end();
            blocksMtx.unlock_shared();
        }

        return isCached;
    }

    OptionalString readVarFromCache(const std::string& blockName, const std::string &varName)
    {
        OptionalString value;   

        if (USE_CACHE)
        {
            blocksMtx.lock_shared();
            auto block = blocks.find(blockName);

            if (block != blocks.end())
            {   
                hitCount++;
                value = block->second[varName];
            }
            
            blocksMtx.unlock_shared();
        }

        return value;
    }

    void lock_memory_updating()
    {
        performingUpdateMtx.lock();
    }

    void unlock_memory_updating()
    {
        performingUpdateMtx.unlock();
    }

    void write_data_to_cache(std::vector<std::string> &keys, std::vector<std::string> &args)
    {
        if (USE_CACHE)
        {
            int i_keys = 0;
            int i_args = 0;

            std::unique_lock lock (blocksMtx);

            for ( ; i_keys > keys.size(); i_keys++, i_args++)
            {
                std::string blockName = keys[i_keys];
                int n_vars = std::stoi(args[i_args]);
                i_args++;

                for (int i = 0; i < n_vars; i++)
                {
                    std::string varName = args[i_args];
                    std::string value = args[i_args+1];
                    i_args += 2;

                    if (USE_CACHE && blocks.find(blockName) != blocks.end())
                    {
                        blocks[blockName][varName] = value;
                    }
                }
            }
        }
    }

    bool write_operations_to_cache(std::vector<DB_operation> operations)
    {
        std::unique_lock lock (blocksMtx);

        for (auto operation : operations)
        {
            auto &op = operation.op;
            auto &blockName = operation.param[0];
            auto &varName = operation.param[1];
            auto &value = operation.param[2]; 

            if (op == DB_opCode::hset) {
                blocks[blockName][varName] = value;
            }
            else if (op == DB_opCode::hsetnx) 
            {
                // Writes it if the variable does not exist, 
                //   to mantain TSO
                if (blocks[blockName].find(varName) 
                        == blocks[blockName].end()) 
                {
                    blocks[blockName][varName] = value;
                }
            }
            else if (op == DB_opCode::hdel) {
                blocks[blockName].erase(varName);
            }
            else if (op == DB_opCode::del) {
                blocks.erase(blockName);
            }
        }

        return true;
    }

    bool writeIfCached(const std::string& blockName, const std::string &varName, const std::string &value)
    {
        bool isCached = false;

        if (USE_CACHE)
        {
            blocksMtx.lock();
            auto block = blocks.find(blockName);

            if (block != blocks.end())
            {
                isCached = true;
                block->second[varName] = value;
            }
            blocksMtx.unlock();
        }

        return isCached;
    }

    bool deleteIfCached (const std::string& blockName, const std::string &varName)
    {
        bool isCached = false;

        if (USE_CACHE)
        {
            blocksMtx.lock();
            auto block = blocks.find(blockName);

            if (block != blocks.end())
            {
                isCached = true;
                block->second.erase(varName);
            }
            blocksMtx.unlock();
        }

        return isCached;
    }

    bool deleteBlock(const std::string& blockName)
    {
        bool isCached = false;

        if (USE_CACHE)
        {
            blocksMtx.lock();
            auto block = blocks.find(blockName);

            if (block != blocks.end())
            {
                isCached = true;
                blocks.erase(block);
            }
            blocksMtx.unlock();
        }

        return isCached;
    }

    void writeVariable(const std::string& blockName, const std::string &varName, const std::string &value)
    {
        blocksMtx.lock();

        if (blocks.find(blockName) == blocks.end())
        {   
            blocksMtx.unlock();
            loadBlock(blockName);
            blocksMtx.lock();
        }   

        blocks[blockName][varName] = value;
        blocksMtx.unlock();
    }

    void deleteVariable(const std::string& blockName, const std::string &varName)
    {
        blocksMtx.lock();
        blocks[blockName].erase(varName);
        blocksMtx.unlock();
    }

    // Updates the cache completely, and returns whether there were any updates or not
    bool updateCache()
    {
        if (USE_CACHE)
        {
            std::unique_lock lock(generalUpdaterMtx);   // Prevents multiple updates at the same time
            
            auto keys_in_each_node = groupCacheByNode();

            for (auto &keys_in_node : keys_in_each_node)  // For each node
            {
                if (keys_in_node.size() > 0)
                {
                    const std::string &sampleKey = *keys_in_node.begin(); // Gets a sample key

                    auto pipe = db->pipeline(sampleKey, false);  

                    // Get all the last update ids of each block
                    auto outdated_keys = getOutdatedKeys(keys_in_node, pipe);

                    if (outdated_keys.size() == 0)       // If there are no outdated blocks, return
                        return false;

                    updateOutdatedBlocks(outdated_keys, pipe); // Updates the outdated blocks
                }
            }
            
            return true;
        }
        else {
            return false;
        }
    }

    // Deletes all cached blocks, and returns whether there were any or not
    bool invalidate_cache()
    {
        if (USE_CACHE)
        {
            blocksMtx.lock();
            blocks.clear();
            blocksMtx.unlock();

            return true;
        }
        else {
            return false;
        }
    }

    // Gets the value of a variable, either from the cache or from the database.
    // Loads the block on read miss
    // Returns an empty optional if the variable does not exist
    // local_useCache forces disable the cache for this operation, for sync operations
    OptionalString getVariable(const std::string &blockName, const std::string &variableName)
    {
        OptionalString value;

        if (USE_CACHE) {
            value = getCachedVariable(blockName, variableName);
        }
        else {
            value = db->hget(blockName, variableName);
        }

        return value;
    }

    bool preload(const std::string &key)
    {
        if (USE_CACHE)
        {
            std::shared_lock lock(blocksMtx);

            if (blocks.find(key) != blocks.end())
                return true;

            lock.unlock();

            return loadBlock(key);
        }
        else {
            return false;
        }
    }

    bool preload(const std::vector<std::string> &keys)
    {
        if (USE_CACHE)
        {
            std::vector<std::string> keysToLoad;
            std::shared_lock lock(blocksMtx);

            for (const auto &key : keys)
            {                
                if (blocks.find(key) == blocks.end())
                {
                    keysToLoad.push_back(key);
                }
            }

            lock.unlock();

            return loadBlocks(keysToLoad);
        }
        else {
            return false;
        }
    }

    std::string wait_event(const std::string &channel, int timeout=0)
    {
        return db->consume_message(channel);
    }

    std::pair<std::string, std::string> getVariableExclusive (
                                            std::string blockName,
                                            const  std::string &variableName)
    {
        auto result = db->exclusive_hget({blockName}, {variableName});
        return std::make_pair(result[0], result[1]);
    }

    int getHitCount()
    {
        return hitCount;
    }

    int getMissCount()
    {
        return missCount;
    }

    double getHitRatio()
    {
        return (double)hitCount / (double)(hitCount + missCount);
    }

    double getMissRatio()
    {
        return (double)missCount / (double)(hitCount + missCount);
    }

    // In microseconds
    double getBlockAvgTime()
    {
        return totalLoadBlockTime / nBlocksLoaded;
    }

    int getCacheElementCount()
    {
        return blocks.size();
    }

    // Returns the number of chars stored in the cache (~= byte size).
    int getCacheMemorySize()
    {
        int size = 0;

        std::shared_lock lock(blocksMtx);

        for (auto it = blocks.begin(); it != blocks.end(); it++)    // For each block
        {
            for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) // For each variable
            {
                size += it2->first.size() + it2->second.size();                     // Adds each variable's size
            }
        }

        return size;
    }

    void printCache()
    {
        std::shared_lock lock(blocksMtx);

        for (auto it = blocks.begin(); it != blocks.end(); it++)    // For each block
        {
            std::cout << it->first << std::endl;

            for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) // For each variable
            {
                std::cout << "\t" << it2->first << ": " << it2->second << std::endl;
            }
        }
    }

    void printBlock(std::string blockName, std::unordered_map<std::string, std::string> &block)
    {
        std::cout << blockName << std::endl;

        for (auto it2 = block.begin(); it2 != block.end(); it2++) // For each variable
        {
            std::cout << "\t" << it2->first << ": " << it2->second << std::endl;
        }

        std::cout << std::endl;
    }
    
	OptionalString get(const std::string &blockName)
	{
		return db->get(blockName);
	}


    bool sismember(const std::string &param1, const std::string &param2)
	{
        return db->sismember(param1, param2);
    }

	long long smembers(const std::string &param1,
            std::unordered_set<std::string> &out)
	{
        return db->smembers(param1, out);
    }

    long long smembers(const std::string &param1,
            std::set<std::string> &out)
	{
        return db->smembers(param1, out);
    }

	long long scard(const std::string &param1)
	{
        return db->scard(param1);
    }    

    std::string ping()
    {
        return db->ping();
    }
};

