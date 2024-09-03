#include <memory>
#include <thread>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include "redis++.h"
#include <shared_mutex>
#include <Windows.h>

using Attrs = std::unordered_map<std::string, std::string>;

using Block = std::unordered_map<std::string, std::string>;	// A block contains a map of variable-value pairs
using Pipe = sw::redis::QueuedRedis<sw::redis::PipelineImpl>;	// Pipe type
using StringPair = std::pair<std::string, std::string>;
using OptionalString = std::optional<std::string>;

using Item = std::pair<std::string, std::optional<Attrs>>;
using ItemStream = std::vector<Item>;

class Redis_memory_controller
{
private:
    /****************************************** CONSTANTS ****************************************************/

    // Long long warrantees to have at least 64 bits. Redis uses 64 bits for the timestamp
    const unsigned long long REDIS_MAX_SECONDARY_TIMESTAMP = 18446744073709551615ull; // Max timestamp for redis secondary indexes
    const int N_MINIMUM_CONNECTIONS = 1;            // Number of additional connections the cache needs internally
                                                    // 1 connection for the updater thread
    const unsigned int REDIS_CLUSTER_N_SLOTS = 16384;

    std::string sepChar1 = ";";		// Separator between the stream name and the updates
    std::string sepChar2 = ",";		// Separator between the update id and the attributes
    std::string noScriptError = "NOSCRIPT";    // Error returned by redis when the script is not cached
    int nConnections;               // Number of connections the redis object needs to hold concurrently. Has to be declared BEFORE the connection

    const std::string streamId = "stream:";				// Identifier for the streams
    const std::string lastUpdateId = "lastUpdateId";    // Identifier for the last update id
    bool USE_CACHE;
    bool CLUSTER_MODE;                      // True if the redis is a cluster
    unsigned int CACHE_REFRESH_RATE;        // Refresh rate for the cache (ms)
    


    std::optional<sw::redis::Redis> redisConnection;	
    std::optional<sw::redis::RedisCluster> redisCluster;

    /******************************************* SCRIPTS ****************************************************/
    
    // Script to recieve ALL the pending updates. 
    // Warrantees a coherent state up to this point, blocking other writes meanwhile.
    /*
    std::string script_get_all_updates = 
        "local nKeys = #(KEYS) "    // Number of streams to check
        "local result = {} "        // Resulting table with all the updates
        "local iStream = 1 "

        "for i = 1, nKeys, 2 do "       // For each stream
            "local streamName = KEYS[i] "       // Stream name
            "local lastUpdateId = KEYS[i+1] "   // Last id updated
            "local streamString = streamName .. '"+sepChar1+"' "
            "local updates = redis.call('XRANGE', streamName, lastUpdateId, '+') " // Gets all the pending changes in the stream

            "if #(updates) > 0 then "           // If there are updates
                "for iItem = 1, #(updates) do " // For each update
                    "local item = updates[iItem] " // Get the item
                    "local itemId = item[1] "      // Get the item id
                    "local itemAttrs = item[2] "   // Get the attributes of the item

                    "streamString = streamString .. itemId " // Add the id to the stream string

                    "local itemVars = {} "         // Table with the variables of the update
                    "for iAttr = 1, #(itemAttrs), 2 do " // For each attribute
                        "streamString = streamString .. '"+sepChar2+"' "
                                        ".. itemAttrs[iAttr] .. '"+sepChar2+"' "
                                        ".. itemAttrs[iAttr+1] " // Add the attribute to the stream string
                    "end "

                    "streamString = streamString .. '"+sepChar2+"' .. '"+sepChar1+"' " // Add the end of the update to the stream string
                "end "

                "result[iStream] = streamString " // Add the stream string to the result
                "iStream = iStream + 1 "
            "end "
        "end "
        "return result ";
    */

    const std::string script_exclusive_hget = 
        "local value = redis.call ('HGET', KEYS[1], ARGV[1]);"
        "local exclusive_timestamp = redis.call('HGET', KEYS[1], '"+lastUpdateId+"');"
        "return {value, exclusive_timestamp};";
                 
    //std::string sha_get_all_updates = "";   // Sha to cache the script above
    
    /******************************************** CACHE *****************************************************/

	std::unordered_set<std::string> cachedTypes;    // Set with all the cached block names
	
    std::shared_mutex blocksMtx;					// Mutex that protects all blocks
    std::unordered_map<std::string, Block> blocks;	// Map with all the blocks in the cache
    std::unordered_map<
            int,
            std::unordered_map<
                std::string,
                std::string>> cachedBlocks;         // Map with all the cached block names and its streams ids
    bool discard_weak_update;                       // Discards the next weak update to force it to update the last ids
    std::mutex strong_update_mtx;                   // Mutex to prevent multiple strong updates at the same time

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


    int gettimeofday(struct timeval* tp, struct timezone* tzp) {
        namespace sc = std::chrono;
        sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
        sc::seconds s = sc::duration_cast<sc::seconds>(d);
        tp->tv_sec = s.count();
        tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

        return 0;
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

    // Function to calculate CRC16 of a string, works like redis implementation
    uint16_t getKeyClusterSlot(std::string key)
    {
        // Gets the string between { and }
        std::size_t pos1 = key.find("{");
        std::size_t pos2 = key.find("}");

        if (pos1 != std::string::npos && pos2 != std::string::npos)
            key = key.substr(pos1+1, pos2-pos1-1);

        return crc16_gpt(key.c_str(), key.size()) % 16384;
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

    sw::redis::ConnectionPoolOptions getPoolOptions()
    {
        sw::redis::ConnectionPoolOptions poolOptions;

        if (nConnections < 1+N_MINIMUM_CONNECTIONS)
        {
            std::cerr << "The number of cache connections must be greater or equal to 1\n";
            throw std::invalid_argument("The number of cache connections must be greater or equal to 1\n");
        }

        poolOptions.size = nConnections;

        return poolOptions;
    }

    // Auxiliar function to check whether the threads should finish atomicaly
	int numberOfCachedBlocks() 
	{
		std::shared_lock lock (blocksMtx);
		return cachedBlocks.size();
	}

    std::string increment_stream_timestamp(std::string timestamp)
    {
        int separator_position = timestamp.find("-");
        std::string s_first_timestamp = timestamp.substr(0, separator_position+1);
        std::string s_second_timestamp = timestamp.substr(separator_position+1);

        unsigned long long i_second_timestamp = std::stoull(s_second_timestamp);
        
        if (i_second_timestamp < REDIS_MAX_SECONDARY_TIMESTAMP)
            i_second_timestamp++;
        else
            i_second_timestamp = 0;


        std::string temp = s_first_timestamp + std::to_string(i_second_timestamp);

        return temp;
    }

    void updateAttribute(const std::string &blockName, Block &block, const std::pair<std::string, std::string> &attr)
    {
        if (attr.second != "DELETED") {    // Variable updated
            block[attr.first] = attr.second;
        }
        else                               // Deleted
        {
            int slot = getKeyClusterSlot(blockName);

            if (attr.first == blockName)        // Block deleted
            {   
                blocks.erase(blockName);
                cachedBlocks[slot].erase(getStreamId(streamId, blockName));
            }
            else                                // Variable deleted     
            {
                block.erase(attr.first);

                if (block.size() == 1) // Block empty, only has the lastUpdateId value
                {
                    blocks.erase(blockName);
                    cachedBlocks[slot].erase(getStreamId(streamId, blockName));
                }
            }
        }
    }

    void updateBlock(const ItemStream& stream, const std::string &blockName)
    {
        for (const Item& item : stream)                 // For each update
        {
            std::string updateId = item.first;
            const Attrs& attrs = item.second.value();

            int slot = getKeyClusterSlot(blockName);
            cachedBlocks[slot][getStreamId(streamId, blockName)] = updateId;     // Updates the id of the last recorded block modification 
            blocks[blockName][lastUpdateId] = updateId;      // Updates the id of the last recorded block modification
            
            Block& block = blocks[blockName];

            for (auto& attr : attrs)                        // For each attribute in the update
            {
                updateAttribute(blockName, block, attr);
            }
        }
    }

    bool updateStreams(const std::unordered_map<std::string, ItemStream> &result)
    {
        if (result.size() > 0)
        {
            blocksMtx.lock();

            if (!discard_weak_update)
            {   
                for (const std::pair<std::string, ItemStream> &pair : result)   // For each recieved block update
                {
                    const ItemStream& stream = pair.second;
                    std::string blockName = pair.first;
                    
                    blockName = getBlockFromStream(blockName);                 
                    updateBlock(stream, blockName); // Updates the block
                }
            }
            else
            {
                discard_weak_update = false;        // Discards the first weak update after a strong update, to prevent old values from being loaded
            }

            blocksMtx.unlock();

            return true;
        }
        else {
            return false;
        }
    }

    sw::redis::QueuedReplies getBlockTimestamps(std::vector<std::string> &keys,
                                    std::unordered_map<std::string, std::string> &slot,
                                    Pipe &pipe)
    {
        int i = 0;
        for (auto &block : slot)    // Gets the last update id of each block
        {
            keys[i] = getBlockFromStream(block.first);
            pipe.hget(keys[i], lastUpdateId);
            i++;
        }

        return pipe.exec();
    }

    std::vector<std::string> filterOutdatedBlocks(const std::vector<std::string> &keys,
                                std::unordered_map<std::string, std::string> &slot, 
                                sw::redis::QueuedReplies &result)
    {
        std::vector<std::string> outdatedKeys;

        for (int i = 0; i < result.size(); i++)
        {
            OptionalString newId = result.get<std::string>(i);

            // The block is outdated
            if (newId.has_value() && newId.value() != slot[keys[i]])               
            {
                outdatedKeys.push_back(keys[i]);
            }
            else if (!newId.has_value()) // The block does not exist anymore
            {
                int slot_id = getKeyClusterSlot(keys[i]);

                blocksMtx.lock();        // Deletes the block from the cache
                blocks.erase(keys[i]);
                cachedBlocks[slot_id].erase(keys[i]);
                blocksMtx.unlock();
            }
        }
        
        return outdatedKeys;
    }

    void updateOutdatedBlocks(std::vector<std::string> &outdatedKeys,
                                std::vector<std::string> &keys, 
                                Pipe &pipe)
    {
        // Get the outdated blocks
        for (auto &key : outdatedKeys)
        {
            pipe.hgetall(key);
        }

        auto result = pipe.exec();
        

        // Updates the outdated blocks atomically

        blocksMtx.lock();
        for (int i = 0; i < result.size(); i++)        
        {
            auto block = result.get<std::unordered_map<std::string, std::string>>(i);
            blocks[keys[i]] = block;                    // Updates the block
        }

        discard_weak_update = true;
        blocksMtx.unlock();
    }

    bool updateSlot(std::unordered_map<std::string, std::string> &slot)
    {
        std::vector<std::string> keys(slot.size()); // Array to order the keys temporarily    
        std::vector<std::string> outdatedKeys;
        std::optional<Pipe> pipe;       
        
        timeval t1, t2;

        if (CLUSTER_MODE)
        {
            const std::string &sampleKey = (*slot.begin()).first;
            pipe = redisCluster->pipeline(sampleKey, false);
        }
        else {
            pipe = redisConnection->pipeline(false);
        }


        // Get all the last update ids of each block
        auto result = getBlockTimestamps(keys, slot, *pipe);
        outdatedKeys = filterOutdatedBlocks(keys, slot, result);

        if (outdatedKeys.size() == 0)       // If there are no outdated blocks, return
            return false;

        updateOutdatedBlocks(outdatedKeys, keys, *pipe); // Updates the outdated blocks
        
        return true;
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
    	while (!finish_updater && numberOfCachedBlocks() > 0)
		{			
			strong_update();	// Processes pending updates, blocking some time when there are none
            std::this_thread::sleep_for(std::chrono::milliseconds(CACHE_REFRESH_RATE));
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

    /*
    std::vector<std::string> execute_script_get_all_updates(const std::vector<std::string> &keys, 
                                        const std::vector<std::string> &args)
    {
        std::vector<std::string> result;

        if (sha_get_all_updates == "")  // If the script still has not been loaded
            sha_get_all_updates = redisConnection.script_load(script_get_all_updates);  // Loads the script

        bool executed = false;
        while (!executed)       // Retries until the script is cached and executed
        {
            try {
                redisConnection.evalsha(sha_get_all_updates, 
                                            keys.begin(), keys.end(), 
                                            args.begin(), args.end(), 
                                            std::back_inserter(result));
                executed = true;
            }
            catch (sw::redis::ReplyError &e)
            {
                std::string suberror = e.what();
                suberror = suberror.substr(0, noScriptError.size());

                if (suberror == noScriptError)
                    sha_get_all_updates = redisConnection.script_load(script_get_all_updates);  // Reloads the script
                else
                    throw e;
            }
        }
        
        //redisConnection.eval(script_get_all_updates, 
        //                    keys.begin(), keys.end(), 
        //                    args.begin(), args.end(), 
        //                    std::back_inserter(result));
        


        return result;
    }
    */

/******************************************************************************************/
/************************************* PUBLIC METHODS *************************************/
/******************************************************************************************/

public:
    Redis_memory_controller(sw::redis::ConnectionOptions options, bool _USE_CACHE, 
                            bool _CLUSTER_MODE, unsigned int _CACHE_REFRESH_RATE, int nConcurrency=1) :
        nConnections(nConcurrency+N_MINIMUM_CONNECTIONS)
    {
        CLUSTER_MODE = _CLUSTER_MODE;
        USE_CACHE = _USE_CACHE;
        CACHE_REFRESH_RATE = _CACHE_REFRESH_RATE;

        hitCount = 0;
        missCount = 0;
        totalLoadBlockTime = 0;
        nBlocksLoaded = 0;
        totalUpdateBlockTime = 0;
        nBlocksUpdated = 0; 

        finish_updater = false;
        discard_weak_update = false;

        if (CLUSTER_MODE)
            redisCluster = sw::redis::RedisCluster(options, getPoolOptions());
        else
            redisConnection = sw::redis::Redis(options, getPoolOptions());
    }

    ~Redis_memory_controller()
    {
        finish_updater = true;

        if (th_updater.joinable())  // If there is or there was an updating thread, wait for it to finish
        {
            th_updater.join();
        }
    }

    // Loads the block into cache, and returns whether it was loaded or not
    bool loadBlock(std::string blockName)
    {
        std::unordered_map<std::string, std::string> block;
        
        if (CLUSTER_MODE)
            redisCluster->hgetall(blockName, std::inserter(block, block.end()));
        else
            redisConnection->hgetall(blockName, std::inserter(block, block.end()));
    

        if (block.size() > 0)   // If the block did exits
        {
            int slot = getKeyClusterSlot(blockName);  // Gets the slot of the block

            blocksMtx.lock();
            blocks[blockName] = block;  // Adds the block to the cache
            cachedBlocks[slot][getStreamId(streamId, blockName)] = block[lastUpdateId]; // Adds the block to the cached blocks
            blocksMtx.unlock();

            performanceMtx.lock();          // Updates performance variables
            totalLoadBlockTime += 0;        // TODO: Add time
            nBlocksLoaded++;
            performanceMtx.unlock();

            if (updater_is_running.try_lock())   // If the updater thread is not running, starts it
            {
                if (th_updater.joinable())  // Deletes the old thread if it exists
                    th_updater.join();

                th_updater = std::thread(&Redis_memory_controller::updaterThread, this);
            }


            return true;
        }
        else {
            return false;
        }
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
            else {
                missCount++;
            }
            
            blocksMtx.unlock_shared();
        }

        return value;
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

    void writeVariable(const std::string& blockName, const std::string &varName, const std::string &value)
    {
        blocksMtx.lock();
        blocks[blockName][varName] = value;
        blocksMtx.unlock();
    }

    void deleteBlock(const std::string& blockName)
    {
        blocksMtx.lock();
        blocks.erase(blockName);
        blocksMtx.unlock();
    }

    void deleteVariable(const std::string& blockName, const std::string &varName)
    {
        blocksMtx.lock();
        blocks[blockName].erase(varName);
        blocksMtx.unlock();
    }


    // Updates some of the outdated blocks, and returns whether there were any updates or not
    // DOES NOT WORK WITH CURRENT NOTIFICATION IMPLEMENTATION, USE STRONG UPDATE INSTEAD
    /*
    bool weak_update()
    {
        blocksMtx.lock_shared();
        std::unordered_map<int, std::unordered_map<std::string, std::string>> blockNames = cachedBlocks; // Blocks to update
        blocksMtx.unlock_shared();

        std::unordered_map<std::string, ItemStream> result;

        int nPages = blockNames.size();

        for (auto &pair : blockNames)   // Gets each page updates
        {
            auto &page = pair.second;    

            // Needs a polling time so that the thread can be stopped, and the blockNames updated
            if (CLUSTER_MODE)
                redisCluster->xread(page.begin(), page.end(), std::chrono::milliseconds(CACHE_REFRESH_RATE/nPages), 1000, std::inserter(result, result.end()));
            else
                redisConnection->xread(page.begin(), page.end(), std::chrono::milliseconds(CACHE_REFRESH_RATE/nPages), 1000, std::inserter(result, result.end()));
        }

        return updateStreams(result);       // Updates the blocks
    }
    */

    // Updates the cache completely, and returns whether there were any updates or not
    bool strong_update()
    {
        if (USE_CACHE)
        {
            strong_update_mtx.lock();
         
            blocksMtx.lock_shared();
            std::unordered_map<
                int,
                std::unordered_map<
                    std::string,
                    std::string>> blockNames = cachedBlocks; // Blocks to update
            blocksMtx.unlock_shared();

            for (auto &pair : blockNames)
            {
                auto &slot = pair.second;
                updateSlot(slot);
            }

            strong_update_mtx.unlock();
            return true;
        }
        else {
            return false;
        }
    }

    // Deletes all cached blocks, and returns whether there were any or not
    bool reset_cache()
    {
        if (USE_CACHE)
        {
            blocksMtx.lock();
            blocks.clear();
            cachedBlocks.clear();
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
        else
        {
            if (CLUSTER_MODE)
                value = redisCluster->hget(blockName, variableName);
            else
                value = redisConnection->hget(blockName, variableName);
        }

        return value;
    }

    std::pair<std::string, std::string> getVariableExclusive (
                                            const std::string &blockName,
                                            const  std::string &variableName)
    {
        std::vector<std::string> result;

        if (CLUSTER_MODE)
        {
            result = redisCluster->eval<std::vector<std::string>>(
                            script_exclusive_hget,
                            {blockName},
                            {variableName}
                        );
        }
        else
        {
            result = redisConnection->eval<std::vector<std::string>>(
                            script_exclusive_hget,
                            {blockName},
                            {variableName}
                        );
        }

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

    
	OptionalString get(const std::string &blockName)
	{
		if (CLUSTER_MODE)
            return redisCluster->get(blockName);
        else
            return redisConnection->get(blockName);
	}


    bool sismember(const std::string &param1, const std::string &param2)
	{
        if (CLUSTER_MODE)
            return redisCluster->sismember(param1, param2);
        else
            return redisConnection->sismember(param1, param2);
    }

    template<typename Output>
	long long sscan(const std::string &param1, long long cursor, int nElems, Output out)
	{
        if (CLUSTER_MODE)
            return redisCluster->sscan(param1, cursor, nElems, out);
        else
            return redisConnection->sscan(param1, cursor, nElems, out);
    }

	long long scard(const std::string &param1)
	{
        if (CLUSTER_MODE)
            return redisCluster->scard(param1);
        else
            return redisConnection->scard(param1);
    }    

    std::string ping()
    {
        if (CLUSTER_MODE)
            return redisCluster->redis("").ping();
        else
            return redisConnection->ping();
    }
};

