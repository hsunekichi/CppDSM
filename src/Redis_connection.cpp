#pragma once 

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <string>
#include <shared_mutex>
#include "redis++.h"

#include "DB_connection.hpp"

#pragma once

class Redis_replies : public DB_replies
{
    protected:
    sw::redis::QueuedReplies replies;

    public:

    Redis_replies(sw::redis::QueuedRedis<sw::redis::PipelineImpl> &pipe) 
    {
        replies = pipe.exec();
    }

    std::optional<std::string> getString (int index) override
    {
        return replies.get<std::optional<std::string>>(index);
    }

    std::vector<std::string> getStringVector (int index) override
    {
        return replies.get<std::vector<std::string>>(index);
    }

    std::unordered_map<std::string, std::string> getStringHashMap (int index) override
    {
        return replies.get<std::unordered_map<std::string, std::string>>(index);
    }

    size_t size() override
    {
        return replies.size();
    }
};



class Redis_connection : public DB_connection 
{
protected:
    sw::redis::ConnectionOptions connection_options;                    // Options for new connections

    unsigned int CLUSTER_SIZE;                                          // Number of nodes in the cluster
    int CLUSTER_MODE = -1;

    std::optional<sw::redis::RedisCluster> redisCluster;
    std::optional<sw::redis::RedisCluster> subscriberCluster;
    std::optional<sw::redis::Redis> redis;
    std::optional<sw::redis::Redis> subscriberRedis;

    std::vector<std::string> sample_cluster_keys;	                    // Map of node id to sample key for each node in the cluster
    std::shared_mutex mtx_sample_cluster_keys;	                        // Mutex for the sample_cluster_keys map


    /****************************************************************/
    /************************* CONSTANTS ****************************/

    inline static const std::string lastUpdateId = "lastUpdateId";    // Identifier for the last update id
    inline static const std::string script_exclusive_hget = 
        "return redis.call ('HMGET', KEYS[1], ARGV[1], '"+lastUpdateId+"');";
    
    inline static const std::string script_write_merge_buffer =  
        "local return_value = 1;" 
        "local lastUpdateId = '"+lastUpdateId+"';"
        "local batch_size = 1000;"                      // Number of max writes per batch, limited by lua "unpack" function
        "local i_args_init = 1;"                        // Iterator for the ARGV array
        
        // Timestamp in us, since scripts are atomic the same stamp can be used for all the writes
        "local time = redis.call('TIME')"
        "local newId = tonumber(time[1])*1000000 + tonumber(time[2]);"  

        "for i_block = 1, #KEYS, 1 do "                 // For each block
            "local nKeys = ARGV[i_args_init];"
            "i_args_init = i_args_init+1;"
            "local i_args_end = i_args_init+(nKeys-1)*2;"

            "local block_key = KEYS[i_block];"             
            "local stream_key = KEYS[i_block+1];"
            
            // For each batch
            "for init_batch = i_args_init, i_args_end, batch_size*2 do "    
                "local end_batch = math.min(i_args_end, init_batch + ((batch_size-1)*2));"

                "local ind = 1;"
                "local values = {};"
                "values[end_batch-init_batch] = ' ';"       // Preallocates memory
                
                "for i = init_batch, end_batch, 2 do "      // For each key
                    "values[ind] = ARGV[i];"                // Adds the key and value to the batch data
                    "values[ind+1] = ARGV[i+1];"
                    "ind = ind + 2;"
                "end "
                
                "redis.call('HSET', block_key, lastUpdateId, newId, unpack(values));"
            "end;"

            "i_args_init = i_args_end+2;"
        "end;"
        
        "return return_value";

    inline static const std::string coherency_hset =  
            "local time = redis.call('TIME');"
            "local newId = tonumber(time[1])*1000000 + tonumber(time[2]);"  // Timestamp in us
            "return redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], '"+lastUpdateId+"', newId);";

    inline static const std::string coherency_hsetnx = 
            "local time = redis.call('TIME');"
            "local newId = tonumber(time[1])*1000000 + tonumber(time[2]);"  // Timestamp in us
            "redis.call('HSETNX', KEYS[1], '"+lastUpdateId+"', newId);"
            "return redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]);";

    inline static const std::string coherency_hdel =  
            "local time = redis.call('TIME');"
            "local newId = tonumber(time[1])*1000000 + tonumber(time[2]);"  // Timestamp in us
            "redis.call('HDEL', KEYS[1], ARGV[1]);"
            "return redis.call('HSET', '"+lastUpdateId+"', newId)";

    inline static const std::string coherency_del =   
            "return redis.call('DEL', KEYS[1]);";

    inline static const std::string exclusive_release_script = 
            // Gets the exclusive access timestamp
			"local current_timestamp = redis.call('HGET', KEYS[1], '"+lastUpdateId+"');"
            "local time = redis.call('TIME');"
            "local newId = tonumber(time[1])*1000000 + tonumber(time[2]);"  // Timestamp in us
			
            // If the variable exclusive timestamp has not changed 
            //      and at least a microsecond has passed (to warrant there is a new timestamp)
            "if ARGV[3] == current_timestamp and current_timestamp ~= newId "             
			"then "
                // Performs the operation
                "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], '"+lastUpdateId+"', newId);"
                "return 1 "
			"else "
				"return 0 "
			"end";

    // Returns the crc16 of a string. 
	// This code works like the one in redis, most crc16 implementations on the internet do not.
	// I don't know the mathematical difference, but this is the one that works (courtesy of chatGPT)
	static uint16_t crc16_gpt(const char *buf, int len) 
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

    // Generates one sample key for each node in the cluster, and returns the map
	void generateNodeKeys()
	{
        if (cluster_mode())
        {
            auto ranges = getClusterSlotRanges_nodesCommand();

            for (auto node_ranges : ranges)
            {
                int node_sample_slot = node_ranges[0].first;	// First slot in the range
                std::string example_key = generateKeyForSlot(node_sample_slot);

                sample_cluster_keys.push_back(example_key);	
            }
        }
        else {
            sample_cluster_keys.push_back("0");
        }
	}

    std::vector<std::vector<std::pair<int, int>>> getClusterSlotRanges_nodesCommand() 
	{
		std::vector<std::vector<std::pair<int, int>>> slotRanges;

		// TODO: This command should be "CLUSTER SHARDS", 
		// 		as redis recommends not to use CLUSTER NODES to get the slot ranges, I don't know why.
		// 		This is only executed for initialization, and implementing this function 
		//		with cluster shards is more complex, so this will do for now.
        //      Specifically, cluster shards exists only from redis 7, so retrocompatibility is more dificult 
        auto nodesInfo = redisCluster->redis("a").command<std::string>("CLUSTER", "NODES"); 	

		// Parse the response and extract slot ranges for each node
		std::istringstream ss(nodesInfo);
		std::string line;

		// For each node in the cluster
		while (std::getline(ss, line)) 
		{
			std::istringstream nodeInfo(line);
			std::string nodeId, masterNodeId, address;
			std::string role, ping_sent, pong_recv, config_epoch, link_state;
			int slotStart, slotEnd;

			nodeInfo >> nodeId >> address >> role >> masterNodeId >> ping_sent >> pong_recv >> config_epoch >> link_state;

			// Get port from address
			std::string port = address.substr(address.find(':') + 1, address.find('@') - address.find(':') - 1);

			if (masterNodeId == "-") 	// If the node is a master node
			{
				// Extract slot ranges from nodeInfo
				std::vector<std::pair<int, int>> ranges;

				while (nodeInfo >> slotStart >> slotEnd) 
				{
					// Slot range is a-b, so the second number has been stored as negative
					slotEnd = -slotEnd;	
					ranges.push_back(std::make_pair(slotStart, slotEnd));
				}

				slotRanges.push_back(ranges);
			}
		}

		return slotRanges;
	}

    std::vector<std::vector<std::pair<int, int>>> getClusterSlotRanges_slotsCommand() 
    {
        std::vector<std::vector<std::pair<int, int>>> slotRanges;

        std::vector<std::optional<std::string>> slotsInfo;

        redisCluster->command
                    ("CLUSTER", "SLOTS", std::back_inserter(slotsInfo)); 	

        for (auto slotInfo : slotsInfo) 
        {
            int slotStart = std::stoi(slotInfo.value());
            int slotEnd = std::stoi(slotInfo.value());

            slotRanges.push_back(std::vector<std::pair<int, int>>{std::make_pair(slotStart, slotEnd)});
        }

        return slotRanges;
    }






    // Generates a sample key that belongs to a slot
	std::string generateKeyForSlot(int targetSlot) {

		for (uint64_t i = 0; i < UINT64_MAX; ++i) 
		{
			std::string key = std::to_string(i);
			uint16_t slot = crc16_gpt(key.c_str(), key.length()) % 16384; // Total number of Redis slots, default is 16384

			if (slot == targetSlot) 
				return key;
		}

		return ""; // Return an empty string if no matching key is found (unlikely/impossible?)
	}

    /****************************************************************/
    /********************* PUBLIC FUNCTIONS *************************/
    /****************************************************************/
public:

    Redis_connection(sw::redis::ConnectionOptions &_options, 
            sw::redis::ConnectionPoolOptions _pool_options=sw::redis::ConnectionPoolOptions()) :
    connection_options (_options)
    {
        sw::redis::ConnectionOptions subscriberOptions = connection_options;
        subscriberOptions.socket_timeout = std::chrono::milliseconds(0);	// Subscriber should not timeout

        if (cluster_mode())  {
            redisCluster = sw::redis::RedisCluster(connection_options, _pool_options); 
            subscriberCluster = sw::redis::RedisCluster(subscriberOptions, _pool_options);
        }
        else { 
            redis = sw::redis::Redis(connection_options, _pool_options);
            subscriberRedis = sw::redis::Redis(subscriberOptions, _pool_options);
        }

        generateNodeKeys();
        CLUSTER_SIZE = sample_cluster_keys.size();
    }

    std::vector<std::vector<std::string>> 
        orderByNode (const std::vector<std::string> &keys) override
    {
        std::vector<std::vector<std::string>> orderedKeys(CLUSTER_SIZE);

        for (auto &key : keys)
        {
            int slot = getNodeOfKey(key);
            orderedKeys[slot].push_back(key);
        }

        return orderedKeys;
    }


    bool cluster_mode()
	{
        if (CLUSTER_MODE == -1)
        {
            CLUSTER_MODE = 1;

            try {
                sw::redis::RedisCluster redis(connection_options);	// Creates a clustered connection to check if an exception is thrown.
            }
            catch(const std::exception& e)
            {
                std::string error = e.what();

                if (error == "ERR This instance has cluster support disabled")	// If the exception is the one thrown when cluster support is disabled
                    CLUSTER_MODE = 0;
                else 
                    throw;
            }
        }

        return CLUSTER_MODE == 1;
	}


    
    // Modifies the key to make sure it hashes to a certain node, and return its id
    void virtualToPhysicalKey (std::string &key)
    {
        Redis_connection::virtualToPhysicalKey(key, cluster_mode(), sample_cluster_keys, CLUSTER_SIZE);
    }

    static void virtualToPhysicalKey (std::string &key, bool cluster_mode, 
                                        std::vector<std::string> &sample_cluster_keys,
                                        unsigned int cluster_size)
    {
        if (cluster_mode)
        {
            int slot = getNodeOfKey(key, cluster_mode, cluster_size);   // Hashes the key to a node

            std::string sample = sample_cluster_keys[slot];
            key = "{" + sample + "}:" + key;                        // Forces the key to be hashed to the node
        }
    }

    void virtualToPhysicalKey (std::vector<std::string> &keys)
    {
        Redis_connection::virtualToPhysicalKey(keys, cluster_mode(), sample_cluster_keys, CLUSTER_SIZE);
    }

    static void virtualToPhysicalKey (std::vector<std::string> &keys, 
                                        bool cluster_mode, 
                                        std::vector<std::string> &sample_cluster_keys,
                                        unsigned int cluster_size)
    {
        if (cluster_mode)
        {
            for (auto &key : keys)
            {
                int slot = getNodeOfKey(key, cluster_mode, cluster_size);   // Hashes the key to a node

                std::string sample = sample_cluster_keys[slot];     
                key = "{"+sample+"}:" + key;                        // Forces the key to be hashed to the node
            }
        }
    }

    static void physicalToVirtualKey (std::string &key)
    {
        // Gets the string between { and }
        std::size_t pos1 = key.find("{");
        std::size_t pos2 = key.find("}");
        std::string example_key;

        if (pos1 != std::string::npos 
            && pos2 != std::string::npos 
            && pos2 > pos1)
        {
            example_key = key.substr(pos1+1, pos2-pos1-1);

            std::size_t pos3 = key.find(":");
            if (pos3 != std::string::npos)
                key = key.substr(pos3+1);
        }
        else {
            throw std::runtime_error("Key is not a physical key");
        }
    }

    

    unsigned int cluster_size()
    {
        return CLUSTER_SIZE;
    }
    
    int getNodeOfKey (const std::string &key)
    {
        return Redis_connection::getNodeOfKey(key, cluster_mode(), CLUSTER_SIZE);
    }

    static int getNodeOfKey (const std::string &key, bool cluster_mode, unsigned int cluster_size)
    {
        if (cluster_mode)
            return crc16_gpt(key.c_str(), key.length()) % cluster_size;
        else
            return 0;
    }

    long long setnx (std::string key, const std::string &value)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->setnx(key, value);
        else
            return redis->setnx(key, value);
    }

    std::string consume_message(std::string channel) override
    {
        virtualToPhysicalKey(channel);

        if (cluster_mode())
        {
            sw::redis::Subscriber sub = subscriberCluster->subscriber();
            std::string msg = "";
            std::mutex mtx;

            sub.on_message([&](const std::string &channel, const std::string &message) {
                std::lock_guard<std::mutex> lock(mtx);
                msg = message;
            });
            sub.subscribe(channel);

            while (msg.empty()) 
            {
                sub.consume();
            }

            std::lock_guard<std::mutex> lock(mtx);
            return msg;
        }
        else
        {
            sw::redis::Subscriber sub = subscriberRedis->subscriber();
            std::string msg = "";
            std::mutex mtx;

            sub.on_message([&](const std::string &channel, const std::string &message) {
                std::lock_guard<std::mutex> lock(mtx);
                msg = message;
            });
            sub.subscribe(channel);

            std::unique_lock<std::mutex> lock(mtx);
            while (msg.empty()) 
            {
                lock.unlock();
                sub.consume();
                lock.lock();
            }

            return msg;
        }
    }
    
    long long increment(std::string key, std::string number) override
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->incrby(key, std::stoll(number));
        else
            return redis->incrby(key, std::stoll(number));
    }

    long long hincrby(std::string key, std::string field, std::string number) override
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->hincrby(key, field, std::stoll(number));
        else
            return redis->hincrby(key, field, std::stoll(number));
    }



    std::unique_ptr<DB_pipeline> pipeline(const std::string &key, 
            bool create_connection=true, 
            bool concurrent_ready =false)
    {
        if (cluster_mode())
        {            
            sw::redis::RedisCluster &rc = *redisCluster;

            auto temp = std::make_unique<Redis_pipeline>(rc, key, 
                    create_connection, cluster_mode(), 
                    CLUSTER_SIZE, sample_cluster_keys, 
                    concurrent_ready);

            return temp;
        }
        else
        {
            return std::make_unique<Redis_pipeline>(*redis, key, 
                    create_connection, cluster_mode(), 
                    CLUSTER_SIZE, sample_cluster_keys, 
                    concurrent_ready);
        }
    }

    std::string evalString (const std::string &script, 
                        std::vector<std::string> keys, 
                        const std::vector<std::string> &args)
    {
        virtualToPhysicalKey(keys);

        if (cluster_mode())
            return redisCluster->eval<std::string>(script, keys.begin(), keys.end(), args.begin(), args.end());
        else
            return redis->eval<std::string>(script, keys.begin(), keys.end(), args.begin(), args.end());
    }

    long long evalInt (const std::string &script, 
                        std::vector<std::string> keys, 
                        const std::vector<std::string> &args)
    {
        virtualToPhysicalKey(keys);

        try 
        {
            if (cluster_mode())
                return redisCluster->eval<long long>(script, keys.begin(), keys.end(), args.begin(), args.end());
            else
                return redis->eval<long long>(script, keys.begin(), keys.end(), args.begin(), args.end());
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n';
            throw;
        }

        return -1;
    }

    std::vector<std::string> evalStringVector (const std::string &script, 
                        std::vector<std::string> keys, 
                        const std::vector<std::string> &args)
    {
        virtualToPhysicalKey(keys);

        if (cluster_mode())
            return redisCluster->eval<std::vector<std::string>>(script, keys.begin(), keys.end(), args.begin(), args.end());
        else
            return redis->eval<std::vector<std::string>>(script, keys.begin(), keys.end(), args.begin(), args.end());
    }

    long long sadd (std::string key, const std::string &value)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->sadd(key, value);
        else
            return redis->sadd(key, value);
    }

    long long srem (std::string key, const std::string &value)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->srem(key, value);
        else
            return redis->srem(key, value);
    }

    long long set (std::string key, const std::string &value)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->set(key, value);
        else
            return redis->set(key, value);
    }

    long long scard (std::string key)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->scard(key);
        else
            return redis->scard(key);
    }

    bool sismember (std::string key, const std::string &value)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->sismember(key, value);
        else
            return redis->sismember(key, value);
    }

    long long publish (std::string channel, const std::string &message)
    {
        virtualToPhysicalKey(channel);

        if (cluster_mode())
            return redisCluster->publish(channel, message);
        else
            return redis->publish(channel, message);
    }

    std::optional<std::string> get(std::string key)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->get(key);
        else
            return redis->get(key);
    }

    std::optional<std::string> hget (std::string key, const std::string &field)
    {
        virtualToPhysicalKey(key);

        if (cluster_mode())
            return redisCluster->hget(key, field);
        else
            return redis->hget(key, field);
    }

    std::unordered_map<std::string, std::string> hgetall(std::string key)
    {
        virtualToPhysicalKey(key);

        std::unordered_map<std::string, std::string> result;

        if (cluster_mode())
            redisCluster->hgetall(key, std::inserter(result, result.end()));
        else
            redis->hgetall(key, std::inserter(result, result.end()));

        return result;
    }

    long long smembers(std::string key, std::unordered_set<std::string> &output)
    {
        virtualToPhysicalKey(key);

		long long cursor = 0;
		do
		{															// Gets all elements in the set
			if (cluster_mode())
                redisCluster->sscan(key, cursor, 1000, std::inserter(output, output.begin()));
            else
                redis->sscan(key, cursor, 1000, std::inserter(output, output.begin()));      
		} 
        while (cursor > 0);

        return output.size();
    }

    long long smembers(std::string key, std::set<std::string> &output)
    {
        virtualToPhysicalKey(key);

		long long cursor = 0;
		do
		{															// Gets all elements in the set
			if (cluster_mode())
                redisCluster->sscan(key, cursor, 1000, std::inserter(output, output.begin()));
            else
                redis->sscan(key, cursor, 1000, std::inserter(output, output.begin()));      
		} 
        while (cursor > 0);

        return output.size();
    }

    std::string ping()
    {
        if (cluster_mode())
            return redisCluster->redis("").ping();
        else
            return redis->ping();
    }

    std::vector<std::string> exclusive_hget(std::vector<std::string> keys, 
                        const std::vector<std::string> &args)
    {
        return evalStringVector(
                            script_exclusive_hget,
                            keys,
                            args);
    }

    long long write_merge_buffer(std::vector<std::string> keys, 
                        const std::vector<std::string> &args)
    {
        return evalInt(script_write_merge_buffer,
                            keys,
                            args);
    }

    long long hset(std::vector<std::string> keys, 
                const std::vector<std::string> &args) override
    {
        return evalInt(coherency_hset, keys, args);
    }

    long long hdel(std::vector<std::string> keys, 
                const std::vector<std::string> &args) override
    {
        return evalInt(coherency_hdel, keys, args);
    }

    long long del(std::vector<std::string> keys, 
                const std::vector<std::string> &args) override
    {
        return evalInt(coherency_del, keys, args);
    }

    long long exclusive_release_hset(std::string blockId, 
                const std::string &key,
                const std::string &value, 
                const std::string &timestamp) override
    {
        return evalInt(exclusive_release_script, {blockId}, {key, value, timestamp});
    }

    long long hsetnx(std::string key, std::string field, std::string value) override
    {
        auto res = evalInt(coherency_hsetnx, {key}, {field, value});
        
        return res;
    }

    static const std::string getHsetScript()
    {
        return coherency_hset;
    }

    static const std::string getHdelScript()
    {
        return coherency_hdel;
    }

    static const std::string getDelScript()
    {
        return coherency_del;
    }

    static const std::string getHsetnxScript()
    {
        return coherency_hsetnx;
    }


class Redis_pipeline : public DB_pipeline
{
protected:
    bool cluster_mode, concurrent_ready;
    unsigned int cluster_size;
    
    std::shared_mutex samples_mutex;
    std::vector<std::string> sample_cluster_keys;

    std::mutex pipeline_mutex;
    sw::redis::Pipeline pipeline;   // Must be constructed after samples and cluster mode

    void virtualToPhysicalKey(std::string &key)
    {
        if (concurrent_ready)
            samples_mutex.lock_shared();
        
        Redis_connection::virtualToPhysicalKey(key, cluster_mode, sample_cluster_keys, cluster_size);
    
        if (concurrent_ready)
            samples_mutex.unlock_shared();
    }

    void virtualToPhysicalKey(std::vector<std::string> &keys)
    {
        if (concurrent_ready)
            samples_mutex.lock_shared();

        Redis_connection::virtualToPhysicalKey(keys, cluster_mode, sample_cluster_keys, cluster_size);

        if (concurrent_ready)
            samples_mutex.unlock_shared();
    }

public:
    Redis_pipeline(sw::redis::RedisCluster &redisc, 
                    std::string key, 
                    bool createConnection,
                    bool cluster_mode,
                    unsigned int cluster_size,
                    std::vector<std::string> sample_cluster_keys,
                    bool _concurrent_ready = false) 
    : 
    cluster_mode (cluster_mode),
    cluster_size (cluster_size),
    concurrent_ready (_concurrent_ready),

    sample_cluster_keys (sample_cluster_keys),
    pipeline(redisc.pipeline(getInitialKey(key), createConnection)) 
    {}   

    Redis_pipeline(sw::redis::Redis &redis, 
                    std::string key, 
                    bool createConnection,
                    bool cluster_mode,
                    unsigned int cluster_size,
                    std::vector<std::string> &_sample_cluster_keys,
                    bool _concurrent_ready = false) 
    : 
    cluster_mode (cluster_mode),
    cluster_size (cluster_size),
    concurrent_ready (_concurrent_ready),

    sample_cluster_keys (_sample_cluster_keys),
    pipeline(redis.pipeline(createConnection))
    {}   

    ~Redis_pipeline()
    {
        pipeline.exec();
    }

    std::string getInitialKey (std::string &key)
    {
        virtualToPhysicalKey(key);
        return key;
    }


    void eval(const std::string &script, 
                std::vector<std::string> keys, 
                const std::vector<std::string> &args) override
    {
        virtualToPhysicalKey(keys);

        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.eval(script, 
                keys.begin(), keys.end(), 
                args.begin(), args.end());
        
        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void sadd(std::string key, const std::string &value) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.sadd(key, value);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void set(std::string key, const std::string &value) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.set(key, value);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void srem(std::string key, const std::string &value) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.srem(key, value);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void scard(std::string key) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.scard(key);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void sismember(std::string key, const std::string &value) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.sismember(key, value);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void publish(std::string channel, const std::string &message) override
    {
        virtualToPhysicalKey(channel);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.publish(channel, message);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void hget(std::string key, const std::string &field) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.hget(key, field);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void increment(std::string key, std::string number) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.incrby(key, std::stoll(number));

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }
    
    void hincrby(std::string key, std::string field, std::string number) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.hincrby(key, field, std::stoll(number));

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void setnx (std::string key, const std::string &value) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.setnx(key, value);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    void hgetall(std::string key) override
    {
        virtualToPhysicalKey(key);
        
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        pipeline.hgetall(key);

        if (concurrent_ready)
            pipeline_mutex.unlock();
    }

    std::unique_ptr<DB_replies> exec() override
    { 
        if (concurrent_ready)
            pipeline_mutex.lock();
        
        auto results = std::make_unique<Redis_replies>(pipeline);   // Executes and get

        if (concurrent_ready)
            pipeline_mutex.unlock();

        return results;
    }

    void hset(std::vector<std::string> keys, 
                const std::vector<std::string> &args) override
    {
        eval(Redis_connection::getHsetScript(), keys, args);
    }

    void hsetnx(std::string key, std::string field, std::string value) override
    {
        eval(Redis_connection::getHsetnxScript(), {key}, {field, value});
    }

    void hdel(std::vector<std::string> keys, 
                const std::vector<std::string> &args) override
    {
        eval(Redis_connection::getHdelScript(), keys, args);
    }

    void del(std::vector<std::string> keys, 
                const std::vector<std::string> &args) override
    {
        eval(Redis_connection::getDelScript(), keys, args);
    }
};
};


