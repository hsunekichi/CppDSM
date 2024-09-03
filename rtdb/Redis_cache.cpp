#include <iostream>
#include <thread>
#include <memory>
#include <chrono>
#include <string>
#include "redis++.h"
#include <queue>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <stdio.h>
#include <Windows.h>
#include <set>
#include <functional>
#include "Redis_buffer.cpp"
#include "Redis_memory_controller.cpp"

using Pipe = sw::redis::QueuedRedis<sw::redis::PipelineImpl>;		// Pipe type
using StringPair = std::pair<std::string, std::string>;

enum base_cache_consistency {Sequential, Causal, TSO};

int l_gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}

/**************************************************************************************************/
//	NAME:			Redis_cache
//	DESCRIPTION:	Wrapper class for Redis operations that offers buffering and caching capabilities
//	IN PARAMETERS:  Commented on each parameter
//	OUT PARAMETERS: Depends on the function
//	RETURNS:		
//	HISTORY:
//		DATE		AUTHOR	DESCRIPTION
//		2022/07/11	HMT		Creation
//		2022/09/14	HMT		Last revision
/**************************************************************************************************/
class Redis_cache
{
protected:

	/***************************************** CONFIGURATION ************************************************/
	// Once initialized, all configuration should be constant

	// Undefined behaviour on runtime changes
	bool USE_BUFFER;			// Whether to activate the buffer
	bool USE_CACHE;				// Whether to activate the cache
	bool CLUSTER_MODE;			// Whether to connect to a redis cluster or a single server
	unsigned int CLUSTER_SIZE;	// Number of nodes in the cluster

	/******************************************** GLOBAL ****************************************************/
	
	base_cache_consistency consistency_mode;						// Consistency model for the cache						
	std::mutex mtx_exclusive_value;									// Mutex to access the exclusive_value variable	
	std::unordered_map<std::thread::id, std::string> exclusive_values;	// Value to set when a variable is exclusively accessed
	
	std::mutex mtx_cache_mutex;										// Mutex to mantain concurrent cache consistency/coherency

	std::optional<sw::redis::RedisCluster> redisCluster;			// Redis cluster connection

	std::shared_mutex mtx_sample_cluster_keys;							// Mutex to access the sample_cluster_keys variable
	std::unordered_map<std::string, std::string> sample_cluster_keys;	// Sample of a key that belongs to each node in the cluster
	
	/****************************************** BUFFER **************************************************/

	// Must be declared under sample_cluster_keys
	Redis_buffer redis_buffer;										// Buffer to store all the operations	

	/****************************************** CACHE ***************************************************/

	Redis_memory_controller redis_memory_controller;				// Cache to store all the variables

	/******************************************** PROTECTED METHODS *****************************************/

	/**************************************************************************************************/
	//	NAME:			loadAsset
	//	DESCRIPTION:	Loads a complete asset into the cache
	//	IN PARAMETERS:	sBlockName	key identifier of the block
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE if the block has been loaded, FALSE otherwise
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/19	HMT		Creation
	/**************************************************************************************************/
	/*
	void loadAsset (const std::string &sBlockName)
	{
		struct timeval begin, end;													// Performance variables
		long long seconds, microseconds;

		std::shared_lock lock (blocksMtx);
		if (!blockIsCached(sBlockName))												// Block is not already cached
		{	
			lock.unlock();
			l_gettimeofday(&begin, 0);												// Gets initial time 

			std::set<std::string> devices;				 							// Temporal set to store all device names
			std::set<std::string> types;				 							// Temporal set to store all types
			auto inserterDevices = std::inserter(devices, devices.begin());
			auto inserterTypes = std::inserter(types, types.begin());

			scanSet(pRedisLoader, sBlockName + sDeviceListSep, inserterDevices);	// Gets all device names in the block
			scanSet(pRedisLoader, sBlockName + sTypesListSep, inserterTypes);		// Gets all type names in the block
				
			std::unique_lock ulock (blocksMtx);
			auto block = addBlock(sBlockName);	
			
			for (auto type : types)
			{
				addType(sBlockName, type);											// Adds the type to the block
			}

			ulock.unlock();

			storeAllDeviceVariables(sBlockName, devices);							// Stores all devices from the block
			storeAllVariableInfo(sBlockName, types);								// Stores all type variables info from the block

			psubscribe(sBlockName + cDELIMITER + "*");								// Subscribes to the hole block

			l_gettimeofday(&end, 0);													// Gets finishing time							
			seconds = end.tv_sec - begin.tv_sec;
			microseconds = end.tv_usec - begin.tv_usec;

			performanceMtx.lock();
			totalLoadBlockTime += seconds + microseconds * 1e-6;					// Stores total time used to get the block
			nBlocksLoaded++;														// Adds one to the number of blocks loaded
			performanceMtx.unlock();
		}

		loaderActiveMtx.lock();														// Releases thread ocupation
		loaderActive = false;
		loaderActiveMtx.unlock();
	}

	// Loads from a block all devices and associated variables to a given type
	void loadType (const std::string &sBlockName, const std::string &sType)
	{
		struct timeval begin, end;												// Performance variables
		long long seconds, microseconds;

		std::shared_lock lock (blocksMtx);
		if (!typeIsCached(sBlockName, sType))									// Type is not already cached
		{	
			lock.unlock();
			l_gettimeofday(&begin, 0);											// Gets initial time 

			std::set<std::string> devices;				 						// Temporal set to store all device names
			std::set<std::string> types;				 						// Temporal set to store all types
			auto inserterDevices = std::inserter(devices, devices.begin());		// Inserter for the devices
			

			scanSet(pRedisLoader, sBlockName + cDELIMITER + sDeviceTypeListSep + cDELIMITER + sType, inserterDevices);		// Gets all devices of the type
			types.insert(sType);																							// Adds the type to the list
			
			std::unique_lock ulock (blocksMtx);

			if (!blockIsCached(sBlockName))										// If the block is not cached, it adds it
			{
				addBlock(sBlockName);
				psubscribe(sBlockName + cDELIMITER + "*");						// Subscribes to the hole block
			}

			auto block = addType(sBlockName, sType);
			ulock.unlock();	

			storeAllDeviceVariables(sBlockName, devices);						// Stores all devices
			storeAllVariableInfo(sBlockName, types);							// Stores info of all variables of a type

			l_gettimeofday(&end, 0);												// Gets finishing time							
			seconds = end.tv_sec - begin.tv_sec;
			microseconds = end.tv_usec - begin.tv_usec;

			performanceMtx.lock();
			totalLoadBlockTime += seconds + microseconds * 1e-6;				// Stores total time used to get the block
			nBlocksLoaded++;													// Adds one to the number of blocks loaded
			performanceMtx.unlock();
		}

		loaderActiveMtx.lock();													// Releases thread ocupation
		loaderActive = false;
		loaderActiveMtx.unlock();
	}
*/

	// Gets a variable, mantaining coherency with buffer and cache
	// useCache = false, forces the function to read from DB. Used for sync operations
	OptionalString getVariable(std::string blockName, std::string variableName)
	{
		OptionalString value;

		if (USE_BUFFER
			&&
			(
				consistency_mode == base_cache_consistency::Sequential	// Strong consistency
				||
				consistency_mode == base_cache_consistency::TSO
			)
		)
		{			
			// In order:
			// Reads the variable from cache
			// If not cached, checks the buffer and loads the block
			// Checks again the buffer in case a write was done while loading the block
			value = redis_memory_controller.readVarFromCache(blockName, variableName);	// Gets the value from the cache

			if (!value)
			{
				OptionalString temp; 

				value = redis_buffer.hashVariableInBuffer(blockName, variableName);		// Miss, checks if the variable is in the buffer
				
				if (!value)	// Miss in buffer, get variable from redis
				{
					value = redis_memory_controller.getVariable(blockName, variableName);	// Loads the value from redis

					// Checks if the variable was not written while reading from redis
					temp = redis_buffer.hashVariableInBuffer(blockName, variableName);
					if (temp)
					{
						value = temp;
						redis_memory_controller.writeVariable(blockName, variableName, value.value());	// Writes the value to the cache
					}
				}
			}				
		}
		else	// Relaxed consistency or no buffer
		{
			value = redis_memory_controller.getVariable(blockName, variableName);
		}

		return value;
	}

	// Returns the crc16 of a string. 
	// This code works like the one in redis, most crc16 implementationsn on the internet do not.
	// I don't know the mathematical difference, but this code is the one that works (courtesy of chatGPT)
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

	std::unordered_map<std::string, std::vector<std::pair<int, int>>> getClusterSlotRanges() 
	{
		std::unordered_map<std::string, std::vector<std::pair<int, int>>> slotRanges;

		// TODO: This command should be "CLUSTER SHARDS", 
		// 		as redis recommends not to use CLUSTER NODES to get the slot ranges, I don't know why.
		// 		This is only executed for initialization, and implementing this function 
		//		with cluster shards is more complex, so this will do for now.
		auto nodesInfo = redisCluster->command<std::string>("CLUSTER", "NODES"); 	

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

				std::string cluster_node = port.substr(port.size()-1);	// TODO: This only works for 1 digit consecutive ports. 
				// The real port/node id should be distinct from the generated node id (crc16 % CLUSTER_SIZE) 
				slotRanges[cluster_node] = ranges;
			}
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

	// Generates one sample key for each node in the cluster, and returns the map
	std::unordered_map<std::string, std::string> generateClusterNodeKeys()
	{
		std::unordered_map<std::string, std::string> cluster_keys;

		auto ranges = getClusterSlotRanges();

		auto lock = std::unique_lock(mtx_sample_cluster_keys);
		for (auto node : ranges)
		{
			int node_sample_slot = node.second[0].first;	// First slot in the range
			std::string example_key = generateKeyForSlot(node_sample_slot);

			cluster_keys[node.first] = example_key;	
		}

		CLUSTER_SIZE = cluster_keys.size();

		return cluster_keys;
	}

    // Returns a string that maps to the node the key should go
    std::string getNodeExampleId(std::string key)
    {
		if (CLUSTER_MODE)
		{
			// Gets the string between { and }
			std::size_t pos1 = key.find("{");
			std::size_t pos2 = key.find("}");

			if (pos1 != std::string::npos 
				&& pos2 != std::string::npos 
				&& pos2 > pos1)
			{
				key = key.substr(pos1+1, pos2-pos1-1);
			}

			auto node = crc16_gpt(key.c_str(), key.size()) % CLUSTER_SIZE;	// Id of the node where the key belongs			

			auto lock = std::shared_lock(mtx_sample_cluster_keys);
			return sample_cluster_keys[std::to_string(node)];	
		}
		else
		{
			return 0;	// No cluster
		}
    }

	std::string getBlockName(const std::string& key)
	{
		if (CLUSTER_MODE)
		{
			std::string nodeId = getNodeExampleId(key);
			return "{"+nodeId+"}:"+key;
		}
		else {
			return key;
		}
	}

	static bool isRedisCluster(sw::redis::ConnectionOptions &options)
	{
		bool isCluster = true;

		try {
			sw::redis::RedisCluster redis(options);	// Creates a clustered connection to check if an exception is thrown.
		}
		catch(const std::exception& e)
		{
			std::string error = e.what();

			if (error == "ERR This instance has cluster support disabled")	// If the exception is the one thrown when cluster support is disabled
				isCluster = false;
			else
				throw e;
		}

		return isCluster;
	}


public:

	/****************************************************************************************************************************************/
	/********************************************************** PUBLIC METHODS **************************************************************/
	/****************************************************************************************************************************************/

	
	// Complete constructor
	Redis_cache(sw::redis::ConnectionOptions options, int _BUFFER_LATENCY=50, int _CACHE_LATENCY=100, int _PIPE_SIZE=0, 
				bool _USE_BUFFER = true, bool _USE_CACHE = true, int nConcurrency=1,
				base_cache_consistency cache_consistency_mode = base_cache_consistency::TSO
	) :
		CLUSTER_MODE(isRedisCluster(options)),	// Checks if the connection is to a cluster or a single server
		redisCluster(CLUSTER_MODE ? std::make_optional(sw::redis::RedisCluster(options)) : std::nullopt),	// If cluster mode, initializes the cluster

		// Since there are a maximum of 16384 slots/nodes, generateClusterNodeKeys could be precalculated if necessary
		sample_cluster_keys(CLUSTER_MODE ? generateClusterNodeKeys() : std::unordered_map<std::string, std::string>()), 
		
		redis_buffer(options, _USE_BUFFER, _BUFFER_LATENCY,
					// If sequential consistency, reordering is not allowed. Needs 1 extra thread for the automatic flush
					cache_consistency_mode != base_cache_consistency::Sequential, 
					CLUSTER_MODE, 
					_PIPE_SIZE, 
					nConcurrency, sample_cluster_keys), 

		redis_memory_controller(options, _USE_CACHE, CLUSTER_MODE, 
								_CACHE_LATENCY, nConcurrency)
	{
		USE_BUFFER = _USE_BUFFER;
		USE_CACHE = _USE_CACHE;
		consistency_mode = cache_consistency_mode;
	}

	// Destructor
	~Redis_cache() {}

	// Up to this point, all memory operations have been performed. Consistency operation
	void release_barrier()
	{
		redis_buffer.flushBuffer();
	}

	// No memory operation will perform before this barrier. Consistency operation
	void aquire_barrier()
	{
		redis_memory_controller.strong_update();
	}

    // Deletes all cached blocks, and returns whether there were any or not
	bool reset_cache()
	{
		return redis_memory_controller.reset_cache();
	}

	/************************************************************************ READ ******************************************************************************/

	// Like redis hget, but if the key has a sDeviceType variable it loads also all the devices of that type
	OptionalString hget(const std::string &unclusteredBlockName, const std::string &variableName)
	{
		std::string blockName = getBlockName(unclusteredBlockName);

		OptionalString variableValue;

		// In sequential consistency, each load is secuential to all stores.
		if (consistency_mode == base_cache_consistency::Sequential)
			release_barrier();
		
		variableValue = getVariable(blockName, variableName);

		if (consistency_mode == base_cache_consistency::Sequential)
			aquire_barrier();

		//std::cout << "hget: " << variableValue.value() << std::endl;

		return variableValue;
	}

	// Like redis hget, but with aquire sinchronization ordering and exclusivity on the variable
	OptionalString hget_exclusive_aquire(const std::string &unclusteredBlockName, const std::string &sKey)
	{
		std::string blockName = getBlockName(unclusteredBlockName);

		if (consistency_mode == base_cache_consistency::Sequential)		// In sequential consistency, each load is secuential to all stores.
			release_barrier();

		std::thread::id this_id = std::this_thread::get_id();			// Gets the id of the current thread


		// Gets variable from redis and its exclusive timestamp
		std::pair<std::string, std::string> pair_result = 
						redis_memory_controller.getVariableExclusive(blockName, sKey);		
		
		// Stores the exclusive timestamp for this thread
		mtx_exclusive_value.lock();										// Locks the mutex to access the exclusive values map
		exclusive_values[this_id] = pair_result.second;					// Stores the exclusive timestamp in the map
		mtx_exclusive_value.unlock();									// Unlocks the mutex


		aquire_barrier();

		return pair_result.first;
	}

	// Returns the number of read operations that were made in the cache
	int getHitCount()
	{
		return redis_memory_controller.getHitCount();
	}

	// Returns the number of read operations that were made in Redis, because the block wasn't loaded
	int getMissCount()
	{
		return redis_memory_controller.getMissCount();
	}

	double getHitRatio()
	{
		return redis_memory_controller.getHitRatio();
	}

	double getMissRatio()
	{
		return redis_memory_controller.getMissRatio();
	}

	// Returns the average of time that loading each block has taken
	double getBlockAvgTime()
	{
		return redis_memory_controller.getBlockAvgTime();
	}

	int getCacheElementCount()
	{
		return redis_memory_controller.getCacheElementCount();
	}

	int getCacheMemorySize()
	{
		return redis_memory_controller.getCacheMemorySize();
	}

	void printCache()
	{
		redis_memory_controller.printCache();
	}

	/**************************************************************** WRITE ****************************************************************/
	
	bool change_consistency_mode(base_cache_consistency new_consistency_model)
	{
		consistency_mode = new_consistency_model;
		return true;
	}
	
	/********************* All the below operations are the same as redis++ ones, but with buffering capabilities **************************/

	// Bottleneck on buffered mode:
	//      executeOperation->
	//			AddOperationToBuffer->
	// 				store operation in the hash map that buffers the operations
	// The time spent in each operation is ~1us or less, directly depends on memory hierarchy.
	// Further optimization will require changing the buffer structure to optimize memory usage.
	bool hset(const std::string &unclusteredBlockName, const std::string &varName, const std::string &value)
	{
		bool executed = false;

		std::string blockName = getBlockName(unclusteredBlockName);

		Redis_operation operation;

		operation.op = RedisOp::hset;			// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = varName;
		operation.param[2] = value;
		
		
		// Prevents writes that do not change the value. Obviously it would be better to read from redis, but would be highly inefficient. 
		OptionalString old = redis_memory_controller.readVarFromCache(blockName, varName);		// Reads the block if is cached
		if (old.has_value() && old.value() == value) {
			executed = true;
		}
		else 
		{
			redis_memory_controller.writeIfCached(blockName, varName, value);		// Writes the block if is cached
			executed = redis_buffer.executeOperation(operation);
		}

		return executed;
	}

	// If the variable has not changed before hget_exclusive_aquire, sets it and returns true
	// Else returns false
	// Should always be performed after a hget_exclusive_aquire
	bool hset_exclusive_release(const std::string &unclusteredBlockName, const std::string &variableName, const std::string &value)
	{	
		std::string blockName = getBlockName(unclusteredBlockName);

		Redis_operation operation;	
		bool operation_performed;

		operation.op = RedisOp::exclusive_hset;
		operation.param[0] = blockName;
		operation.param[1] = variableName;
		operation.param[2] = value;


		release_barrier();		// Up to this point, all memory operations are done

		mtx_exclusive_value.lock();		// Locks the mutex to access the exclusive values map
		std::string exclusive_value = exclusive_values[std::this_thread::get_id()];		// Gets the exclusive value for this thread
		mtx_exclusive_value.unlock();	// Unlocks the mutex
		
		operation.param[3] = exclusive_value;		// Sets the exclusive value as a parameter of the operation


		long long response = redis_buffer.executeOperation(operation);		// Executes the operation
		operation_performed = bool(response);


		if (operation_performed)	// If the operation was executed
			redis_memory_controller.writeIfCached(blockName, variableName, value);		// Writes the block if is cached

		if (consistency_mode == base_cache_consistency::Sequential)
			aquire_barrier();	// No memory operation will perform before this barrier

		return operation_performed;
	}
	

	long long sadd(const std::string &param1, const std::string &param2)
	{
		std::string blockName = getBlockName(param1);

		Redis_operation operation;

		operation.op = RedisOp::sadd;			// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = param2;

		return redis_buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	long long srem(const std::string &param1, const std::string &param2)
	{
		std::string blockName = getBlockName(param1);

		Redis_operation operation;

		operation.op = RedisOp::srem;			// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = param2;

		return redis_buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	template<typename Input>
	long long hdel(const std::string &param1, Input begin, Input end)
	{
		std::string blockName = getBlockName(param1);

		auto size = std::distance(begin, end);

		for (auto param2 = begin; param2 != end; param2++)					// For each hdel
		{
			Redis_operation operation;

			operation.op = RedisOp::hdel;									// Creates the operation
			operation.param[0] = blockName;
			operation.param[1] = *param2;

			std::unique_lock lock(mtx_cache_mutex);					// Locks cache mutex
			redis_memory_controller.deleteVariable(blockName, *param2);		// Writes the block if is cached

			redis_buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
		}

		return size;
	}

	long long hdel(const std::string &param1, const std::string &param2)
	{
		std::string blockName = getBlockName(param1);

		Redis_operation operation;

		operation.op = RedisOp::hdel;									// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = param2;

		std::unique_lock lock(mtx_cache_mutex);				// Locks cache mutex
		redis_memory_controller.deleteVariable(blockName, param2);		// Deletes the block if is cached

		return redis_buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	long long del(const std::string &param1)
	{
		std::string blockName = getBlockName(param1);

		Redis_operation operation;

		operation.op = RedisOp::del;									// Creates the operation
		operation.param[0] = blockName;
		
		std::unique_lock lock(mtx_cache_mutex);				// Locks cache mutex
		redis_memory_controller.deleteBlock(blockName);		// Deletes the block if is cached

		return redis_buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	OptionalString get(const std::string &param1)
	{
		std::string blockName = getBlockName(param1);
		return redis_memory_controller.get(blockName);		// Gets the block if is cached
	}

	bool sismember(const std::string &param1, const std::string &param2)
	{
		std::string blockName = getBlockName(param1);
		return redis_memory_controller.sismember(blockName, param2);		// Gets the block if is cached
	}

	template<typename Output>
	long long sscan(const std::string &param1, long long cursor, int nElems, Output out)
	{
		std::string blockName = getBlockName(param1);
		return redis_memory_controller.sscan(blockName, cursor, nElems, out);		// Gets the block if is cached
	}

	long long scard(const std::string &param1)
	{
		std::string blockName = getBlockName(param1);
		return redis_memory_controller.scard(blockName);		// Gets the block if is cached
	}
	
	std::string ping()
	{
		return redis_memory_controller.ping();		// Gets the block if is cached
	}
};