#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
//#include <Windows.h>
#include <set>
#include "DB_buffer.cpp"
#include "Memory_controller.cpp"

#pragma once

using StringPair = std::pair<std::string, std::string>;

enum base_cache_consistency {Sequential, LRC}; // LRC -> Lazy Release Consistency

int l_gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}

/**************************************************************************************************/
//	NAME:			DB_cache
//	DESCRIPTION:	Wrapper for DB clients that offers buffering and caching capabilities
//	IN PARAMETERS:  Commented on each parameter
//	OUT PARAMETERS: -
//	RETURNS:		
//	HISTORY:
//		DATE		AUTHOR	DESCRIPTION
//		2022/07/11	HMT		Creation
//		2022/09/14	HMT		Last revision
/**************************************************************************************************/
class DB_cache
{
protected:

	/***************************************** CONFIGURATION ************************************************/
	// Once initialized, all configuration should be constant

	// Undefined behaviour on runtime changes
	bool USE_BUFFER;			// Whether to activate the buffer
	bool USE_CACHE;				// Whether to activate the cache
	unsigned int CLUSTER_SIZE;	// Number of nodes in the cluster

	/******************************************** GLOBAL ****************************************************/
	
	base_cache_consistency consistency_mode;						// Consistency model for the cache						
	std::mutex mtx_exclusive_value;									// Mutex to access the exclusive_value variable	
	std::unordered_map<std::thread::id, std::string> exclusive_values;	// Value to set when a variable is exclusively accessed
	
	std::mutex mtx_cache_mutex;										// Mutex to mantain concurrent cache consistency/coherency

	/****************************************** CACHE ***************************************************/

	// Must be declared before buffer
	Memory_controller memory_controller;				// Cache to store all the variables

	/****************************************** BUFFER **************************************************/

	DB_buffer buffer;										// Buffer to store all the operations	


	/******************************************** PROTECTED METHODS *****************************************/

	sw::redis::ConnectionOptions generate_options(std::string host, int port)
	{
		sw::redis::ConnectionOptions options;
		options.host = host;
		options.port = port;
		return options;
	}

	void soft_barrier_synchronization(std::string barrier_name, int nNodes)
	{
		long long new_val = increment("cntr_"+barrier_name, 1);

		// If the last node has arrived, send the event to all the nodes
		if (new_val == nNodes)
		{
			del("cntr_"+barrier_name);
			send_event("evnt_"+barrier_name, "1");
		}
		// Else, wait until the event is sent
		else {
			wait_event("evnt_"+barrier_name);
		}
	}

	

public:

	/****************************************************************************************************************************************/
	/********************************************************** PUBLIC METHODS **************************************************************/
	/****************************************************************************************************************************************/

	
	// Complete constructor
	DB_cache(sw::redis::ConnectionOptions options, int nConcurrency=1, int _BUFFER_LATENCY=50, int _CACHE_LATENCY=100, int _PIPE_SIZE=10000, 
				bool _USE_BUFFER = true, bool _USE_CACHE = true,
				base_cache_consistency cache_consistency_mode = base_cache_consistency::LRC
	) :
		memory_controller(options, 
				_USE_CACHE && cache_consistency_mode != base_cache_consistency::Sequential, 
				true,
				_CACHE_LATENCY, nConcurrency),
		
		buffer(options, 
				_USE_BUFFER && cache_consistency_mode != base_cache_consistency::Sequential, 
				_BUFFER_LATENCY,
				// If sequential consistency, reordering is not allowed. 
				true, 
				_PIPE_SIZE, nConcurrency)
	{
		USE_BUFFER = _USE_BUFFER;
		USE_CACHE = _USE_CACHE;
		consistency_mode = cache_consistency_mode;
	}

	DB_cache(std::string host, int port, int nConcurrency=1, int _BUFFER_LATENCY=50, int _CACHE_LATENCY=100, int _PIPE_SIZE=10000, 
				bool _USE_BUFFER = true, bool _USE_CACHE = true,
				base_cache_consistency cache_consistency_mode = base_cache_consistency::LRC
	) :
		DB_cache(generate_options(host, port), nConcurrency, _BUFFER_LATENCY, _CACHE_LATENCY, _PIPE_SIZE, _USE_BUFFER, _USE_CACHE, cache_consistency_mode)
	{}

	DB_cache() : DB_cache("localhost", 6379) {}

	// Destructor
	~DB_cache() {}

	// All previous write operations will be performed BEFORE this operation. Consistency operation
	void release_sync()
	{
		buffer.flushBuffer();
	}

	// All following read operations will be performed AFTER this point. Consistency operation
	void acquire_sync(bool invalidate=false)
	{
		if (invalidate)
			memory_controller.invalidate_cache();
		else
			memory_controller.updateCache();
	}

	// No memory operation can be reordered through this point
	void full_sync(bool invalidate=false)
	{
		release_sync();
		acquire_sync(invalidate);
	}

    // Deletes all cached blocks, and returns whether there were any or not
	bool clear_cache()
	{
		return memory_controller.invalidate_cache();
	}

	/************************************************************************ READ ******************************************************************************/

	// If the variable exists returns the value of variableName in BlockName, 
	//    if it does not exists returns NullOpt
	OptionalString hget(std::string blockName, const std::string &variableName)
	{
		auto variableValue = memory_controller.getVariable(blockName, variableName);
		return variableValue;
	}

	// Like redis hget, but with aquire sinchronization ordering and exclusivity on the variable
	OptionalString hget_exclusive_acquire(std::string blockName, const std::string &sKey)
	{
		std::thread::id this_id = std::this_thread::get_id();			// Gets the id of the current thread


		// Gets variable from DB and its exclusive timestamp
		std::pair<std::string, std::string> pair_result = 
						memory_controller.getVariableExclusive(blockName, sKey);		

		// Stores the exclusive timestamp for this thread
		mtx_exclusive_value.lock();										// Locks the mutex to access the exclusive values map
		exclusive_values[this_id] = pair_result.second;					// Stores the exclusive timestamp in the map
		mtx_exclusive_value.unlock();									// Unlocks the mutex

		//std::cout << "Data end: " << memory_controller.getVariable("data_end", "v").value_or("NULL") << std::endl;
		acquire_sync(true);

		return pair_result.first;
	}

	// Returns the number of read operations that were made in the cache
	long long get_hit_count()
	{
		return memory_controller.getHitCount();
	}

	// Returns the number of read operations that were made to DB because the block wasn't loaded
	long long get_miss_count()
	{
		return memory_controller.getMissCount();
	}

	double get_hit_ratio()
	{
		return memory_controller.getHitRatio();
	}

	double get_miss_ratio()
	{
		return memory_controller.getMissRatio();
	}

	// Returns the average of time that loading each block has taken in microseconds
	double get_block_avg_time()
	{
		return memory_controller.getBlockAvgTime();
	}

	long long get_n_cached_blocks()
	{
		return memory_controller.getCacheElementCount();
	}

	long long get_cache_memory_size()
	{
		return memory_controller.getCacheMemorySize();
	}

	void print_cache()
	{
		memory_controller.printCache();
	}

	// Loads into the cache the block in the list
	bool preload(std::string blockName)
	{
		return memory_controller.preload(blockName);
	}

	// Loads into the cache all the blocks that exist in the list
	bool preload(const std::vector<std::string> &blockNames)
	{
		return memory_controller.preload(blockNames);
	}

	/**************************************************************** WRITE ****************************************************************/
	
	/********************* All the below operations are the same as redis++ ones, but with buffering capabilities **************************/

	// Bottleneck on buffered mode:
	//      executeOperation->
	//			AddOperationToBuffer->
	// 				store operation in the hash map that buffers the operations
	// The time spent in each operation is ~1us or less, directly depends on memory hierarchy.
	// Further optimization will require changing the buffer structure to optimize memory usage.
	bool hset(const std::string &blockName, const std::string &varName, const std::string &value)
	{
		bool executed = false;

		DB_operation operation;

		operation.op = DB_opCode::hset;			// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = varName;
		operation.param[2] = value;

		// Prevents writes that do not change the value. Obviously it would be better to read from DB, but would be highly inefficient. 
		OptionalString old = memory_controller.readVarFromCache(blockName, varName);		// Reads the block if is cached
		if (old.has_value() && old.value() == value) {
			executed = true;
		}
		else 
		{
			// Write to cache and buffer
			memory_controller.writeVariable(blockName, varName, value);
			executed = buffer.executeOperation(operation);
		}

		return executed;
	}

	// If the variable does not exist, writes $value to $varName in $blockName and returns true.
 	//    Returns false otherwise and does not write anything.
	bool hsetnx(const std::string &blockName, const std::string &varName, const std::string &value)
	{
		bool executed = false;

		DB_operation operation;

		operation.op = DB_opCode::hsetnx;			// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = varName;
		operation.param[2] = value;

		// Prevents writes that do not change the value. Obviously it would be better to read from DB, but would be highly inefficient. 
		OptionalString old = memory_controller.readVarFromCache(blockName, varName);		// Reads the block if is cached
		if (old.has_value() && old.value() == value) {
			executed = true;
		}
		else 
		{
			// Write to cache and buffer
			memory_controller.writeVariable(blockName, varName, value);
			executed = buffer.executeOperation(operation);
		}

		return executed;
	}

	// If the variable has not changed before hget_exclusive_aquire, sets it and returns true
	// Else returns false
	// Should always be performed after a hget_exclusive_aquire
	bool hset_exclusive_release(const std::string &blockName, const std::string &variableName, const std::string &value)
	{	
		DB_operation operation;	
		bool operation_performed;

		operation.op = DB_opCode::exclusive_hset;
		operation.param[0] = blockName;
		operation.param[1] = variableName;
		operation.param[2] = value;


		release_sync();		// Up to this point, all memory operations are done

		mtx_exclusive_value.lock();		// Locks the mutex to access the exclusive values map
		std::string exclusive_value = exclusive_values[std::this_thread::get_id()];		// Gets the exclusive value for this thread
		mtx_exclusive_value.unlock();	// Unlocks the mutex
		
		operation.param[3] = exclusive_value;		// Sets the exclusive value as a parameter of the operation

		
		long long response = buffer.executeOperation(operation);		// Executes the operation
		operation_performed = bool(response);

		if (operation_performed) {
			memory_controller.writeVariable(blockName, variableName, value);		// Writes the variable to the cache
		}

		return operation_performed;
	}
	

	long long sadd(const std::string &blockName, const std::string &param2)
	{
		DB_operation operation;

		operation.op = DB_opCode::sadd;			// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = param2;

		return buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	long long srem(const std::string &blockName, const std::string &param2)
	{
		DB_operation operation;

		operation.op = DB_opCode::srem;			// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = param2;

		return buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	template<typename Input>
	long long hdel(const std::string &blockName, Input begin, Input end)
	{
		auto size = std::distance(begin, end);

		for (auto param2 = begin; param2 != end; param2++)					// For each hdel
		{
			hdel(blockName, *param2);										// Executes the operation (buffered)
		}

		return size;
	}

	long long hdel(const std::string &blockName, const std::string &param2)
	{
		DB_operation operation;

		operation.op = DB_opCode::hdel;									// Creates the operation
		operation.param[0] = blockName;
		operation.param[1] = param2;

		memory_controller.deleteVariable(blockName, param2);		// Deletes the block if is cached

		return buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	long long del(const std::string &blockName)
	{
		DB_operation operation;

		operation.op = DB_opCode::del;									// Creates the operation
		operation.param[0] = blockName;

		//std::unique_lock lock(mtx_cache_mutex);			// Locks cache mutex
		memory_controller.deleteBlock(blockName);		// Deletes the block if is cached

		return buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	OptionalString get(const std::string &blockName)
	{
		return memory_controller.get(blockName);	
	}

	// Increments an integer variable and returns the new value
	long long increment(const std::string &name, int number)
	{
		DB_operation operation;

		operation.op = DB_opCode::increment;		// Creates the operation
		operation.param[0] = name;
		operation.param[1] = std::to_string(number);

		return buffer.executeOperation(operation, true);	// Buffer mantains sequential consistency if necesary
	}

	// Increments an integer variable and returns the new value
	long long hincrby(const std::string &name, const std::string &variable, int number, bool execute_buffered=true)
	{
		DB_operation operation;

		operation.op = DB_opCode::hincrby;		// Creates the operation
		operation.param[0] = name;
		operation.param[1] = variable;
		operation.param[2] = std::to_string(number);

		memory_controller.incrementVariable(name, variable, number);	// Increments the variable if is cached
		return buffer.executeOperation(operation, !execute_buffered);	// Buffer mantains sequential consistency if necesary
	}

	// Sends an event to all the nodes waiting for it
	void send_event(const std::string &name, const std::string &content)
	{
		DB_operation operation;

		operation.op = DB_opCode::publish;									// Creates the operation
		operation.param[0] = name;
		operation.param[1] = content;

		buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
		release_sync(); // Force the buffer to send the operation now
	}

	// Waits until the event is recieved
	std::string wait_event(const std::string &name)
	{
		return memory_controller.wait_event(name);
	}

	bool set(std::string key, std::string value)
	{
		DB_operation operation;

		operation.op = DB_opCode::set;									// Creates the operation
		operation.param[0] = key;
		operation.param[1] = value;

		return buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	bool setnx(std::string key, std::string value)
	{
		DB_operation operation;

		operation.op = DB_opCode::setnx;									// Creates the operation
		operation.param[0] = key;
		operation.param[1] = value;

		return buffer.executeOperation(operation);	// Buffer mantains sequential consistency if necesary
	}

	bool sismember(const std::string &blockName, const std::string &param2)
	{
		return memory_controller.sismember(blockName, param2);		
	}

	long long smembers(const std::string &blockName, std::unordered_set<std::string> &out)
	{
		return memory_controller.smembers(blockName, out);
	}

	long long smembers(const std::string &blockName, std::set<std::string> &out)
	{
		return memory_controller.smembers(blockName, out);
	}

	long long scard(const std::string &blockName)
	{
		return memory_controller.scard(blockName);
	}
	
	// Returns “Pong” if there is conection with DB, empty string otherwise
	std::string ping()
	{
		return memory_controller.ping();
	}


	// Waits until nNodes have arrived to this barrier
	// If sync_consistency is true, it also performs a full synchronization of shared data
	void barrier_synchronization(std::string barrier_name, int nNodes, bool sync_consistency=true)
	{
		if (!sync_consistency)
		{
			soft_barrier_synchronization(barrier_name, nNodes);
		}
		else
		{
			// Send all data
			release_sync();
			soft_barrier_synchronization(barrier_name, nNodes); // All data sent

			// Update the writes of other nodes
			acquire_sync();
			soft_barrier_synchronization(barrier_name, nNodes); // All data recieved
		}
	}
};
