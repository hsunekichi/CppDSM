#include "redis++.h"
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <vector>
#include <atomic>
#include <Windows.h>


enum RedisOp {hset, exclusive_hset, publish, sadd, srem, hdel, del};


struct Redis_operation
{
	RedisOp op;
	std::string param[4];
};

using Pipe = sw::redis::QueuedRedis<sw::redis::PipelineImpl>;	// Pipe type
using Operations_buffer = std::unordered_map<
                                int, 
                                std::vector<Redis_operation>>;	// Buffer type
using OptionalString = std::optional<std::string>;
using Block = std::unordered_map<std::string, std::string>;		// Block type
using MERGE_BUFFER = std::unordered_map<
                        int, 
                        std::unordered_map<
                            std::string, 
                            Block>>;

using DATA_BATCH = std::vector<std::string>;
using SLOT_DATA_BATCHES = std::list<DATA_BATCH>;
using WRITE_SCRIPT_DATA = std::unordered_map<int, SLOT_DATA_BATCHES>;

class Redis_buffer
{
    protected:

    /********************************************** Constants **********************************************/

    const std::string lastUpdateId = "lastUpdateId";    // Identifier for the last update id
    const std::string streamId = "stream:";            // Identifier for the stream id
    const std::string REDIS_ERROR_INTEGER_OVERFLOW = "ERR increment or decrement would overflow"; // Redis error message for integer overflow
    const std::string noScriptError = "NOSCRIPT";       // Error returned by redis when the script is not cached
    const unsigned int REDIS_CLUSTER_N_SLOTS = 16384;

    const int N_MINIMUM_CONNECTIONS = 1;                // Number of additional connections the buffer needs internally
                                                            // 1 For the flushing thread

    unsigned int CLUSTER_SIZE;                          // Number of nodes in the cluster
    int nConnections;					                // Number of connections to the Redis server. Has to be declared BEFORE the connection
    int MAX_BUFFER_SIZE;                                // Maximum size of the buffer
    bool CLUSTER_MODE;                                  // True if the Redis server is a cluster
    bool USE_BUFFER, ALLOW_REORDERING;
    unsigned int BUFFER_LATENCY;                        // Time in milliseconds to wait before writing the buffer to DB
    std::thread th_bufferThread;
    std::vector<std::thread> th_write_merge_buffer;     // Threads to write the merge buffer to each cluster node

    std::optional<sw::redis::Redis> redisConnection;	    // Redis object
    std::optional<sw::redis::RedisCluster> redisCluster;    // Redis object


    //std::string coherency_script_sha;     // SHA of the coherency script
    // Lua scripts to mantain coherency. 
    // They cannot be cached, since the script could be lost in the middle of a pipeline execution 
    //      and make fail all the eval calls.

    std::string sha_write_merge_buffer = "";     // SHA of the write_merge_buffer script
    

    const std::string script_write_merge_buffer =  
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


    const std::string coherency_hset =  
            "local time = redis.call('TIME')"
            "local newId = tonumber(time[1])*1000000 + tonumber(time[2]);"  // Timestamp in us
            "return redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], '"+lastUpdateId+"', newId);";

    const std::string coherency_hdel =  
            "local time = redis.call('TIME')"
            "local newId = tonumber(time[1])*1000000 + tonumber(time[2]);"  // Timestamp in us
            "redis.call('HDEL', KEYS[1], ARGV[1]);"
            "return redis.call('HSET', '"+lastUpdateId+"', newId)";

    const std::string coherency_del =   
            "return redis.call('DEL', KEYS[1]);";

    const std::string exclusive_release_script = 
            // Gets the exclusive access timestamp
			"local current_timestamp = redis.call('HGET', KEYS[1], '"+lastUpdateId+"');"
            "local time = redis.call('TIME')"
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

    const std::string script_get_atomic_id =
            "local time = redis.call('TIME');"
            "local timestamp = time[1] * 1000000 + time[2];"
            // In case two operations are executed in the same microsecond, highly unlikely
            "if redis.call('GET', KEYS[1]) == timestamp then "  
                "return 0;"
            "end;"

            "redis.call('SET', KEYS[1], timestamp);"    // Sets the new timestamp
            "return timestamp;";

    /********************************************** Variables **********************************************/

    std::atomic<bool> b_finishThreads = false;                            // Flag to finish the threads

    std::mutex flushingMtx;                 // Mutex to prevent concurrent flushes
    std::mutex operationsBufferMtx;			// Mutex for the buffers
    Operations_buffer operationsBuffer;	    // Buffer to store the pendant operations. Must mantain order

    std::shared_mutex merge_buffer_mtx;     // Mutex for the merge buffer
    MERGE_BUFFER merge_buffer;       // Buffer to merge the hset operations

    std::shared_mutex merge_buffer_aux_mtx; // Mutex for the merge buffer aux
    MERGE_BUFFER merge_buffer_aux;           // Buffer to allow inspection of the merges beying written

    // Variables to write the merge buffer
    WRITE_SCRIPT_DATA batches_keys;
    WRITE_SCRIPT_DATA batches_args;
    std::condition_variable cv_write_merge_buffer;
    std::condition_variable cv_write_merge_buffer_finished;
    std::atomic<int> nThreadsWritingMergeBuffer;
    std::atomic<int> writingMergeBufferEpoch = 0;   // Epoch to prevent the threads from writing twice on an spurious wakeup
    std::mutex mtx_write_merge_buffer;

    /********************************************** Private methods **********************************************/

    bool coutDebug(int var)
    {
        std::cout << var << "\n";
        return true;
    }
    
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
        if (CLUSTER_MODE)
        {
            // Gets the string between { and }
            std::size_t pos1 = key.find("{");
            std::size_t pos2 = key.find("}");

            if (pos1 != std::string::npos && pos2 != std::string::npos)
                key = key.substr(pos1+1, pos2-pos1-1);

            return crc16_gpt(key.c_str(), key.size()) % 16384;
        }
        else {
            return 0;
        }
    }


    // Each BUFFER_LATENCY ms, the thread awakes and flushes the mainbuffer
    void flushThread()
    {
        while (!b_finishThreads)
        {
            flushBuffer();		// Empties the write buffer
            std::this_thread::sleep_for(std::chrono::milliseconds(BUFFER_LATENCY));			// Sleeps
        }
    }

    sw::redis::ConnectionPoolOptions getPoolOptions()
    {
        sw::redis::ConnectionPoolOptions poolOptions;

        if (nConnections < 1)
        {
            std::cerr << "The number of cache connections must be greater or equal to 1\n";
            throw std::invalid_argument("The number of cache connections must be greater or equal to 1\n");
        }

        poolOptions.size = nConnections;

        return poolOptions;
    }

    std::string getStreamId(const std::string &key)
    {
        return streamId+key;
    }

    // Adds a Redis_operation to a pipeline
	void addOperationToPipe (const Redis_operation &temp, Pipe &pipe)
	{
		switch (temp.op)												// Adds the operation to the pipeline
		{
			case RedisOp::hset:
            {
                pipe.eval(coherency_hset, 
                            {temp.param[0]}, 
                            {temp.param[1], temp.param[2]});

                break;
            }
			case RedisOp::sadd:
				pipe.sadd(temp.param[0], temp.param[1]);
				break;

			case RedisOp::srem:
				pipe.srem(temp.param[0], temp.param[1]);
				break;

			case RedisOp::hdel:
            {
                pipe.eval(coherency_hdel, 
                            {
                                getStreamId(temp.param[0]), 
                                temp.param[0], 
                            }, 
                            {temp.param[1]});
                break;
            }

			case RedisOp::del:
            {
                pipe.eval(coherency_del, 
                            {
                                getStreamId(temp.param[0]), 
                                temp.param[0]
                            }, 
                            {});
                break;
            }

            case RedisOp::publish:
				pipe.publish(temp.param[0], temp.param[1]);
				break;

			default:
                std::cerr << "Error, incorrect operation added to redis buffer\n";
				break;
		}
	}

    long long sendDirectOperation(const Redis_operation &operation)
    {
        long long result = -1;

        switch (operation.op)												// Adds the operation to the pipeline
		{
			case RedisOp::hset:
            {
                if (CLUSTER_MODE)
                {
                    result = redisCluster->eval<long long>(
                                coherency_hset, 
                                {operation.param[0]}, 
                                {operation.param[1], operation.param[2]});
                }
                else
                {
                    result = redisConnection->eval<long long>(
                                coherency_hset, 
                                {operation.param[0]}, 
                                {operation.param[1], operation.param[2]});
                }

                break;
            }
			case RedisOp::sadd:
                if (CLUSTER_MODE)
                    result = redisCluster->sadd(operation.param[0], operation.param[1]);
                else
                    result = redisConnection->sadd(operation.param[0], operation.param[1]);
				break;

			case RedisOp::srem:
                if (CLUSTER_MODE)
                    result = redisCluster->srem(operation.param[0], operation.param[1]);
                else
				    result = redisConnection->srem(operation.param[0], operation.param[1]);
				break;

			case RedisOp::hdel:
            {
                if (CLUSTER_MODE)
                {
                    result = redisCluster->eval<long long>(
                                coherency_hdel, 
                                {operation.param[0]}, 
                                {operation.param[1]});
                }
                else
                {
                    result = redisConnection->eval<long long>(
                                coherency_hdel, 
                                {operation.param[0]}, 
                                {operation.param[1]});
                }

				break;
            }

			case RedisOp::del:
            {
                if (CLUSTER_MODE)
                    result = redisCluster->eval<long long>(coherency_del, 
                                {operation.param[0]}, 
                                {});
                else
                    result = redisConnection->eval<long long>(coherency_del, 
                                {operation.param[0]}, 
                                {});
				break;
            }

            case RedisOp::publish:
                if (CLUSTER_MODE)
                    result = redisCluster->publish(operation.param[0], operation.param[1]);
                else
                    result = redisConnection->publish(operation.param[0], operation.param[1]);
				break;
            
            case RedisOp::exclusive_hset:  
            {
                if (operation.param[3] == "0") {
                    result = 0; // Get_exclusive_value failed, highly unlikely
                }
                else 
                {
                    try{
                        if (CLUSTER_MODE)
                        {
                            result = redisCluster->eval<long long>(
                                        exclusive_release_script, 
                                        {operation.param[0]}, 
                                        {
                                            operation.param[1], 
                                            operation.param[2],
                                            operation.param[3]
                                        });
                        }
                        else
                        {
                            result = redisConnection->eval<long long>(
                                        exclusive_release_script, 
                                        {operation.param[0]}, 
                                        {
                                            operation.param[1], 
                                            operation.param[2],
                                            operation.param[3]
                                        });
                        }
                    }
                    catch (const std::exception& e)
                    {
                        std::cerr << "Error in exclusive_hset: " << e.what() << '\n';
                    }
                }

                break;
            }

			default:
                std::cerr << "Error, incorrect operation added to redis buffer\n";
				break;
		}
        
        return result;
    }

    // Moves all the pendant write operations from the shared buffer to a local buffer
	Operations_buffer swapOperationsBuffer()
	{
   		Operations_buffer local_buffer;	            // Creates a local buffer

		operationsBufferMtx.lock();	                // Locks the buffer
		local_buffer.swap(operationsBuffer);		// Swaps the full queue with an empty one
        operationsBufferMtx.unlock();	            // Unlocks the buffer

		return local_buffer;
	}

    std::string getRedisIdInKey(const std::string &key)
    {
        if (CLUSTER_MODE)
            return key.substr(key.find("{")+1, key.find("}")-key.find("{")-1);  // Gets the string between brackets
        else
            return "";
    }

    
    void processSlot(   const int &slotId,
                        const std::unordered_map<std::string, Block> &slot, 
                        WRITE_SCRIPT_DATA &output_keys, 
                        WRITE_SCRIPT_DATA &output_args
                    )
    {
        std::vector<std::string> keys;
        std::vector<std::string> args;
        int batchSize = 0;

        for (auto &pair : slot)         // For each slot
        {
            const std::string &blockName = pair.first;

            keys.push_back(blockName);

            int nPendingWrites = pair.second.size();        // Number of variables to be written

            // If there is no space for all the pending writes
            if (nPendingWrites > MAX_BUFFER_SIZE - batchSize)     
                args.push_back(std::to_string(MAX_BUFFER_SIZE - batchSize));
            else
                args.push_back(std::to_string(nPendingWrites));
            
            for (auto &field : pair.second)                 // For each key
            {   
                args.push_back(field.first);    // Variable name
                args.push_back(field.second);   // Variable value
                nPendingWrites--;
                batchSize++;

                if (batchSize == MAX_BUFFER_SIZE) 
                {
                    output_keys[slotId].push_back(keys);
                    output_args[slotId].push_back(args);

                    keys.clear();
                    args.clear();
                    batchSize = 0;

                    if (nPendingWrites > 0)     // Save and reinitialize the block
                    {
                        keys.push_back(blockName);

                        // If there is no space for all the writes
                        if (nPendingWrites > MAX_BUFFER_SIZE)
                            args.push_back(std::to_string(MAX_BUFFER_SIZE));
                        else
                            args.push_back(std::to_string(nPendingWrites));
                    }
                }
            }
        }

        if (batchSize > 0)
        {
            output_keys[slotId].push_back(keys);
            output_args[slotId].push_back(args);
        }
    }

    void writeSlotToRedis(  const int &slotId,
                            SLOT_DATA_BATCHES &slot_batches_keys, 
                            SLOT_DATA_BATCHES &slot_batches_args)
    {
        std::optional<Pipe> pipe;
        //struct timeval begin, end;

        if (slot_batches_keys.size() > 0)
        {
            // Gets a pipeline to the node
            if (CLUSTER_MODE)
                pipe = redisCluster->pipeline(slot_batches_keys.begin()->at(0), false);
            else
                pipe = redisConnection->pipeline(false);


            auto keys_it = slot_batches_keys.begin();
            auto args_it = slot_batches_args.begin();

            for ( ; keys_it != slot_batches_keys.end(); keys_it++, args_it++)
            {
                pipe->eval(script_write_merge_buffer, 
                            keys_it->begin(),
                            keys_it->end(),
                            args_it->begin(),
                            args_it->end());
            }

            //gettimeofday(&begin, NULL);            // Updates the last update time

            // Executes the pipeline each time
            pipe->exec();
            pipe.reset();   // Releases the redis connection so a new pipe can be created

            //gettimeofday(&end, NULL);              // Updates the last update time

            //<std::cout << "Time to execute " << slot_batches_keys.size() << " slots, first size " << slot_batches_keys[0].size() << ": " 
            //<< ((end.tv_sec - begin.tv_sec) * 1000000 + double(end.tv_usec - begin.tv_usec)) << " us\n";
            //std::cout << "Writes executed per second: " << (double)temp_buffer.size() / ((end.tv_sec - begin.tv_sec) + double(end.tv_usec - begin.tv_usec) / 1000000) << "\n\n";
        }
    }

    void mergeBufferWritingThread(int slotId)
    {
        unsigned int local_epoch = writingMergeBufferEpoch+1;   // Will wait to the next epoch
        std::unique_lock lock(mtx_write_merge_buffer);

        nThreadsWritingMergeBuffer--;   // Thread ready to write

        while (!b_finishThreads)
        {
            while (!(local_epoch == writingMergeBufferEpoch   // Prevents the thread from writing twice on an spurious wakeup
                    || b_finishThreads))
            {
                // Waits until there is something to write or the program is finishing
                cv_write_merge_buffer.wait_for(lock, std::chrono::milliseconds(100));
            }
            
            if (!b_finishThreads)
            {
                // If there are writes to do
                if (batches_keys.find(slotId) != batches_keys.end())
                {
                    auto &keys = batches_keys[slotId];
                    auto &args = batches_args[slotId];

                    lock.unlock();

                    writeSlotToRedis(slotId, keys, args);   // Writes the slot to redis

                    lock.lock();

                    keys.clear();
                    args.clear();
                }

                nThreadsWritingMergeBuffer--;
                local_epoch++;  // When overflow it will be 0 and start again, no problem

                if (nThreadsWritingMergeBuffer == 0)
                {
                    cv_write_merge_buffer_finished.notify_all();
                }
            }
        }

        nThreadsWritingMergeBuffer--;   // Thread finished
    }

    long long writeMergeBuffer()
    {
        timeval t1, t2;
        MERGE_BUFFER temp_buffer;

        merge_buffer_aux_mtx.lock();

        merge_buffer_mtx.lock();
        temp_buffer.swap(merge_buffer);
        merge_buffer_mtx.unlock();

        merge_buffer_aux = temp_buffer;
        merge_buffer_aux_mtx.unlock();


        /***************************************** Data preprocesing *********************************/

        gettimeofday(&t1, NULL);            // Updates the last update time
        for (auto &slot : temp_buffer)
        {
            processSlot(slot.first, slot.second, batches_keys, batches_args);
        }

        gettimeofday(&t2, NULL);            // Updates the last update time

        //std::cout << "Time to process " << temp_buffer.size() << " slots: " << ((t2.tv_sec - t1.tv_sec) * 1000000 + double(t2.tv_usec - t1.tv_usec)) << " us\n";
        

        /***************************************** Data transfer *************************************/
        if (batches_keys.size() > 0)    // Prevents awaking the threads if there is nothing to write
        {   
            //timeval t1,t2;

            //gettimeofday(&t1, NULL);            // Updates the last update time
            if (CLUSTER_MODE)
            {
                nThreadsWritingMergeBuffer = CLUSTER_SIZE;
                writingMergeBufferEpoch++;
                cv_write_merge_buffer.notify_all();         // Notifies the writing threads

                // Waits for the threads to finish work
                std::unique_lock lock(mtx_write_merge_buffer);
                cv_write_merge_buffer_finished.wait(lock, [&](){return nThreadsWritingMergeBuffer == 0;});   
            }
            else
            {
                for (auto &slot : temp_buffer)  // There should only be one slot
                {
                    writeSlotToRedis(slot.first, batches_keys[slot.first], batches_args[slot.first]);
                    batches_keys[slot.first].clear();
                    batches_args[slot.first].clear();
                }
            }   

            //gettimeofday(&t2, NULL);            // Updates the last update time

            //std::cout << "Ms used: " << (t2.tv_sec - t1.tv_sec) * 1000 + (t2.tv_usec - t1.tv_usec) / 1000 << "\n"; 

            merge_buffer_aux_mtx.lock();
            merge_buffer_aux.clear();
            merge_buffer_aux_mtx.unlock();

            return 1;
        }
        else {
            return 0;
        }
    }


    // Adds a Redis_operation to the buffer
    void addOperationToBuffer (const Redis_operation &operation)
    {           
        int slot = getKeyClusterSlot(operation.param[0]);

        if (ALLOW_REORDERING && operation.op == RedisOp::hset) 
        {
            merge_buffer_mtx.lock();

            Block &block = merge_buffer[slot][operation.param[0]];  // Bottleneck on the whole hset operation
            block[operation.param[1]] = operation.param[2];         // --------------------------------------

            merge_buffer_mtx.unlock();

        }
        else {
            operationsBufferMtx.lock();	                        // Locks the buffer
            operationsBuffer[slot].push_back(operation);		// Adds the operation to the buffer
            operationsBufferMtx.unlock();	
        }
    }


    /************************************************************************************************************/
    /********************************************** Public methods **********************************************/
    /************************************************************************************************************/

    public:

    Redis_buffer(sw::redis::ConnectionOptions options,
                bool _USE_BUFFER, unsigned int _BUFFER_LATENCY,  
                bool _allow_reordering, bool _CLUSTER_MODE,
                unsigned int _MAX_BUFFER_SIZE=0, int nConcurrency=1,
                std::unordered_map<std::string, std::string> sample_cluster_keys = {}
    ):
        nConnections (nConcurrency + N_MINIMUM_CONNECTIONS + sample_cluster_keys.size()),
        MAX_BUFFER_SIZE((_MAX_BUFFER_SIZE == 0) ? 1000 : _MAX_BUFFER_SIZE) // MUST BE INITIALIZED HERE, BEFORE THE SCRIPTS INITIALIZATION
    {
        //coherency_script_sha = redisConnection->script_load(coherency_script);
        CLUSTER_MODE = _CLUSTER_MODE;
        ALLOW_REORDERING = _allow_reordering;
        USE_BUFFER = _USE_BUFFER;
        BUFFER_LATENCY = _BUFFER_LATENCY;
        CLUSTER_SIZE = sample_cluster_keys.size();

        if (CLUSTER_MODE)
            redisCluster = sw::redis::RedisCluster(options, getPoolOptions());
        else
            redisConnection = sw::redis::Redis(options, getPoolOptions());


        if (USE_BUFFER)			    // Creates updating thread
        {   
            nThreadsWritingMergeBuffer = CLUSTER_SIZE; // When the threads start, they will decrement the counter

			th_bufferThread = std::thread(&Redis_buffer::flushThread, this);

            for (auto sample_key : sample_cluster_keys) {
                th_write_merge_buffer.push_back(std::thread(&Redis_buffer::mergeBufferWritingThread, this, getKeyClusterSlot(sample_key.second)));
            }
        }
    }

    ~Redis_buffer()
    {
        if (USE_BUFFER)
        {   
            flushBuffer();

            b_finishThreads = true;

            th_bufferThread.join();
            cv_write_merge_buffer.notify_all();
            cv_write_merge_buffer_finished.notify_all();

            for (int i = 0; i < CLUSTER_SIZE; i++) {
                th_write_merge_buffer[i].join();
            }
        }
    }


    // Executes all pending operations in the buffer
    // Performance:
    //      WriteMergeBuffer has a moderate cost on processSlot (~50ms), caused by a computational bottleneck
    //      A code optimization would be moderately effective to improve memory usage, primarly
    //      The bottleneck is on writeSlotsToRedis, whick takes even seconds (bottleneck on redis)
    //
    //      Writing normal operations depends directly on redis pipeline, 
    //          so there are not many optimizations to do.
	void flushBuffer()
	{
        struct timeval begin, end, end2;
        
        std::unique_lock lock(flushingMtx);

		auto local_operationsBuffer = swapOperationsBuffer();	// Gets operations from the buffer


        if (!local_operationsBuffer.empty() || !merge_buffer.empty())
        {
            int mergesize = merge_buffer.size();

            gettimeofday(&begin, NULL);            // Updates the last update time

            if (merge_buffer.size() > 0)                        // Flushes the merge buffer
            {   
                writeMergeBuffer();
            }

            gettimeofday(&end, NULL);            // Updates the last update time

            if (local_operationsBuffer.size() > 0)
            {
                for (auto &pair : local_operationsBuffer)       // For each slot
                {
                    auto &slot = pair.second;
                    
                    // Must be deleted on each iteration to release redis connection
                    std::optional<Pipe> pipe;       

                    if (CLUSTER_MODE)
                        pipe = redisCluster->pipeline(slot[0].param[0], false);
                    else
                        pipe = redisConnection->pipeline(false);
                    
                    for (auto &operation : slot)			    // Gets all the operations from the buffer
                    {
                        addOperationToPipe(operation, *pipe);        // Adds the operation to the pipeline	
                    }

                    pipe->exec();
                }
            }

            
            double elapsed = (end.tv_sec - begin.tv_sec) + ((end.tv_usec - begin.tv_usec)/1000000.0);
            
            //std::cout << "Added " << local_operationsBuffer.size()+mergesize << " operations in " << elapsed << " seconds\n";
            //std::cout << "Operations added per second: " << (local_operationsBuffer.size()+mergesize)/elapsed << "\n";

            //std::string s_bin;
            //std::cin >> s_bin;
        }
	}

    // Executes the redis write
    long long executeOperation (const Redis_operation &operation)
    {
        long long result = 0;

        if (USE_BUFFER && operation.op != RedisOp::exclusive_hset)
        {
            addOperationToBuffer(operation);
            result = 1;
        }
        else
        {
            if (USE_BUFFER && !ALLOW_REORDERING)
            {
                flushBuffer();
                flushingMtx.lock();       // Prevents new operations while performing the sync write
            }

            result = sendDirectOperation(operation);

            if (USE_BUFFER && !ALLOW_REORDERING)
                flushingMtx.unlock();
        }

        return result;
    }


    // If the hash variable is being written locally, returns its value
    // Allows TSO and sequential consistency
    OptionalString hashVariableInBuffer (const std::string& blockName, const std::string& variableName)
    {
        std::shared_lock lock1 (merge_buffer_aux_mtx);	    // Locks the buffer
        std::shared_lock lock2 (merge_buffer_mtx);	        // Locks the buffer

        int slot = getKeyClusterSlot(blockName);

        if (merge_buffer[slot].find(blockName) != merge_buffer[slot].end()) {
            Block &block = merge_buffer[slot][blockName];
            if (block.find(variableName) != block.end()) {
                return block[variableName];
            }
        }

        if (merge_buffer_aux[slot].find(blockName) != merge_buffer_aux[slot].end()) {
            Block &block = merge_buffer_aux[slot][blockName];
            if (block.find(variableName) != block.end()) {
                return block[variableName];
            }
        }

        return OptionalString();
    }
};