#pragma once

#include <mutex>
#include <shared_mutex>
#include <optional>
#include <vector>
#include <atomic>
#include <thread>
#include <iostream>
#include <chrono>
#include <condition_variable>
#include <unordered_map>
//#include <Windows.h>
#include <queue>

#include "DB_connection.hpp"
#include "Redis_connection.cpp"


using Operations_buffer = std::vector<DB_operation>;	// Buffer type
using OptionalString = std::optional<std::string>;
using Block = std::unordered_map<std::string, std::string>;		// Block type
using MERGE_BUFFER = std::unordered_map<    // Map of blocks
                            std::string,    // Block name
                            Block>;         // Block data

using DATA_BATCH = std::pair<std::vector<std::string>,
                            std::vector<std::string>>;
using WRITE_DATA_BATCHES = std::queue<DATA_BATCH>;

class DB_buffer
{
    protected:

    /********************************************** Constants **********************************************/

    const unsigned int CLUSTER_N_SLOTS = 16384;

    const int N_MINIMUM_CONNECTIONS = 1;                // Number of additional connections the buffer needs internally
                                                            // 1 For the flushing thread

    int nConnections;					                // Number of connections to the Redis server. Has to be declared BEFORE the connection
    int MAX_BUFFER_SIZE;                                // Maximum size of the buffer
    bool USE_BUFFER, ALLOW_REORDERING;
    unsigned int BUFFER_LATENCY;                        // Time in milliseconds to wait before writing the buffer to DB
    std::thread th_bufferThread;
    std::vector<std::thread> th_write_merge_buffer;     // Threads to write the merge buffer to each cluster node

    std::unique_ptr<DB_connection> db;
    std::optional<sw::redis::Redis> redisConnection;	    // Redis object
    std::optional<sw::redis::RedisCluster> redisCluster;    // Redis object


    /********************************************** Variables **********************************************/

    std::atomic<bool> b_finishBuffer = false;                            // Flag to finish the buffer thread
    std::atomic<bool> b_finishWriterThreads = false;                            // Flag to finish the threads
    
    std::mutex flushingMtx;                 // Mutex to prevent concurrent flushes
    std::mutex operationsBufferMtx;			// Mutex for the buffers
    Operations_buffer operationsBuffer;	    // Buffer to store the pendant operations. Must mantain order

    std::shared_mutex merge_buffer_mtx;     // Mutex for the merge buffer
    MERGE_BUFFER merge_buffer;              // Buffer to merge the hset operations

    // Variables to write the merge buffer
    WRITE_DATA_BATCHES write_data_batches;
    std::condition_variable cv_write_merge_buffer;
    std::condition_variable cv_write_merge_buffer_finished;
    int working_flushing_threads = 0;
    std::mutex mtx_write_merge_buffer;

    /********************************************** Private methods **********************************************/

    bool coutDebug(int var)
    {
        std::cout << var << "\n";
        return true;
    }
    
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


    // Each BUFFER_LATENCY ms, the thread flushes the main buffer
    void flushThread()
    {
        // Get time
        auto next_update = std::chrono::high_resolution_clock::now() 
                + std::chrono::milliseconds(BUFFER_LATENCY);

        while (!b_finishBuffer)
        {
            auto now = std::chrono::high_resolution_clock::now();
            if (now >= next_update)
            {
                flushBuffer();		// Empties the write buffer

                // Add BUFFER_LATENCY ms to nextUpdate
                next_update = now + std::chrono::milliseconds(BUFFER_LATENCY);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(std::min(BUFFER_LATENCY, 1000u)));  // Sleeps max 1s
        }
    }

    Operations_buffer newOperationsBuffer()
    {
        int cluster_size = db->cluster_size();
        Operations_buffer temp_buffer(cluster_size);

        return temp_buffer;
    }

    // Adds a DB_operation to a pipeline
	void addOperationToPipe (const DB_operation &temp, std::unique_ptr<DB_pipeline> &pipe)
	{
		switch (temp.op)        // Adds the operation to the pipeline
		{
			case DB_opCode::hset:
                pipe->hset({temp.param[0]}, {temp.param[1], temp.param[2]});
                break;
            
            case DB_opCode::hsetnx:
                pipe->hsetnx(temp.param[0], temp.param[1], temp.param[2]);
                break;
            
			case DB_opCode::sadd:
				pipe->sadd(temp.param[0], temp.param[1]);
				break;

			case DB_opCode::srem:
				pipe->srem(temp.param[0], temp.param[1]);
				break;

            case DB_opCode::set:
                pipe->set(temp.param[0], temp.param[1]);
                break;

			case DB_opCode::hdel:
            {
                pipe->hdel({temp.param[0]}, 
                            {temp.param[1]});
                break;
            }

			case DB_opCode::del:
            {
                pipe->del({temp.param[0]}, {});
                break;
            }

            case DB_opCode::increment:
                pipe->increment(temp.param[0], temp.param[1]);
                break;
            
            case DB_opCode::hincrby:
                pipe->hincrby(temp.param[0], temp.param[1], temp.param[2]);
                break;

            case DB_opCode::setnx:
                pipe->setnx(temp.param[0], temp.param[1]);
                break;

            case DB_opCode::publish:
				pipe->publish(temp.param[0], temp.param[1]);
				break;

			default:
                std::cerr << "Error, incorrect operation added to DB pipeline\n";
				break;
		}
	}

    long long sendDirectOperation(const DB_operation &operation)
    {
        long long result = -1;

        switch (operation.op)											
		{
			case DB_opCode::hset:
            {
                result = db->hset({operation.param[0]}, 
                            {operation.param[1], operation.param[2]});
                
                break;
            }
            case DB_opCode::hsetnx:
            {
                result = db->hsetnx(operation.param[0], operation.param[1], operation.param[2]);
                break;
            }

			case DB_opCode::sadd:
                result = db->sadd(operation.param[0], operation.param[1]);
				break;

			case DB_opCode::srem:
                result = db->srem(operation.param[0], operation.param[1]);
				break;

            case DB_opCode::set:
                result = db->set(operation.param[0], operation.param[1]);
                break;

			case DB_opCode::hdel:
            {
                result = db->hdel({operation.param[0]}, 
                            {operation.param[1]});

				break;
            }

			case DB_opCode::del:
            {
                result = db->del({operation.param[0]}, 
                            {});

				break;
            }

            case DB_opCode::publish:
                result = db->publish(operation.param[0], operation.param[1]);
				break;

            case DB_opCode::increment:
                result = db->increment(operation.param[0], operation.param[1]);
                break;

            case DB_opCode::hincrby:
                result = db->hincrby(operation.param[0], operation.param[1], operation.param[2]);
                break;

            case DB_opCode::setnx:
                result = db->setnx(operation.param[0], operation.param[1]);
                break;
            
            case DB_opCode::exclusive_hset:  
            {
                if (operation.param[3] == "0") {
                    result = 0; // Get_exclusive_value failed, highly unlikely
                }
                else 
                {
                    try{
                        result = db->exclusive_release_hset(operation.param[0], 
                                    operation.param[1], 
                                    operation.param[2],
                                    operation.param[3]);
                    }
                    catch (const std::exception& e)
                    {
                        std::cerr << "Error in exclusive_hset: " << e.what() << '\n';
                    }
                }

                break;
            }

			default:
                std::cerr << "Error, incorrect operation added to DB buffer\n";
				break;
		}
        
        return result;
    }

    // Moves all the pendant write operations from the shared buffer to a local buffer
	Operations_buffer swapOperationsBuffer()
	{
   		Operations_buffer local_buffer = newOperationsBuffer();	            // Creates a local buffer

		operationsBufferMtx.lock();	                // Locks the buffer
		local_buffer.swap(operationsBuffer);		// Swaps the full queue with an empty one
        operationsBufferMtx.unlock();	            // Unlocks the buffer

		return local_buffer;
	}

    
    void generate_data_batches(const std::unordered_map<std::string, Block> &nodeData, 
                        WRITE_DATA_BATCHES &output
                    )
    {
        std::vector<std::string> keys;
        std::vector<std::string> args;
        int batchSize = 0;    

        for (auto &pair : nodeData)         // For each block
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
                    auto pair = std::make_pair(keys, args);
                    output.push(pair);

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
            auto pair = std::make_pair(keys, args);
            output.push(pair);
        }
    }


    void mergeBufferWritingThread()
    {
        std::unique_lock lock(mtx_write_merge_buffer);

        while (!b_finishWriterThreads)
        {
            // Waits until there is something to write or the program is finishing
            cv_write_merge_buffer.wait(lock, [&](){return write_data_batches.size() > 0
                                                            || b_finishWriterThreads;});

            working_flushing_threads++;

            // While there are writes to do
            while (write_data_batches.size() > 0)
            {
                auto data = write_data_batches.front();
                write_data_batches.pop();

                lock.unlock();

                auto &keys = data.first;
                auto &args = data.second;

                db->write_merge_buffer(keys, args); // Writes the data in the DB

                lock.lock();

                keys.clear();
                args.clear();
            }

            working_flushing_threads--;

            // This must be done atomically with the wait for, 
            //  as an other flushBuffer call could notify this thread before it is waiting
            if (working_flushing_threads == 0)
                cv_write_merge_buffer_finished.notify_all();
        }
    }

    std::vector<MERGE_BUFFER>
        separate_data_per_node(MERGE_BUFFER &data)
    {
        int cluster_size = db->cluster_size();
        std::vector<MERGE_BUFFER> separated_data(cluster_size);

        for (auto &block : data)
        {
            int node_id = db->getNodeOfKey(block.first);
            auto &block_data = separated_data[node_id][block.first];

            block_data.swap(block.second);
        }

        return separated_data;
    }

    std::vector<Operations_buffer>
        separate_data_per_node(Operations_buffer &data)
    {
        int cluster_size = db->cluster_size();
        std::vector<Operations_buffer> separated_data(cluster_size);

        for (auto &operation : data)
        {
            int node_id = db->getNodeOfKey(operation.param[0]);
            auto &node_data = separated_data[node_id];

            node_data.push_back(operation);
        }

        return separated_data;
    }


    long long writeMergeBuffer()
    {
        timeval t1, t2;
        MERGE_BUFFER temp_buffer;
        int cluster_size = db->cluster_size();

        merge_buffer_mtx.lock();
        temp_buffer.swap(merge_buffer);
        merge_buffer_mtx.unlock();

        /***************************************** Data preprocesing *********************************/
        
        auto nodes_data = separate_data_per_node(temp_buffer);

        std::unique_lock data_batches_lock(mtx_write_merge_buffer);

        for (int node_id = 0; node_id < cluster_size; node_id++)
        {
            generate_data_batches(nodes_data[node_id], write_data_batches);
        }

        data_batches_lock.unlock();
        

        /***************************************** Data transfer *************************************/
        if (write_data_batches.size() > 0)    // Prevents awaking the threads if there is nothing to write
        {   
            //gettimeofday(&t1, NULL);            // Updates the last update time
            cv_write_merge_buffer.notify_all();         // Notifies the writing threads

            // Waits for the threads to finish work
            data_batches_lock.lock();
            cv_write_merge_buffer_finished.wait(data_batches_lock, [&](){return write_data_batches.size() == 0
                                                            && working_flushing_threads == 0;});   
            data_batches_lock.unlock();
            //gettimeofday(&t2, NULL);            // Updates the last update time

            //std::cout << "Ms used: " << (t2.tv_sec - t1.tv_sec) * 1000 + (t2.tv_usec - t1.tv_usec) / 1000 << "\n"; 

            return 1;
        }
        else {
            return 0;
        }
    }

    void writeOperationsBuffer()
    {
        auto local_operationsBuffer = swapOperationsBuffer();	// Gets operations from the buffer

        //std::cout << "Writing " << local_operationsBuffer.size() << " operations\n";

        if (local_operationsBuffer.size() > 0)
        {
            auto write_data = separate_data_per_node(local_operationsBuffer);

            for (auto &node_data : write_data)       // For each node
            {    
                if (node_data.size() > 0) 
                {
                    // Must be deleted on each iteration to release db connection  
                    auto pipe = db->pipeline(node_data[0].param[0], false);        // Gets a pipeline to the node

                    for (auto &operation : node_data)			    // Gets all the operations from the buffer
                    {
                        addOperationToPipe(operation, pipe);        // Adds the operation to the pipeline	
                    }

                    pipe->exec();
                }
            }
        }
    }


    // Adds a DB_operation to the buffer
    void addOperationToBuffer (const DB_operation &operation)
    {           
        if (ALLOW_REORDERING && operation.op == DB_opCode::hset) 
        {
            merge_buffer_mtx.lock();

            Block &block = merge_buffer[operation.param[0]];
            block[operation.param[1]] = operation.param[2];
            
            merge_buffer_mtx.unlock();
        }
        else {
            operationsBufferMtx.lock();	                        // Locks the buffer
            operationsBuffer.push_back(operation);		// Adds the operation to the buffer
            operationsBufferMtx.unlock();	
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

    /************************************************************************************************************/
    /********************************************** Public methods **********************************************/
    /************************************************************************************************************/

    public:

    DB_buffer(sw::redis::ConnectionOptions options,
                bool _USE_BUFFER, unsigned int _BUFFER_LATENCY,  
                bool _allow_reordering,
                unsigned int _MAX_BUFFER_SIZE, int nConcurrency=1
    ):
        nConnections (nConcurrency + N_MINIMUM_CONNECTIONS),
        MAX_BUFFER_SIZE(_MAX_BUFFER_SIZE)
    {
        db = std::make_unique<Redis_connection>(options, getPoolOptions());

        ALLOW_REORDERING = _allow_reordering;
        USE_BUFFER = _USE_BUFFER;
        BUFFER_LATENCY = _BUFFER_LATENCY;

        if (USE_BUFFER)			    // Creates updating thread
        {   
            int cluster_size = db->cluster_size();

			th_bufferThread = std::thread(&DB_buffer::flushThread, this);
            working_flushing_threads = 0;

            for (int id = 0; id < cluster_size; id++) {
                th_write_merge_buffer.push_back(std::thread(&DB_buffer::mergeBufferWritingThread, this));
            }
        }
    }

    ~DB_buffer()
    {
        if (USE_BUFFER)
        {   
            flushBuffer();

            b_finishBuffer = true;
            th_bufferThread.join();

            b_finishWriterThreads = true;

            cv_write_merge_buffer.notify_all();
            cv_write_merge_buffer_finished.notify_all();

            int cluster_size = db->cluster_size();
            for (int i = 0; i < cluster_size; i++) {
                th_write_merge_buffer[i].join();
            }
        }
    }


    // Executes all pending operations in the buffer
    // Performance:
    //      WriteMergeBuffer has a moderate cost on processSlot (~50ms), caused by a computational bottleneck
    //      A code optimization would be moderately effective to improve memory usage, primarly.
    //      The bottleneck is on sending operations to DB, which takes even seconds (bottleneck on DB)
    //
    //      Writing normal operations depends directly on DB pipeline, 
    //          so there are not many optimizations to do.
	void flushBuffer()
	{
        struct timeval begin, end, end2;
        
        std::unique_lock flushingLock(flushingMtx);
        std::shared_lock merge_buffer_lock(merge_buffer_mtx);

        if (!operationsBuffer.empty() || !merge_buffer.empty())
        {
            merge_buffer_lock.unlock();

            // Flushes the merge buffer  
            writeMergeBuffer();              // Will write data both in db and in the cache
            writeOperationsBuffer();      
        }
	}

    // Executes the DB write
    long long executeOperation (const DB_operation &operation, bool force_direct=false)
    {
        long long result = 0;

        if (!force_direct &&
            USE_BUFFER &&
            operation.op != DB_opCode::exclusive_hset)
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
    /*
    OptionalString hashVariableInBuffer (const std::string& blockName, const std::string& variableName)
    {
        std::shared_lock lock1 (merge_buffer_aux_mtx);	    // Locks the buffer
        std::shared_lock lock2 (merge_buffer_mtx);	        // Locks the buffer

        int node_id = db->getNodeOfKey(blockName);

        if (merge_buffer[node_id].find(blockName) != merge_buffer[node_id].end()) 
        {
            Block &block = merge_buffer[node_id][blockName];

            if (block.find(variableName) != block.end()) {
                return block[variableName];
            }
        }

        if (merge_buffer_aux.size() > 0 &&
            merge_buffer_aux[node_id].find(blockName) != merge_buffer_aux[node_id].end()) 
        {
            Block &block = merge_buffer_aux[node_id][blockName];

            if (block.find(variableName) != block.end()) {
                return block[variableName];
            }
        }

        return OptionalString();
    }
    */
};