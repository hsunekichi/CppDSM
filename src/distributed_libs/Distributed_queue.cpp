#pragma once

#include <string>
#include <memory>
#include <thread>


#include "../DB_cache.cpp"
#include "Distributed_variables.cpp"
#include "Distributed_vector.cpp"


template <typename T>
class Distributed_queue
{

};

template <>
class Distributed_queue<std::string>
{
    std::string queue_id;
    Distributed_variable<int> init_id; // Points to the first valid element
    Distributed_variable<int> end_id;  // Points to the first invalid element
    std::shared_ptr<DB_cache> pcache;

    unsigned int blockSize = 10000;

    int number_blocks()
    {
        return (size() + blockSize - 1) / blockSize;
    }

    std::string block_name(int blockIndex)
    {
        return queue_id + "_" + Distributed_variable<int>::to_string(blockIndex);
    }

    int i_to_block(int index)
    {
        return index / blockSize;
    }

    std::string i_to_block_name(int index)
    {
        int blockId = i_to_block(index);
        return block_name(blockId);
    }

public:
    Distributed_queue(std::string _queue_id, std::shared_ptr<DB_cache> _pcache)
    : end_id(_queue_id + "_end", _pcache), 
        init_id(_queue_id + "_init", _pcache)
    {
        pcache = _pcache;
        queue_id = _queue_id;
        
        init_id = 0;
        end_id = 0;
    }

    int size()
    {
        return end_id.get() - init_id.get();
    }

    bool empty()
    {
        return size() == 0;
    }

    bool preload()
    {
        int nBlocks = this->number_blocks();
        int initBlock = i_to_block(init_id.get());

        std::vector<std::string> keys;
        for (int i = initBlock; i < initBlock+nBlocks; i++)
        {
            keys.push_back(block_name(i));
        }

        pcache->preload(keys);

        return true;
    }

    bool clear()
    {
        // Delete all blocks
        int nBlocks = this->number_blocks();
        int initBlock = i_to_block(init_id.get());

        for (int i = initBlock; i < initBlock+nBlocks; i++)
        {
            pcache->del(block_name(i));
        }

        init_id = 0;
        end_id = 0;
        
        pcache->release_sync();

        return true;
    }

    bool push(std::string value)
    {
        int endId = this->end_id.get();

        auto temp = 
            pcache->hset(i_to_block_name(endId), 
            Distributed_variable<int>::to_string(endId), 
            Distributed_variable<std::string>::to_string(value));

        endId++;
        return temp;
    }

    bool push(std::vector<std::string> &values)
    {
        int endId = this->end_id.get();

        for (auto &value : values)
        {
            pcache->hset(i_to_block_name(endId), 
                Distributed_variable<int>::to_string(endId), 
                Distributed_variable<std::string>::to_string(value));
            endId++;
        }

        return true;
    }

    bool pop(std::string &value)
    {
        int init = init_id;
        if (init_id.get() == end_id.get())
            return false;

        value = get(init);
        pcache->hdel(i_to_block_name(init), Distributed_variable<int>::to_string(init));
        init_id++;

        return true;
    }

    std::string get (int index)
    {
        return pcache->hget(i_to_block_name(index), Distributed_variable<int>::to_string(index)).value();
    }

    int pop(std::vector<std::string> &values, int n)
    {
        int i = 0;
        int init = init_id.get();

        for (; i < n && init != end_id.get(); i++)
        {
            values.push_back(get(init));
            pcache->hdel(i_to_block_name(init), Distributed_variable<int>::to_string(init));
            init_id++;
        }

        return i;
    }

    bool pop()
    {
        int init = init_id;
        if (init_id.get() == end_id.get())
            return false;

        pcache->hdel(i_to_block_name(init), Distributed_variable<int>::to_string(init));
        init_id++;

        return true;
    }

    // Undetermined behavior if the queue is empty
    std::string front()
    {
        int init = init_id;
        return get(init);
    }

    // Undetermined behavior if the queue is empty
    std::string back()
    { 
        int end = end_id;
        return get(end-1);
    }
};


template <>
class Distributed_queue<int>
{
    std::string queue_id;
    Distributed_variable<int> init_id; // Points to the first valid element
    Distributed_variable<int> end_id;  // Points to the first invalid element
    std::shared_ptr<DB_cache> pcache;

public:
    Distributed_queue(std::string _queue_id, std::shared_ptr<DB_cache> _pcache)
    : end_id(_queue_id + "_end", _pcache), 
        init_id(_queue_id + "_init", _pcache)
    {
        pcache = _pcache;
        queue_id = _queue_id;
        
        init_id = 0;
        end_id = 0;
    }

    int size()
    {
        return end_id.get() - init_id.get();
    }

    bool empty()
    {
        return size() == 0;
    }

    bool clear()
    {
        pcache->del(queue_id);
        init_id = 0;
        end_id = 0;
        return true;
    }

    bool push(std::vector<int> &values)
    {
        // Copy binary data to string with memcpy
        std::string data;
        data.resize(values.size() * sizeof(int));
        memcpy(&data[0], &values[0], values.size() * sizeof(int));

        pcache->hset(queue_id, Distributed_variable<int>::to_string(end_id), data);
        end_id++;

        return true;
    }


    int pop(std::vector<int> &values)
    {
        int init = init_id.get();
        if (init == end_id.get())
            return 0;

        std::string data = pcache->hget(queue_id, Distributed_variable<int>::to_string(init)).value();
        int n = data.size() / sizeof(int);
        values.resize(n);
        memcpy(&values[0], &data[0], n * sizeof(int));

        pcache->hdel(queue_id, Distributed_variable<int>::to_string(init));
        init_id++;

        return n;
    }

};