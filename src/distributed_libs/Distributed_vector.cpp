#pragma once

#include <string>
#include <memory>
#include <thread>


#include "../DB_cache.cpp"
#include "Distributed_variables.cpp"


template <typename T>
class Distributed_vector
{
    std::string vector_id;
    Distributed_variable<int> i_size;
    std::shared_ptr<DB_cache> pcache;

    unsigned int blockSize = 10000;

    int number_blocks()
    {
        return (size() + blockSize - 1) / blockSize;
    }

    std::string block_name(int blockIndex)
    {
        return vector_id + "_" + Distributed_variable<int>::to_string(blockIndex);
    }

    std::string i_to_block(int index)
    {
        int blockId = index / blockSize;
        return block_name(blockId);
    }

public:
    Distributed_vector(std::string _vector_id, std::shared_ptr<DB_cache> _pcache)
    : i_size(_vector_id + "_size", _pcache)
    {
        pcache = _pcache;
        vector_id = _vector_id;
    }

    int size()
    {
        return i_size.get();
    }

    bool preload()
    {
        int nBlocks = this->number_blocks();

        std::vector<std::string> keys;
        for (int i = 0; i < nBlocks; i++)
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

        for (int i = 0; i < nBlocks; i ++)
        {
            pcache->del(block_name(i));
        }

        i_size = 0;
        
        pcache->release_sync();

        return true;
    }

    bool resize(int new_size)
    {
        if (new_size == 0)
            return clear();


        // Delete the extra data
        for (int i = new_size; i < size(); i++)
        {
            pcache->hdel(i_to_block(i), Distributed_variable<int>::to_string(i));
        }

        // If there is not enough data, 
        //  it will be created lazily when accessed 

        i_size = new_size;

        return true;
    }

    bool push_back(T value)
    {
        int size = i_size;

        auto temp = 
            pcache->hset(i_to_block(size), 
            Distributed_variable<int>::to_string(size), 
            Distributed_variable<T>::to_string(value));

        i_size++;
        return temp;
    } 

    T at(int index)
    {
        auto val = pcache->hget(i_to_block(index), 
                Distributed_variable<T>::to_string(index));

        if (val)
            return Distributed_variable<T>::from_string(val.value());

        else if (index < size())
            return T();

        else
            throw std::out_of_range("Key not found");
    }

    bool set(int index, T value)
    {
        return pcache->hset(i_to_block(index), 
            Distributed_variable<int>::to_string(index), 
            Distributed_variable<T>::to_string(value));
    }

    bool operator=(std::vector<T> vec)
    {
        // Delete the extra data
        for (int i = vec.size(); i < size(); i++)
        {
            pcache->hdel(i_to_block(i), Distributed_variable<int>::to_string(i));
        }

        // Set the new data
        for (int i = 0; i < vec.size(); i++)
        {
            set(i, vec[i]);
        }

        i_size = vec.size();

        return true;
    }

    bool operator=(Distributed_vector<T> vec)
    {
        // Delete the extra data
        for (int i = vec.size(); i < size(); i++)
        {
            pcache->hdel(i_to_block(i), Distributed_variable<int>::to_string(i));
        }

        // Set the new data
        for (int i = 0; i < vec.size(); i++)
        {
            set(i, vec[i]);
        }

        i_size = vec.size();

        return true;
    }

    // Operator [] (returns a distributed variable as a proxy)
    Distributed_variable<T> operator[](int index)
    {
        // Proxys create a default variable if it does not exist
        Distributed_variable<T> value (i_to_block(index), Distributed_variable<int>::to_string(index), pcache);
        return value;
    }

    // Iterator
    class iterator
    {
        int index;
        Distributed_vector<T>* pvec;

    public:
        iterator(Distributed_vector<T>* _pvec, int _index)
        {
            pvec = _pvec;
            index = _index;
        }

        iterator& operator++()
        {
            index++;
            return *this;
        }

        bool operator!=(const iterator& other)
        {
            return index != other.index;
        }

        T operator*()
        {
            return pvec->at(index);
        }
    };

    iterator begin()
    {
        return iterator(this, 0);
    }

    iterator end()
    {
        return iterator(this, size());
    }

    iterator insert(iterator it, T value)
    {
        int index = it.index;

        // Move the data
        for (int i = size(); i > index; i--)
        {
            set(i, at(i-1));
        }

        // Insert the new value
        set(index, value);

        i_size++;

        return iterator(this, index);
    }

    void insert_back(const std::vector<T> &data)
    {
        for (auto &value : data)
        {
            push_back(value);
        }
    }

    // Safely inserts a vector of data at the end. 
    //  Does not require a mutex, but other nodes will not have consistency 
    //  until a sync is performed AFTER this operation
    void insert_results_concurrently(std::vector<T> &data)
    {
        // Reserve space atomically
        int new_val = i_size.increment(data.size(), true);
        int init = new_val - data.size();

        // Write data concurrently
        for (int i = 0; i < data.size(); i++)
        {
            set(init + i, data[i]);
        }
    }

    void insert_results_concurrently(std::vector<long long> &data)
    {
        // Reserve space atomically
        int new_val = i_size.increment(data.size(), true);
        int init = new_val - data.size();

        // Write data concurrently
        for (int i = 0; i < data.size(); i++)
        {
            set(init + i, data[i]);
        }
    }

};


/****************** Function specializations ******************/

template <>
int Distributed_vector<int>::at(int index)
{
    auto val = pcache->hget(i_to_block(index), 
            Distributed_variable<int>::to_string(index));

    if (val)
        return Distributed_variable<int>::from_string(val.value());
    else
        throw std::out_of_range("Index "
                +std::to_string(index)+" not found");
}