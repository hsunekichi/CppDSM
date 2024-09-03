#pragma once

#include <string>
#include <memory>
#include <thread>


#include "../DB_cache.cpp"



// WARNING: This mutex is NOT failure tolerant, 
//  it will be blocked forever if the process that locked it crashes
//  without unlocking it
class Distributed_mutex
{
    const std::string mtx_key = "lck";
    std::string mtx_id;

    std::shared_ptr<DB_cache> pcache;
    std::mutex mtx;

    // Statistics
    std::atomic<unsigned int> nFallos = 0;
    std::atomic<unsigned int> nItEsperando = 0;

public:

    Distributed_mutex(std::string mutex_name, std::shared_ptr<DB_cache> _pcache)
    {
        pcache = _pcache;
        mtx_id = mutex_name;

        pcache->hsetnx(mtx_id, mtx_key, "u");    // Creates the mutex if it does not exist
        pcache->full_sync();               // Warrantees consistency
    }

    Distributed_mutex(const Distributed_mutex&) = delete;
    Distributed_mutex(const Distributed_mutex&&) = delete;


    bool lock()
    {
        bool acquired = false;

        while (!acquired) // Until the key has been obtained atomically
        {
            bool locked = true;

            while (locked)  // Waits until the key is unlocked
            {   
                auto mtx_value = pcache->hget_exclusive_acquire(mtx_id, mtx_key);    // Gets mtx key exclusively

                if (mtx_value)
                    locked = (mtx_value.value() == "l");
                else 
                    std::cerr << "El mutex no existe" << std::endl;

                if (locked) {
                    nItEsperando++;
                    std::this_thread::yield();
                }
            }
    
            if (pcache->hset_exclusive_release(mtx_id, mtx_key, "l"))  // Removes the key
                acquired = true;
            else
                nFallos++;
        }

        return true;
    }

    bool unlock()
    {
        pcache->release_sync();              // Warrantees consistency
        pcache->hset(mtx_id, mtx_key, "u");     // Writes the key
        pcache->release_sync();              // Forces the release as soon as posible
        
        return true;
    }

    unsigned long long getFallos() const
    {
        return nFallos;
    }

    unsigned long long getItEsperando() const
    {
        return nItEsperando;
    }
};


template <typename T>
class Distributed_atomic
{

};

// Template for int
template <>
class Distributed_atomic<int>
{
    std::string atomic_id;
    std::shared_ptr<DB_cache> pcache;

public:

    Distributed_atomic(std::string _atomic_id, std::shared_ptr<DB_cache> _pcache)
    {
        pcache = _pcache;
        atomic_id = _atomic_id;

        pcache->setnx(atomic_id, "0");       // Creates the mutex if it does not exist
        pcache->full_sync();   // Warrantees consistency
    }

    operator int()
    {
        return get();
    }

    // Returns the raw, unprotected value of the atomic
    int get()
    {
        pcache->acquire_sync(true);   // Warrantees consistency
        auto val = pcache->get(atomic_id);

        if (val)
            return stoi(val.value());
        else
            throw std::out_of_range("Key not found");
    }

    // Sets the raw value of the atomic
    bool set(int value)
    {
        pcache->set(atomic_id, std::to_string(value));
        pcache->release_sync();  // Warrantees consistency

        return true;
    }

    bool operator=(int value)
    {
        return set(value);
    }

    // Increments the atomic and returns the new value
    int increment(int value = 1)
    {
        int new_val = (int) pcache->increment(atomic_id, value);
    
        return new_val;
    }

    // Operator ++
    int operator++()
    {
        return increment();
    }

    // Operator ++
    int operator++(int)
    {
        return increment()-1;
    }

    // Decrements the atomic
    bool decrement(int value = 1)
    {
        return increment(-value);
    }

    // Operator --
    bool operator--()
    {
        return decrement();
    }

    // Operator --
    bool operator--(int)
    {
        return decrement()+1;
    }

    // Adds a value to the atomic
    bool add(int value)
    {
        return increment(value);
    }

    // Operator +=
    bool operator+=(int value)
    {
        return add(value);
    }

    // Substracts a value to the atomic
    bool substract(int value)
    {
        return decrement(value);
    }

    // Operator -=
    bool operator-=(int value)
    {
        return substract(value);
    }

    // Arithmetic operators
    int operator+(int value)
    {
        return get() + value;
    }

    int operator-(int value)
    {
        return get() - value;
    }

    int operator/(int value)
    {
        return get() / value;
    }

    // Multiplication
    int operator*(int value)
    {
        return get() * value;
    }

    // Modulus
    int operator%(int value)
    {
        return get() % value;
    }

    // Comparison operators
    bool operator==(int value)
    {
        return get() == value;
    }

    bool operator!=(int value)
    {
        return get() != value;
    }

    bool operator<(int value)
    {
        return get() < value;
    }

    bool operator>(int value)
    {
        return get() > value;
    }

    bool operator<=(int value)
    {
        return get() <= value;
    }

    bool operator>=(int value)
    {
        return get() >= value;
    }
};