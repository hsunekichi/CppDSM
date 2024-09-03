#pragma once

#include <string>
#include <memory>
#include <thread>
#include <optional>


#include "../DB_cache.cpp"


template <typename T>
class Distributed_variable
{

};


template <>
class Distributed_variable<std::string>
{
    std::string string_id;
    std::string value_id = "v";
    std::shared_ptr<DB_cache> pcache;
    bool default_on_miss = false;

public:
    Distributed_variable(std::string _string_id, std::shared_ptr<DB_cache> _pcache)
    {
        pcache = _pcache;
        string_id = _string_id;

        pcache->hsetnx(string_id, value_id, 
            Distributed_variable::to_string(""));  // Creates the variable if it does not exist
        
        pcache->release_sync();             // Warrantees consistency
    }

    // int proxy for other distributed classes
    //   If the variable does not exist, it is created
    Distributed_variable(std::string _string_id, std::string _value_id, std::shared_ptr<DB_cache> _pcache)
    {
        pcache = _pcache;
        string_id = _string_id;
        value_id = _value_id;
        default_on_miss = true;
    }

    bool set_var_id(const std::string &_string_id)
    {
        string_id = _string_id;
        return true;
    }

    bool exists() 
    {
        return pcache->hget(string_id, value_id) != std::nullopt;
    }

    static std::string to_string(std::string value)
    {
        return value;
    }

    static std::string from_string(std::string str)
    {
        return str;
    }

    // Returns the raw, unprotected value of the variable
    std::string get()
    {
        auto val = pcache->hget(string_id, value_id);

        if (val)
            return Distributed_variable::from_string(val.value());
        else if (default_on_miss)
            return "";
        else
            throw std::out_of_range("Key not found");
    }

    // Sets the raw value of the variable
    bool set(std::string value)
    {
        return pcache->hset(string_id, value_id, 
                Distributed_variable::to_string(value));
    }

    /*********************** Operators ***********************/

    bool operator=(std::string value)
    {
        return set(value);
    }

    operator std::string()
    {
        return get();
    }

    std::string str()
    {
        return get();
    }

    // Comparison operators
    bool operator==(std::string value)
    {
        return get() == value;
    }

    bool operator!=(std::string value)
    {
        return get() != value;
    }

    bool operator<(std::string value)
    {
        return get() < value;
    }

    bool operator>(std::string value)
    {
        return get() > value;
    }

    bool operator<=(std::string value)
    {
        return get() <= value;
    }

    // Swap
    friend void swap(Distributed_variable<std::string>& first, Distributed_variable<std::string>& second)
    {
        auto temp = first.get();
        first.set(second.get());
        second.set(temp);
    }
};


template <>
class Distributed_variable<int>
{
    std::string int_id;
    std::string value_id = "v";
    std::shared_ptr<DB_cache> pcache;
    bool default_on_miss = false;

public:

    Distributed_variable(const std::string &_int_id, std::shared_ptr<DB_cache> _pcache)
    {
        pcache = _pcache;
        int_id = _int_id;

        pcache->hsetnx(int_id, value_id, 
            Distributed_variable::to_string(0));  // Creates the variable if it does not exist
        
        pcache->full_sync();             // Warrantees consistency
    }

    // int proxy for other distributed classes
    //   If the variable does not exist, it is created
    Distributed_variable(const std::string &_int_id, const std::string &_value_id, std::shared_ptr<DB_cache> _pcache)
    {
        pcache = _pcache;
        int_id = _int_id;
        value_id = _value_id;
        default_on_miss = true;
    }

    bool exists() 
    {
        return pcache->hget(int_id, value_id) != std::nullopt;
    }

    bool set_var_id(const std::string &_int_id)
    {
        int_id = _int_id;
        return true;
    }

    static std::string to_string(int value)
    {
        // Convert int to ascii byte by byte
        std::string str;
        //str.resize(sizeof(int));
        //memcpy((void*)str.data(), &value, sizeof(int));

        str = std::to_string(value);

        return str;
    }

    static int from_string(const std::string &str)
    {
        // Convert ascii byte by byte to int
        int value;
        //memcpy(&value, str.data(), sizeof(int));
        value = std::stoi(str);

        return value;
    }

    // Returns the raw, unprotected value of the variable
    int get()
    {
        auto val = pcache->hget(int_id, value_id);

        if (val)
            return Distributed_variable::from_string(val.value());
        else if (default_on_miss)
            return 0;
        else
            throw std::out_of_range("Distributed variable "+int_id+" not found");
    }

    // Operator =
    bool operator=(int value)
    {
        return set(value);
    }

    // Sets the raw value of the variable
    bool set(int value)
    {
        return pcache->hset(int_id, value_id, 
                Distributed_variable::to_string(value));
    }

    // Incre    // Increments the atomic and returns the new value
    int increment(int value = 1)
    {
        int new_val = (int) pcache->hincrby(int_id, value_id, value);
    
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

    operator int()
    {
        return get();
    }

    operator std::string()
    {
        return std::to_string(get());
    }

    std::string str()
    {
        return std::to_string(get());
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

    bool operator==(Distributed_variable<int> value)
    {
        return get() == value.get();
    }

    bool operator!=(Distributed_variable<int> value)
    {
        return get() != value.get();
    }

    bool operator<(Distributed_variable<int> value)
    {
        return get() < value.get();
    }

    bool operator>(Distributed_variable<int> value)
    {
        return get() > value.get();
    }

    bool operator<=(Distributed_variable<int> value)
    {
        return get() <= value.get();
    }

    bool operator>=(Distributed_variable<int> value)
    {
        return get() >= value.get();
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

    int operator*(int value)
    {
        return get() * value;
    }

    int operator/(int value)
    {
        return get() / value;
    }

    int operator%(int value)
    {
        return get() % value;
    }

    int operator+(Distributed_variable<int> value)
    {
        return get() + value.get();
    }

    int operator-(Distributed_variable<int> value)
    {
        return get() - value.get();
    }

    int operator*(Distributed_variable<int> value)
    {
        return get() * value.get();
    }

    int operator/(Distributed_variable<int> value)
    {
        return get() / value.get();
    }

    int operator%(Distributed_variable<int> value)
    {
        return get() % value.get();
    }

    // Compound arithmetic operators

    bool operator*=(Distributed_variable<int> value)
    {
        return set(get() * value.get());
    }

    bool operator/=(Distributed_variable<int> value)
    {
        return set(get() / value.get());
    }

    bool operator%=(Distributed_variable<int> value)
    {
        return set(get() % value.get());
    }

    bool operator*=(int value)
    {
        return set(get() * value);
    }

    bool operator/=(int value)
    {
        return set(get() / value);
    }

    bool operator%=(int value)
    {
        return set(get() % value);
    }

    // Bitwise operators

    int operator&(int value)
    {
        return get() & value;
    }

    int operator|(int value)
    {
        return get() | value;
    }

    int operator^(int value)
    {
        return get() ^ value;
    }

    int operator&(Distributed_variable<int> value)
    {
        return get() & value.get();
    }

    int operator|(Distributed_variable<int> value)
    {
        return get() | value.get();
    }

    int operator^(Distributed_variable<int> value)
    {
        return get() ^ value.get();
    }

    // Compound assignment operators

    bool operator&=(int value)
    {
        return set(get() & value);
    }

    bool operator|=(int value)
    {
        return set(get() | value);
    }

    bool operator^=(int value)
    {
        return set(get() ^ value);
    }

    bool operator&=(Distributed_variable<int> value)
    {
        return set(get() & value.get());
    }

    bool operator|=(Distributed_variable<int> value)
    {
        return set(get() | value.get());
    }

    bool operator^=(Distributed_variable<int> value)
    {
        return set(get() ^ value.get());
    }

    // Shift operators

    int operator<<(int value)
    {
        return get() << value;
    }

    int operator>>(int value)
    {
        return get() >> value;
    }

    int operator<<(Distributed_variable<int> value)
    {
        return get() << value.get();
    }

    int operator>>(Distributed_variable<int> value)
    {
        return get() >> value.get();
    }

    // Logical operators

    bool operator&&(int value)
    {
        return get() && value;
    }

    bool operator||(int value)
    {
        return get() || value;
    }

    bool operator&&(Distributed_variable<int> value)
    {
        return get() && value.get();
    }

    bool operator||(Distributed_variable<int> value)
    {
        return get() || value.get();
    }
    

    // Swap
    friend void swap(Distributed_variable<int> first, Distributed_variable<int> second)
    {
        auto temp = first.get();
        first.set(second.get());
        second.set(temp);
    }
};

typedef Distributed_variable<int> Distributed_int;
typedef Distributed_variable<std::string> Distributed_string;
