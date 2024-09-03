// Implementation of a std vector using a unordered map

#include <iostream>
#include <vector>
#include <unordered_map>
#include <string>

template <class T>
class Hash_vector
{
private:
    std::unordered_map<std::string, T> map;
    int i_size = 0;

public:
    Hash_vector() {}

    Hash_vector(int size)
    {
        this->i_size = size;
    }

    void push_back(T value)
    {
        map[std::to_string(i_size)] = value;
        i_size++;
    }
    
    void resize(int size)
    {
        i_size = size;
    }

    void pop_back()
    {
        map.erase(std::to_string(i_size));
        i_size--;
    }

    T &operator[](int index)
    {
        if (index >= i_size) {
            throw std::out_of_range("Index out of range");
        }

        if (map.find(std::to_string(index)) == map.end()) {
            map[std::to_string(index)] = T();
        }

        return map[std::to_string(index)];
    }

    int size() const
    {
        return i_size;
    }

    void clear()
    {
        map.clear();
        i_size = 0;
    }
};