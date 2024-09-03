// Implementation of a std vector using a unordered map

#include <iostream>
#include <vector>
#include <unordered_map>
#include <string>


template <class T>
class Hash_queue
{
private:
    std::vector<int> data;
    int init = 0;
    int end = 0;


public:
    Hash_queue() {}


    void push(std::vector<T> &values)
    {
        data.insert(data.end(), values.begin(), values.end());
        end += values.size();
    }

    void clear()
    {
        data.clear();
        init = 0;
        end = 0;
    }

    bool empty()
    {
        return init == end;
    }

    int size() const
    {
        return end - init;
    }


    int pop(std::vector<T> &values, int n=100000)
    {
        if (init == end)
            return 0;

        int n_values = std::min(n, end - init);
        values.resize(n_values);

        for (int i = 0; i < n_values; i++)
            values[i] = data[init + i];

        init += n_values;

        return n_values;
    }
};



/*
template <class T>
class Hash_queue
{
private:
    std::unordered_map<std::string, std::string> map;
    int init = 0;
    int end = 0;


public:
    Hash_queue() {}


    void push(std::vector<int> &values)
    {
        // Copy the data to the string
        std::string data;
        data.resize(values.size() * sizeof(int));
        memcpy(&data[0], &values[0], values.size() * sizeof(int));

        map[std::to_string(end)] = data;
        end++;
    }

    void clear()
    {
        map.clear();
        init = 0;
        end = 0;
    }

    bool empty()
    {
        return init == end;
    }

    int pop(std::vector<int> &values)
    {
        if (init == end)
            return 0;

        std::string data = map[std::to_string(init)];
        int n = data.size() / sizeof(int);
        values.resize(n);
        memcpy(&values[0], &data[0], n * sizeof(int));

        map.erase(std::to_string(init));
        init++;

        return n;
    }

    int size() const
    {
        return end - init;
    }
};

*/