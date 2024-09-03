#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <string>

#pragma once



enum class DB_opCode {hset, hsetnx, exclusive_hset, publish, set, sadd, srem, hdel, del, increment, hincrby, setnx};


struct DB_operation
{
    DB_opCode op;
    std::string param[4];
};

class DB_replies
{
public:
    // Ugly, but c++ does not allow virtual templated functions
    virtual std::optional<std::string> getString (int index) = 0;
    virtual std::vector<std::string> getStringVector (int index) = 0;
    virtual std::unordered_map<std::string, std::string> getStringHashMap (int index) = 0;
    virtual size_t size() = 0;
};

class DB_pipeline
{
    public:
    virtual void eval (const std::string &script, 
                        std::vector<std::string> keys, 
                        const std::vector<std::string> &args) = 0;
    virtual void sadd (std::string key, const std::string &value) = 0;
    virtual void srem (std::string key, const std::string &value) = 0;
    virtual void scard (std::string key) = 0;
    virtual void sismember (std::string key,const  std::string &value) = 0;
    virtual void set(std::string key, const std::string &value) = 0;
    virtual void publish (std::string channel, const std::string &message) = 0;
    virtual void increment (std::string key, std::string number) = 0;
    virtual void hincrby (std::string key, std::string field, std::string number) = 0;
    virtual void setnx (std::string key, const std::string &value) = 0;
    
    virtual void hget (std::string key, const std::string &field) = 0;
    virtual void hgetall (std::string key) = 0;

    virtual void hset(std::vector<std::string> keys, 
                const std::vector<std::string> &args) = 0;

    virtual void hsetnx(std::string key, std::string field, std::string value) = 0;

    virtual void hdel(std::vector<std::string> keys, 
                const std::vector<std::string> &args)  = 0;

    virtual void del(std::vector<std::string> keys, 
                const std::vector<std::string> &args)  = 0;

    virtual std::unique_ptr<DB_replies> exec () = 0;
};


class DB_connection 
{
    public:

    virtual bool cluster_mode() = 0;
    virtual unsigned int cluster_size() = 0;

    virtual int getNodeOfKey (const std::string &key) = 0;
    virtual std::vector<std::vector<std::string>> 
        orderByNode(const std::vector<std::string> &keys) = 0;

    virtual std::unique_ptr<DB_pipeline> pipeline(const std::string &key, 
            bool create_connection=true, 
            bool concurrent_ready=false) = 0;

    virtual std::string evalString (const std::string &script, 
            std::vector<std::string> keys, 
            const std::vector<std::string> &args) = 0;

    virtual long long evalInt (const std::string &script, 
            std::vector<std::string> keys, 
            const std::vector<std::string> &args) = 0;

    virtual std::vector<std::string> evalStringVector (const std::string &script, 
            std::vector<std::string> keys, 
            const std::vector<std::string> &args) = 0;
                    
    virtual long long sadd (std::string key, const std::string &value) = 0;
    virtual long long srem (std::string key, const std::string &value) = 0;
    virtual long long scard (std::string key) = 0;
    virtual bool sismember (std::string key, const std::string &value) = 0;
    virtual long long publish (std::string channel, const std::string &message) = 0;
    virtual long long set(std::string key, const std::string &value) = 0;
    virtual long long increment (std::string key, std::string number) = 0;
    virtual long long hincrby (std::string key, std::string field, std::string number) = 0;
    virtual long long setnx (std::string key, const std::string &value) = 0;

    virtual std::optional<std::string> get(std::string key) = 0;
    virtual std::optional<std::string> hget (std::string key, const std::string &field) = 0;
    virtual std::unordered_map<std::string, std::string> hgetall(std::string key) = 0;
    
    virtual long long smembers(std::string key, std::unordered_set<std::string> &output) = 0;
    virtual long long smembers(std::string key, std::set<std::string> &output) = 0;

    virtual std::vector<std::string> exclusive_hget(std::vector<std::string> keys, 
        const std::vector<std::string> &args) = 0;

    virtual long long write_merge_buffer(std::vector<std::string> keys, 
            const std::vector<std::string> &args) = 0;

    virtual long long hset(std::vector<std::string> keys, 
            const std::vector<std::string> &args) = 0;

    virtual long long hsetnx(std::string key, std::string field, std::string value) = 0;

    virtual long long hdel(std::vector<std::string> keys, 
            const std::vector<std::string> &args) = 0;

    virtual long long del(std::vector<std::string> keys, 
            const std::vector<std::string> &args) = 0;

    virtual std::string consume_message(std::string channel) = 0;

    virtual long long exclusive_release_hset(std::string blockId, 
        const std::string &key,
        const std::string &value, 
        const std::string &timestamp) = 0;

    virtual std::string ping() = 0;
};


