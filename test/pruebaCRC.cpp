#include "redis++.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <atomic>

using Attrs = std::unordered_map<std::string, std::string>;
using Item = std::pair<std::string, std::optional<Attrs>>;
using ItemStream = std::vector<Item>;

using namespace std;


std::string makeFixedLength(const int i, const int length)
{
    if (true)
    {
        std::ostringstream ostr;

        ostr << std::setfill('0') << std::setw(length) << i;

        return ostr.str();
    }
    
    return to_string(i);
}

int pipeSize = 1000;
string script_block_size = "1000";

int nBlocks = 10000000;
int nVars = 1;
int nThreads = 16;
int nReps = 1;
int s_len = 10;
// Per hset value size = 7 + 2*slen


static const uint16_t crctable[256] =
{
    0x0000, 0x1189, 0x2312, 0x329B, 0x4624, 0x57AD, 0x6536, 0x74BF,
    0x8C48, 0x9DC1, 0xAF5A, 0xBED3, 0xCA6C, 0xDBE5, 0xE97E, 0xF8F7,
    0x0919, 0x1890, 0x2A0B, 0x3B82, 0x4F3D, 0x5EB4, 0x6C2F, 0x7DA6,
    0x8551, 0x94D8, 0xA643, 0xB7CA, 0xC375, 0xD2FC, 0xE067, 0xF1EE,
    0x1232, 0x03BB, 0x3120, 0x20A9, 0x5416, 0x459F, 0x7704, 0x668D,
    0x9E7A, 0x8FF3, 0xBD68, 0xACE1, 0xD85E, 0xC9D7, 0xFB4C, 0xEAC5,
    0x1B2B, 0x0AA2, 0x3839, 0x29B0, 0x5D0F, 0x4C86, 0x7E1D, 0x6F94,
    0x9763, 0x86EA, 0xB471, 0xA5F8, 0xD147, 0xC0CE, 0xF255, 0xE3DC,
    0x2464, 0x35ED, 0x0776, 0x16FF, 0x6240, 0x73C9, 0x4152, 0x50DB,
    0xA82C, 0xB9A5, 0x8B3E, 0x9AB7, 0xEE08, 0xFF81, 0xCD1A, 0xDC93,
    0x2D7D, 0x3CF4, 0x0E6F, 0x1FE6, 0x6B59, 0x7AD0, 0x484B, 0x59C2,
    0xA135, 0xB0BC, 0x8227, 0x93AE, 0xE711, 0xF698, 0xC403, 0xD58A,
    0x3656, 0x27DF, 0x1544, 0x04CD, 0x7072, 0x61FB, 0x5360, 0x42E9,
    0xBA1E, 0xAB97, 0x990C, 0x8885, 0xFC3A, 0xEDB3, 0xDF28, 0xCEA1,
    0x3F4F, 0x2EC6, 0x1C5D, 0x0DD4, 0x796B, 0x68E2, 0x5A79, 0x4BF0,
    0xB307, 0xA28E, 0x9015, 0x819C, 0xF523, 0xE4AA, 0xD631, 0xC7B8,
    0x48C8, 0x5941, 0x6BDA, 0x7A53, 0x0EEC, 0x1F65, 0x2DFE, 0x3C77,
    0xC480, 0xD509, 0xE792, 0xF61B, 0x82A4, 0x932D, 0xA1B6, 0xB03F,
    0x41D1, 0x5058, 0x62C3, 0x734A, 0x07F5, 0x167C, 0x24E7, 0x356E,
    0xCD99, 0xDC10, 0xEE8B, 0xFF02, 0x8BBD, 0x9A34, 0xA8AF, 0xB926,
    0x5AFA, 0x4B73, 0x79E8, 0x6861, 0x1CDE, 0x0D57, 0x3FCC, 0x2E45,
    0xD6B2, 0xC73B, 0xF5A0, 0xE429, 0x9096, 0x811F, 0xB384, 0xA20D,
    0x53E3, 0x426A, 0x70F1, 0x6178, 0x15C7, 0x044E, 0x36D5, 0x275C,
    0xDFAB, 0xCE22, 0xFCB9, 0xED30, 0x998F, 0x8806, 0xBA9D, 0xAB14,
    0x6CAC, 0x7D25, 0x4FBE, 0x5E37, 0x2A88, 0x3B01, 0x099A, 0x1813,
    0xE0E4, 0xF16D, 0xC3F6, 0xD27F, 0xA6C0, 0xB749, 0x85D2, 0x945B,
    0x65B5, 0x743C, 0x46A7, 0x572E, 0x2391, 0x3218, 0x0083, 0x110A,
    0xE9FD, 0xF874, 0xCAEF, 0xDB66, 0xAFD9, 0xBE50, 0x8CCB, 0x9D42,
    0x7E9E, 0x6F17, 0x5D8C, 0x4C05, 0x38BA, 0x2933, 0x1BA8, 0x0A21,
    0xF2D6, 0xE35F, 0xD1C4, 0xC04D, 0xB4F2, 0xA57B, 0x97E0, 0x8669,
    0x7787, 0x660E, 0x5495, 0x451C, 0x31A3, 0x202A, 0x12B1, 0x0338,
    0xFBCF, 0xEA46, 0xD8DD, 0xC954, 0xBDEB, 0xAC62, 0x9EF9, 0x8F70
};

uint16_t // Returns Calculated CRC value
CalculateCRC16(
    uint16_t crc,      // Seed for CRC calculation
    const uint8_t *c_ptr, // Pointer to byte array to perform CRC on
    size_t len)        // Number of bytes to CRC
{
    const uint8_t *c = c_ptr;

    while (len--)
        crc = (crc << 8) ^ crctable[((crc >> 8) ^ *c++)];

    return crc;
}


uint16_t crc16_gpt(const char *buf, int len) {
    uint16_t crc = 0;
    for (int i = 0; i < len; ++i) {
        crc = crc ^ ((uint16_t)buf[i] << 8);
        for (int j = 0; j < 8; ++j) {
            if (crc & 0x8000)
                crc = (crc << 1) ^ 0x1021;
            else
                crc <<= 1;
        }
    }
    return crc;
}


    const std::string script_write_merge_buffer =  
        "local return_value = 1;" 
        "local lastUpdateId = 'lastUpdateId';"
        "local batch_size = "+std::to_string(1000)+";"
        "local i_args_init = 1;"                        // Iterator for the ARGV array

        "for i_block = 1, #KEYS, 2 do "                 // For each block
            "local nKeys = ARGV[i_args_init];"
            "i_args_init = i_args_init+1;"
            "local i_args_end = i_args_init+(nKeys-1)*2;"

            "local block_key = KEYS[i_block];"             
            "local stream_key = KEYS[i_block+1];"
            
            "if redis.call('EXISTS', block_key) ~= 0 then "  // If the block does not exist, when it is moved to another page or deleted
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

                    "local newId = redis.call('XADD', stream_key, '*', unpack(values));"
                    "redis.call('HSET', block_key, lastUpdateId, newId, unpack(values));"
                "end;"

            "else "
                "return_value = 2;" // If the block does not exists, returns 2 so the TLB will be updated
            "end;"

            "i_args_init = i_args_end+2;"
        "end;"
        
        "return return_value";


/*

    vector<string> keys;
    vector<string> args;

    keys.push_back("lastUpdateId");
    args.push_back("");                     // Padding

    
    for (int i_block = 0; i_block < nBlocks; i_block++)         // For each block
    {
        keys.push_back("hset:"+makeFixedLength(i_block, s_len));
        keys.push_back("stream:hset:"+makeFixedLength(i_block, s_len));
        keys.push_back(makeFixedLength(nVars, s_len)); // Number of keys

        args.push_back("");                 // Padding
        args.push_back("");                 // Padding
        args.push_back("");                 // Padding

        for (int i_var = 0; i_var < nVars; i_var++)             // For each key
        {
            keys.push_back("key:"+makeFixedLength(i_block, s_len)+":"+makeFixedLength(i_var, s_len));
            args.push_back("value:"+makeFixedLength(i_block, s_len)+":"+makeFixedLength(i_var, s_len));
            nWrites++;
        }

        if (nWrites > pipeSize || i_block == nBlocks-1)
        {
            try {
                pipe.eval(script_hset, keys.begin(), keys.end(), args.begin(), args.end());
            }
            catch (const sw::redis::Error &e) {
                std::cout << e.what() << std::endl;
            }


            nWrites = 0;

            keys.clear();
            args.clear();

            keys.push_back("lastUpdateId");
            args.push_back("");                     // Padding
        }
    }

    pipe.exec();
*/


void escrituras(int id, sw::redis::ConnectionOptions options)
{
    sw::redis::RedisCluster redisc(options);

    string s_id = makeFixedLength(id, s_len);
    int local_nBlocks = nBlocks/nThreads;
    int ipagina = 0;

    auto pipe = redisc.pipeline(s_id +makeFixedLength(ipagina, s_len));
    
    int nWrites = 0;

    for (int i = 0; i < local_nBlocks; i++)
    {
        unordered_map<string, string> keys;
        for (int j = 0; j < nVars; j++)
        {
            keys[to_string(i)+":"+to_string(j)] = "0";
        }

        pipe.hset("{"+s_id+"}:"+to_string(i), keys.begin(), keys.end());
    }

    pipe.exec();
}

void escrituras_dir(int id, sw::redis::ConnectionOptions options)
{
    sw::redis::Redis redis(options);

    string s_id = makeFixedLength(id, s_len);
    int local_nBlocks = nBlocks/nThreads;
    auto pipe = redis.pipeline();
    int nWrites = 0;


    for (int i = 0; i < local_nBlocks; i++)
    {
        unordered_map<string, string> keys;
        for (int j = 0; j < nVars; j++)
        {
            keys[to_string(i)+":"+to_string(j)] = "0";
        }

        pipe.hset("{"+s_id+"}:"+to_string(i), keys.begin(), keys.end());
    }
    
    pipe.exec();
}

int main()
{   
    sw::redis::ConnectionOptions options, options_local;
    sw::redis::ConnectionPoolOptions pool_options;

    options_local.host = "127.0.0.1";
    options_local.port = 6379;

    sw::redis::Redis redis(options_local);

    std::cout << "Connection created\n";

    std::cout << redis.ping() << std::endl;

    cout << "Fin\n";
}