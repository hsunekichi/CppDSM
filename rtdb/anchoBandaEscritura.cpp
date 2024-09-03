#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <functional>
#include <chrono>
#include <stdio.h>
#include "Redis_handler.cpp"
#include <Windows.h>
#include <stdint.h>
#include <memory>
#include <thread>
#include <mutex>


ST_DEVICE crearDispositivo(int n)
{
    ST_DEVICE disp;

    disp.sDeviceName = "SEM" + std::to_string(n);
    disp.sDeviceType = "SEM";
    disp.sAsset = "BALEARES";

    return disp;
}

ST_TAG_VALUE crearTag(int n)
{
    ST_TAG_VALUE tag;
    tag.sValue = "red" + std::to_string(n);
    tag.nDataTimestamp = 1000;
    tag.nWriteTimestamp = 1000;
    tag.nQuality = 100;

    return tag;
}

using namespace std;

int main()
{
    struct timeval begin, end;
	long long seconds, microseconds;

    string sAsset = "BALEARES";
    int nDisp = 100000;
    int nVar = 10;
    sw::redis::ConnectionOptions options;
    options.host = "10.152.11.221";
    options.port = 65225;

    sw::redis::Redis redis (options);
    redis.flushdb();

    auto rtdb = new Redis_handler(false);

    for (int i = 0; i < nVar; i++)  //********************************************************************************* ADDS ALL VARIABLES TO THE TYPE
    {
        ST_DEVICE_TYPE_VARIABLE VAR;
        VAR.sVariableName = "color" + to_string(i);
        VAR.sFormat = "red+" + to_string(i);
        VAR.nChangeEvent = 1;
        VAR.nAlarmEvent = 1;
        VAR.nHistEvent = 1;
        VAR.cType = 69;

        rtdb->RTDB_AddVarToTypeOfDevice(sAsset, "SEM", VAR);
    }

       vector<ST_DEVICE> vDisp;

    rtdb->RTDB_RemoveAsset(sAsset);


    rtdb->RTDB_AddAsset(sAsset);
    rtdb->RTDB_AddTypeOfDevice(sAsset, "SEM");
   
    for (int i = 0; i < nVar; i++)  //********************************************************************************* ADDS ALL VARIABLES TO THE TYPE
    {
        ST_DEVICE_TYPE_VARIABLE VAR;
        VAR.sVariableName = "color" + to_string(i);
        VAR.sFormat = "red+" + to_string(i);
        VAR.nChangeEvent = 1;
        VAR.nAlarmEvent = 1;
        VAR.nHistEvent = 1;
        VAR.cType = 69;

        rtdb->RTDB_AddVarToTypeOfDevice(sAsset, "SEM", VAR);
    }
    
    for (int i = 0; i < nDisp; i++)
    {
        vDisp.push_back(crearDispositivo(i));
    }
    
    cout << "dispositivos creados\n";

    gettimeofday(&begin, 0);
    for (auto disp : vDisp) //***************************************************************************************** ADDS ALL DEVICES
    {
        rtdb->RTDB_AddDevice(disp.sAsset, disp.sDeviceType, disp.sDeviceName);

        for (int i = 0; i < nVar; i++)  //***************************************************************************** ADDS ALL VARIABLES TO EACH DEVICE
        {
            rtdb->RTDB_SetValue(sAsset, disp.sDeviceName, "color" + to_string(i), crearTag(i));
        }
    }

    cout << "Dispositivos encolados\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(30000));  // Makes all async flushing threads to work on flushing, instead of directly killing them on the delete
    delete rtdb; 

    cout << "Dispositivos escritos\n";
    return 0;
}