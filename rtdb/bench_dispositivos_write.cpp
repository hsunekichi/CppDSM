#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <chrono>
#include "Redis_handler.cpp"
#include <Windows.h>
#include <thread>
#include <mutex>


int local_gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}

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
    int nDisp = 100;
    int nVar = 10000;
    sw::redis::ConnectionOptions options;
    options.host = "169.254.226.41";
    options.port = 7000;

    sw::redis::Redis redis (options);
    redis.ping();

    auto rtdb = new Redis_handler(false);

    vector<ST_DEVICE> vDisp;

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
        VAR.cType = 16;

        rtdb->RTDB_AddVarToTypeOfDevice(sAsset, "SEM", VAR);
    }
    
    for (int i = 0; i < nDisp; i++)
    {
        vDisp.push_back(crearDispositivo(i));
    }
    
    cout << "dispositivos generados\n";

    for (auto disp : vDisp) //***************************************************************************************** ADDS ALL DEVICES
    {
        rtdb->RTDB_AddDevice(disp.sAsset, disp.sDeviceType, disp.sDeviceName);
    }


    local_gettimeofday(&begin, 0);
    for (auto disp : vDisp) //***************************************************************************************** ADDS ALL DEVICES
    {
        for (int i = 0; i < nVar; i++)  //***************************************************************************** ADDS ALL VARIABLES TO EACH DEVICE
        {
            rtdb->RTDB_SetValue(sAsset, disp.sDeviceName, "color" + to_string(i), crearTag(i));
        }
    }
    local_gettimeofday(&end, 0);

    std::cout << "Tiempo aparente en escribir los dispositivos: " << double(end.tv_sec - begin.tv_sec)*1000 + double(end.tv_usec - begin.tv_usec)/1000 << " ms\n"; 
    std::cout << "Escrituras aparentes por segundo: " << nVar*nDisp / (double(end.tv_sec - begin.tv_sec) + double(end.tv_usec - begin.tv_usec)/1000000) << std::endl;
    
    delete rtdb;
    local_gettimeofday(&end, 0);

    std::cout << "Tiempo real en escribir los dispositivos: " << double(end.tv_sec - begin.tv_sec)*1000 + double(end.tv_usec - begin.tv_usec)/1000 << " ms\n"; 
    std::cout << "Escrituras real por segundo: " << nVar*nDisp / (double(end.tv_sec - begin.tv_sec) + double(end.tv_usec - begin.tv_usec)/1000000) << std::endl;

    return 0;
}