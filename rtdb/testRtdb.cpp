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

using namespace std;

/*
int gettimeofday(struct timeval* tp, struct timezone* tzp) {
	namespace sc = std::chrono;
	sc::system_clock::duration d = sc::system_clock::now().time_since_epoch();
	sc::seconds s = sc::duration_cast<sc::seconds>(d);
	tp->tv_sec = s.count();
	tp->tv_usec = sc::duration_cast<sc::microseconds>(d - s).count();

	return 0;
}*/

ST_DEVICE crearDispositivo(int n)
{
    ST_DEVICE disp;

    disp.sDeviceName = "SEM" + to_string(n);
    disp.sDeviceType = "SEM";
    disp.sAsset = "BALEARES";

    return disp;
}

ST_TAG_VALUE crearTag(int n)
{
    ST_TAG_VALUE tag;
    tag.sValue = "red" + to_string(n);
    tag.nDataTimestamp = 1000;
    tag.nWriteTimestamp = 1000;
    tag.nQuality = 100;

    return tag;
}

int main()
{
    struct timeval begin, end;
	long long seconds, microseconds;

    string sAsset = "BALEARES";
    int nDisp = 10000;
    int nVar = 10;
    sw::redis::ConnectionOptions options;
    options.host = "10.152.11.221";
    options.port = 65225;

    sw::redis::Redis redis (options);
    redis.flushdb();

    auto rtdb = new Redis_handler(false);
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

    cout << "dispositivos inicializados\n";
    gettimeofday(&end, 0);
	seconds = end.tv_sec - begin.tv_sec;
	microseconds = end.tv_usec - begin.tv_usec;
	double tiempoEscritura = seconds + microseconds * 1e-6;

	//std::this_thread::sleep_for(std::chrono::milliseconds(3000));			// Sleeps

    gettimeofday(&begin, 0);

    //auto variables = *rtdb->RTDB_GetVariables(sAsset, "SEM");
    //cout << variables.size() << " variables obtenidas\n";

    gettimeofday(&end, 0);
	seconds = end.tv_sec - begin.tv_sec;
	microseconds = end.tv_usec - begin.tv_usec;
	double tiempoLecturaVariables1 = seconds + microseconds * 1e-6;

    cout << "A la espera\n";
    //while(true)
    //{
	//    std::this_thread::sleep_for(std::chrono::milliseconds(1000));			// Sleeps
    //    cout << rtdb->getMessagesRecived() << endl;
    //}






    gettimeofday(&begin, 0);

    //for (auto var : variables)                                      // Gets all variable values of SEM10
    {
        ST_TAG_VALUE temp;
        //rtdb->RTDB_GetValue(sAsset, "SEM10", var.sVariableName, temp);
        //cout << temp.sValue << endl;
    }
    cout << "varibles leidas\n";
    gettimeofday(&end, 0);
	seconds = end.tv_sec - begin.tv_sec;
	microseconds = end.tv_usec - begin.tv_usec;
	double tiempoLecturaVariables2 = seconds + microseconds * 1e-6;

    gettimeofday(&begin, 0);
    ST_DEVICE output; 
    //for(int i = 0; i < nDisp; i++)  
    //    rtdb->RTDB_GetDevice(sAsset, "SEM" + to_string(i), output);                    // Gets all devices
    cout << "dispositivo: " << output.sDeviceName << endl;

    gettimeofday(&end, 0);
	seconds = end.tv_sec - begin.tv_sec;
	microseconds = end.tv_usec - begin.tv_usec;
	double tiempoLecturaDevice = seconds + microseconds * 1e-6;

    rtdb->printBlockTime();
    
    cout << "tiempo escritura: " << tiempoEscritura << endl;
    cout << "tiempo lectura variables 1: " << tiempoLecturaVariables1 << endl;
    cout << "tiempo lectura variables 2: " << tiempoLecturaVariables2 << endl;
    cout << "tiempo lectura device: " << tiempoLecturaDevice << endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));			// Sleeps
    std::cout << "despierto\n";
    gettimeofday(&begin, 0);

    delete rtdb; 
    
    gettimeofday(&end, 0);
	seconds = end.tv_sec - begin.tv_sec;
	microseconds = end.tv_usec - begin.tv_usec;
	cout << "tiempo: " << seconds + microseconds * 1e-6 << endl;

    cout << "adios\n";

}