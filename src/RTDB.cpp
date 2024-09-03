#include "Remote_db_handler.cpp"
#include "RTDB.h"
#include "RTDBUtils.h"

const std::string DEFAULT_ASSET = "DEFAULT";

RTDB_HANDLE RTDB_Open(const std::string &sConnectionString, BOOL bCreateUpdateThread, void *pLogger)
{
    auto rtdb = new Redis_handler(sConnectionString, bCreateUpdateThread, pLogger);
    rtdb->RTDB_AddAsset(DEFAULT_ASSET);                                             // Adds the default asset
    return rtdb;
}



BOOL RTDB_Close(RTDB_HANDLE hRTDB)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    delete rtdb;
    return TRUE;
}



BOOL RTDB_AddAsset(RTDB_HANDLE hRTDB, const std::string &sConnectionString)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddAsset(DEFAULT_ASSET);
}

BOOL RTDB_AddAsset(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sConnectionString)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddAsset(sAsset);
}



BOOL RTDB_RemoveAsset(RTDB_HANDLE hRTDB)
{
	auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_RemoveAsset(DEFAULT_ASSET);
}

BOOL RTDB_RemoveAsset(RTDB_HANDLE hRTDB, const std::string &sAsset)
{
	auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_RemoveAsset(sAsset);
}



uint32_t RTDB_GetUsedEntries(RTDB_HANDLE hRTDB)
{
	auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetUsedEntries(DEFAULT_ASSET);
}

uint32_t RTDB_GetUsedEntries(RTDB_HANDLE hRTDB, const std::string &sAsset)
{
	auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetUsedEntries(sAsset);
}



TEQ_LIST_PTR RTDB_GetDeviceTypes(RTDB_HANDLE hRTDB)
{
	auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDeviceTypes(DEFAULT_ASSET);
}

TEQ_LIST_PTR RTDB_GetDeviceTypes(RTDB_HANDLE hRTDB, const std::string &sAsset)
{
	auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDeviceTypes(sAsset);
}



VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetVariables(DEFAULT_ASSET, sTypeOfDevice);
}

VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetVariables(sAsset, sTypeOfDevice);
}

VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const ST_DEVICE_TYPE &stDeviceType)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetVariables(DEFAULT_ASSET, stDeviceType.sDeviceTypeName);
}

VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const std::string &sAsset, const ST_DEVICE_TYPE &stDeviceType)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetVariables(sAsset, stDeviceType.sDeviceTypeName);
}



BOOL RTDB_GetDevice(RTDB_HANDLE hRTDB, const std::string &sDevice, ST_DEVICE &stDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDevice(DEFAULT_ASSET, sDevice, stDevice);   
}

BOOL RTDB_GetDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sDevice, ST_DEVICE &stDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDevice(sAsset, sDevice, stDevice);   
}



EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const std::string &sDeviceType)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDevices(DEFAULT_ASSET, sDeviceType);
}

EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sDeviceType)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDevices(sAsset, sDeviceType);
}

EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const ST_DEVICE_TYPE &stTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDevices(DEFAULT_ASSET, stTypeOfDevice.sDeviceTypeName); 
}

EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const std::string &sAsset, const ST_DEVICE_TYPE &stTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetDevices(sAsset, stTypeOfDevice.sDeviceTypeName); 
}



BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const std::string &sDevice, const std::string &sVariable, ST_TAG_VALUE *pOutput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetValue(DEFAULT_ASSET, sDevice, sVariable, *pOutput);    
}

BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sDevice, const std::string &sVariable, ST_TAG_VALUE *pOutput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetValue(sAsset, sDevice, sVariable, *pOutput);    
}

BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const ST_DEVICE &pDevice, const ST_DEVICE_TYPE_VARIABLE &pVariable, ST_TAG_VALUE *pOutput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetValue(DEFAULT_ASSET, pDevice.sDeviceName, pVariable.sVariableName, *pOutput);    
}

BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const std::string &sAsset, const ST_DEVICE &pDevice, const ST_DEVICE_TYPE_VARIABLE &pVariable, ST_TAG_VALUE *pOutput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetValue(sAsset, pDevice.sDeviceName, pVariable.sVariableName, *pOutput);    
}



BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const std::string &sDevice, const std::string &sVariable, const ST_TAG_VALUE *pInput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_SetValue(DEFAULT_ASSET, sDevice, sVariable, *pInput);  
}

BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sDevice, const std::string &sVariable, const ST_TAG_VALUE *pInput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_SetValue(sAsset, sDevice, sVariable, *pInput);  
}

BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const ST_DEVICE &pDevice, const ST_DEVICE_TYPE_VARIABLE &pVariable, const ST_TAG_VALUE *pInput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_SetValue(DEFAULT_ASSET, pDevice.sDeviceName, pVariable.sVariableName, *pInput);     
}

BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const std::string &sAsset, const ST_DEVICE &pDevice, const ST_DEVICE_TYPE_VARIABLE &pVariable, const ST_TAG_VALUE *pInput)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_SetValue(sAsset, pDevice.sDeviceName, pVariable.sVariableName, *pInput);     
}



BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const std::string &sDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddDevice(DEFAULT_ASSET, sTypeOfDevice, sDevice);     
}

BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const std::string &sDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddDevice(sAsset, sTypeOfDevice, sDevice);     
}



BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const std::string &sDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_DelDevice(DEFAULT_ASSET, sDevice);  
}

BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_DelDevice(sAsset, sDevice);  
}



BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const std::string &sVariable)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_DelVarToTypeOfDevice(DEFAULT_ASSET, sTypeOfDevice, sVariable);  
}

BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const std::string &sVariable)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_DelVarToTypeOfDevice(sAsset, sTypeOfDevice, sVariable);  
}



BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddVarToTypeOfDevice(DEFAULT_ASSET, sTypeOfDevice, newVar);  
}

BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddVarToTypeOfDevice(sAsset, sTypeOfDevice, newVar);  
}



BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_ModifyVarToTypeOfDevice(DEFAULT_ASSET, sTypeOfDevice, newVar);     
}

BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_ModifyVarToTypeOfDevice(sAsset, sTypeOfDevice, newVar);     
}


BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddTypeOfDevice(DEFAULT_ASSET, sTypeOfDevice);       
}

BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_AddTypeOfDevice(sAsset, sTypeOfDevice);       
}



BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_DelTypeOfDevice(DEFAULT_ASSET, sTypeOfDevice);    
}

BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_DelTypeOfDevice(sAsset, sTypeOfDevice);    
}



ASSET_LIST RTDB_GetAssets(RTDB_HANDLE hRTDB)
{
    auto rtdb = reinterpret_cast<Redis_handler*>(hRTDB);
    return rtdb->RTDB_GetAssets();    
}












/********************************************************* Char pointer versions *************************************************************************/
RTDB_HANDLE RTDB_Open(const char *szConnectionString, BOOL bCreateUpdateThread, void *pLogger)
{
    std::string sConnectionString (szConnectionString);
    return RTDB_Open(sConnectionString, bCreateUpdateThread, pLogger);
}



BOOL RTDB_AddAsset(RTDB_HANDLE hRTDB, const char *szConnectionString)
{
    std::string sConnectionString (szConnectionString);
    return RTDB_AddAsset(hRTDB, sConnectionString);
}

BOOL RTDB_AddAsset(RTDB_HANDLE hRTDB, const char *szAsset, const char *szConnectionString)
{
    std::string sConnectionString (szConnectionString);
    std::string sAsset (szAsset);
    return RTDB_AddAsset(hRTDB, sAsset, sConnectionString);
}

BOOL RTDB_RemoveAsset(RTDB_HANDLE hRTDB, const char *szAsset)
{
    std::string sAsset (szAsset);
    return RTDB_RemoveAsset(hRTDB, sAsset);
}


uint32_t RTDB_GetUsedEntries(RTDB_HANDLE hRTDB, const char* szAsset)
{
    std::string sAsset (szAsset);
    return RTDB_GetUsedEntries(hRTDB, sAsset);
}

TEQ_LIST_PTR RTDB_GetDeviceTypes(RTDB_HANDLE hRTDB, const char* szAsset)
{
    std::string sAsset (szAsset);
    return RTDB_GetDeviceTypes(hRTDB, sAsset);
}



VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const char *szDeviceType)
{
    std::string sDeviceType (szDeviceType);
    return RTDB_GetVariables(hRTDB, sDeviceType);
}

VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const char* szAsset, const char *szDeviceType)
{
    std::string sAsset (szAsset);
    std::string sDeviceType (szDeviceType);
    return RTDB_GetVariables(hRTDB, sAsset, sDeviceType);
}

VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const char* szAsset, const ST_DEVICE_TYPE &stDeviceType)
{
    std::string sAsset (szAsset);
    return RTDB_GetVariables(hRTDB, sAsset, stDeviceType);
}



BOOL RTDB_GetDevice(RTDB_HANDLE hRTDB, const char *szDevice, ST_DEVICE &stDevice)
{
    std::string sDevice (szDevice);
    return RTDB_GetDevice(hRTDB, sDevice, stDevice);   
}

BOOL RTDB_GetDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szDevice, ST_DEVICE &stDevice)
{
    std::string sAsset (szAsset);
    std::string sDevice (szDevice);
    return RTDB_GetDevice(hRTDB, sAsset, sDevice, stDevice);   
}



EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const char *szDeviceType)
{
    std::string sDeviceType (szDeviceType);
    return RTDB_GetDevices(hRTDB, sDeviceType);
}

EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const char* szAsset, const char *szDeviceType)
{
    std::string sAsset (szAsset);
    std::string sDeviceType (szDeviceType);
    return RTDB_GetDevices(hRTDB, sAsset, sDeviceType);
}

EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const char* szAsset, const ST_DEVICE_TYPE &stDeviceType)
{
    std::string sAsset (szAsset);
    return RTDB_GetDevices(hRTDB, sAsset, stDeviceType); 
}



BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const char *szDevice, const char *szVariable, ST_TAG_VALUE *pOutput)
{
    std::string sDevice (szDevice);
    std::string sVariable (szVariable);
    return RTDB_GetValue(hRTDB, sDevice, sVariable, pOutput);    
}

BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const char* szAsset, const char *szDevice, const char *szVariable, ST_TAG_VALUE *pOutput)
{
    std::string sAsset (szAsset);
    std::string sDevice (szDevice);
    std::string sVariable (szVariable);
    return RTDB_GetValue(hRTDB, sAsset, sDevice, sVariable, pOutput);    
}




BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const char *szDevice, const char *szVariable, const ST_TAG_VALUE *pInput)
{
    std::string sDevice (szDevice);
    std::string sVariable (szVariable);
    return RTDB_SetValue(hRTDB, sDevice, sVariable, pInput);  
}

BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const char* szAsset, const char *szDevice, const char *szVariable, const ST_TAG_VALUE *pInput)
{
    std::string sAsset (szAsset);
    std::string sDevice (szDevice);
    std::string sVariable (szVariable);
    return RTDB_SetValue(hRTDB, sAsset, sDevice, sVariable, pInput);  
}



BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const char *szDevice)
{
    std::string sTypeOfDevice (szTypeOfDevice);
    std::string sDevice (szDevice);
    return RTDB_AddDevice(hRTDB, sTypeOfDevice, sDevice);     
}

BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const char *szDevice)
{
    std::string sAsset (szAsset);
    std::string sTypeOfDevice (szTypeOfDevice);
    std::string sDevice (szDevice);
    return RTDB_AddDevice(hRTDB, sAsset, sTypeOfDevice, sDevice);     
}



BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const char *szDevice)
{
    std::string sDevice (szDevice);
    return RTDB_DelDevice(hRTDB, sDevice);  
}

BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szDevice)
{
    std::string sAsset (szAsset);
    std::string sDevice (szDevice);
    return RTDB_DelDevice(hRTDB, sAsset, sDevice);  
}



BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const char* szVariable)
{
    std::string sTypeOfDevice (szTypeOfDevice);
    std::string sVariable (szVariable);
    return RTDB_DelVarToTypeOfDevice(hRTDB, sTypeOfDevice, sVariable);  
}


BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const char* szVariable)
{
    std::string sAsset (szAsset);
    std::string sTypeOfDevice (szTypeOfDevice);
    std::string sVariable (szVariable);
    return RTDB_DelVarToTypeOfDevice(hRTDB, sAsset, sTypeOfDevice, sVariable);  
}



BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_AddVarToTypeOfDevice(hRTDB, sTypeOfDevice, newVar);  
}

BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    std::string sAsset (szAsset);
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_AddVarToTypeOfDevice(hRTDB, sAsset, sTypeOfDevice, newVar);  
}



BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_ModifyVarToTypeOfDevice(hRTDB, sTypeOfDevice, newVar);     
}

BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
{
    std::string sAsset (szAsset);
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_ModifyVarToTypeOfDevice(hRTDB, sAsset, sTypeOfDevice, newVar);     
}



BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice)
{
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_AddTypeOfDevice(hRTDB, sTypeOfDevice);       
}

BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice)
{
    std::string sAsset (szAsset);
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_AddTypeOfDevice(hRTDB, sAsset, sTypeOfDevice);       
}



BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice)
{
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_DelTypeOfDevice(hRTDB, sTypeOfDevice);    
}

BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice)
{
    std::string sAsset (szAsset);
    std::string sTypeOfDevice (szTypeOfDevice);
    return RTDB_DelTypeOfDevice(hRTDB, sAsset, sTypeOfDevice);    
}

void RTDB_EpochToSYSTEMTIME(DATETYPE msEpoch, void* pSystemTime)
{
	EpochToSYSTEMTIME(msEpoch, reinterpret_cast<SYSTEMTIME*>(pSystemTime));
}