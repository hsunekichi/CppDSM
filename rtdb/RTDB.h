/**=================================================================================================
*												SICE
**==================================================================================================
* PROJECT: LibRTDB
* FILE NAME: RTDB.h
* AUTHOR: Yujin Kang
* CREATION DATE: 2019/01/31
* DESCRIPTION: LibRTDB DLL header file
* ================================================================================================*/
#pragma once
#include "RTDBInterfaceTypes.h"
#include <list>

#define DLLEXPORT __declspec(dllexport)

using TEQ_LIST = std::list<ST_DEVICE_TYPE>;
using TEQ_ITER = TEQ_LIST::iterator;
using TEQ_LIST_PTR = std::shared_ptr<TEQ_LIST>;

using VAR_LIST = std::list<ST_DEVICE_TYPE_VARIABLE>;
using VAR_ITER = VAR_LIST::iterator;
using VAR_LIST_PTR = std::shared_ptr<VAR_LIST>;

using EQ_LIST = std::list<ST_DEVICE>;
using EQ_ITER = EQ_LIST::iterator;
using EQ_LIST_PTR = std::shared_ptr<EQ_LIST>;

using ASSET_LIST = std::list<std::string>;

using RTDB_HANDLE = void*;
#ifdef LOGWRITER_H
DLLEXPORT RTDB_HANDLE RTDB_Open(const char *szConnectionString, BOOL bCreateUpdateThread = TRUE, void *pLogger = CLogWriter::getInstance());
DLLEXPORT RTDB_HANDLE RTDB_Open(const std::string &sConnectionString, BOOL bCreateUpdateThread = TRUE, void *pLogger = CLogWriter::getInstance());
#else
DLLEXPORT RTDB_HANDLE RTDB_Open(const char *szConnectionString, BOOL bCreateUpdateThread = TRUE, void *pLogger = nullptr);
DLLEXPORT RTDB_HANDLE RTDB_Open(const std::string &sConnectionString, BOOL bCreateUpdateThread = TRUE, void *pLogger = nullptr);
#endif
DLLEXPORT BOOL RTDB_Close(RTDB_HANDLE hRTDB);

DLLEXPORT BOOL RTDB_AddAsset(RTDB_HANDLE hRTDB, const char *szAsset, const char *szConnectionString);
DLLEXPORT BOOL RTDB_AddAsset(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sConnectionString);
DLLEXPORT BOOL RTDB_RemoveAsset(RTDB_HANDLE hRTDB, const char *szAsset);
DLLEXPORT BOOL RTDB_RemoveAsset(RTDB_HANDLE hRTDB, const std::string &sAsset);

DLLEXPORT uint32_t RTDB_GetUsedEntries(RTDB_HANDLE hRTDB, const char* szAsset);
DLLEXPORT uint32_t RTDB_GetUsedEntries(RTDB_HANDLE hRTDB, const std::string &sAsset);

DLLEXPORT TEQ_LIST_PTR RTDB_GetDeviceTypes(RTDB_HANDLE hRTDB, const char* szAsset);
DLLEXPORT TEQ_LIST_PTR RTDB_GetDeviceTypes(RTDB_HANDLE hRTDB, const std::string &sAsset);

DLLEXPORT VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice);
DLLEXPORT VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice);
DLLEXPORT VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const char* szAsset, const ST_DEVICE_TYPE &stTypeOfDevice);
DLLEXPORT VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const std::string &sAsset, const ST_DEVICE_TYPE &stTypeOfDevice);


DLLEXPORT EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice);
DLLEXPORT EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &szTypeOfDevice);
DLLEXPORT EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const char* szAsset, const ST_DEVICE_TYPE &stTypeOfDevice);
DLLEXPORT EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const std::string &sAsset, const ST_DEVICE_TYPE &stTypeOfDevice);

DLLEXPORT BOOL RTDB_GetDevice(RTDB_HANDLE hRTDB, const char *szDevice, ST_DEVICE &stDevice);
DLLEXPORT BOOL RTDB_GetDevice(RTDB_HANDLE hRTDB, const std::string &sDevice, ST_DEVICE &stDevice);

DLLEXPORT BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const char *szDevice, const char *szVariable, ST_TAG_VALUE *pOutput);
DLLEXPORT BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const std::string &sDevice, const std::string &sVariable, ST_TAG_VALUE *pOutput);
DLLEXPORT BOOL RTDB_GetValue(RTDB_HANDLE hRTDB, const ST_DEVICE &pDevice, const ST_DEVICE_TYPE_VARIABLE &pVariable, ST_TAG_VALUE *pOutput);

DLLEXPORT BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const char *szDevice, const char *szVariable, const ST_TAG_VALUE *pInput);
DLLEXPORT BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const std::string &sDevice, const std::string &sVariable, const ST_TAG_VALUE *pInput);
DLLEXPORT BOOL RTDB_SetValue(RTDB_HANDLE hRTDB, const ST_DEVICE &pDevice, const ST_DEVICE_TYPE_VARIABLE &pVariable, const ST_TAG_VALUE *pInput);

DLLEXPORT BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice);
DLLEXPORT BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice);
DLLEXPORT BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice);
DLLEXPORT BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice);

DLLEXPORT BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const char* szVariable);
DLLEXPORT BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const std::string &sVariable);

DLLEXPORT BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szTypeOfDevice, const char *szDevice);
DLLEXPORT BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sTypeOfDevice, const std::string &sDevice);
DLLEXPORT BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const char* szAsset, const char *szDevice);
DLLEXPORT BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const std::string &sAsset, const std::string &sDevice);

DLLEXPORT ASSET_LIST RTDB_GetAssets(RTDB_HANDLE hRTDB);


//Functions with no Asset parameter. Default asset is assumed.

DLLEXPORT uint32_t RTDB_GetUsedEntries(RTDB_HANDLE hRTDB);

DLLEXPORT TEQ_LIST_PTR RTDB_GetDeviceTypes(RTDB_HANDLE hRTDB);

DLLEXPORT VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const char *szTypeOfDevice);
DLLEXPORT VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice);
DLLEXPORT VAR_LIST_PTR RTDB_GetVariables(RTDB_HANDLE hRTDB, const ST_DEVICE_TYPE &stTypeOfDevice);

DLLEXPORT EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const char *szTypeOfDevice);
DLLEXPORT EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice);
DLLEXPORT EQ_LIST_PTR RTDB_GetDevices(RTDB_HANDLE hRTDB, const ST_DEVICE_TYPE &stTypeOfDevice);

DLLEXPORT BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice);
DLLEXPORT BOOL RTDB_AddTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice);
DLLEXPORT BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice);
DLLEXPORT BOOL RTDB_DelTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice);

DLLEXPORT BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_ModifyVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_AddVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar);
DLLEXPORT BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const char* szVariable);
DLLEXPORT BOOL RTDB_DelVarToTypeOfDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const std::string &sVariable);

DLLEXPORT BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const char *szTypeOfDevice, const char *szDevice);
DLLEXPORT BOOL RTDB_AddDevice(RTDB_HANDLE hRTDB, const std::string &sTypeOfDevice, const std::string &sDevice);
DLLEXPORT BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const char *szDevice);
DLLEXPORT BOOL RTDB_DelDevice(RTDB_HANDLE hRTDB, const std::string &sDevice);

//Utils
DLLEXPORT void RTDB_EpochToSYSTEMTIME(DATETYPE msEpoch, void* pSystemTime);
