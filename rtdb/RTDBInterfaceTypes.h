/**=================================================================================================
*												SICE
**==================================================================================================
* PROJECT: LibRTDB
* FILE NAME: RTDBInterfaceTypes.h
* AUTHOR: Yujin Kang
* CREATION DATE: 2019/01/14
* DESCRIPTION:	Declaration structures used in the DLL API of the RTDB
* ================================================================================================*/

#pragma once
#include <string>
#include "RTDBCommonTypes.h"

#pragma pack(push, 8)
//#pragma pack(show)

//If compilation options between LibRTDB and the project using LibRTDB differ, the size of std::string
//might differ, in which case the program will malfunction. Specifically, this behaviour was seen
//when the selected runtime library differed. "C/C++ -> Code generation -> Execution Runtime library"
//E.g. Multiprocess debugging DLL gives a size of 28 bytes, while Multiprocess DLL gives 24 bytes

typedef struct ST_DEVICE_TYPE
{
	std::string		sDeviceTypeName;
	uint32_t		nNumberDevices = 0;
	uint16_t		nNumberVariables = 0;

	uint32_t		_deviceType = 0;
} ST_DEVICE_TYPE;

typedef struct ST_DEVICE_TYPE_VARIABLE
{
	std::string		sVariableName;
	std::string		sFormat;
	uint8_t			nChangeEvent = 0;
	uint8_t			nAlarmEvent = 0;
	uint8_t			nHistEvent = 0;
	char			cType = 0;
} ST_DEVICE_TYPE_VARIABLE;

typedef struct ST_DEVICE
{
	std::string		sDeviceName;
	std::string		sDeviceType;
	std::string		sAsset;
} ST_DEVICE;

typedef struct ST_TAG_VALUE
{
	std::string	sValue;
	int64_t		nDataTimestamp = WRONG_DATE;
	int64_t		nWriteTimestamp = WRONG_DATE;
	int8_t		nQuality = 0;
} ST_TAG_VALUE;

#pragma pack(pop)