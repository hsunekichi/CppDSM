/**=================================================================================================
*												SICE
**==================================================================================================
* PROJECT: LibRTDB
* FILE NAME: CommonTypes.h
* AUTHOR: Yujin Kang
* CREATION DATE: 2019/03/11
* DESCRIPTION: Header containing common type definitions
* ================================================================================================*/

#pragma once
#include <cstdint>
#include <string>

#define RTDB_PREFIX "ITS_RTDB"
#define RTDB_NO_ASSET_NAME "NO_ASSET"

//Type definitions
#ifndef BOOL
using BOOL = int;
#endif
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

using DATETYPE = int64_t;
using INDEX_TYPE = uint32_t;
using SIGNED_INDEX_TYPE = int32_t;

#define INVALID_INDEX	UINT32_MAX
#define WRONG_DATE 0x8000000000000000