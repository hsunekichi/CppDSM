#pragma once
#include <RTDBCommonTypes.h>
#include <LogWriter.h>
#include <sys/types.h>
#include <sys/timeb.h>

extern CLogWriter *RTDB_pLogger;

//#define RTDB_LOG_PRINTF
#ifndef RTDB_LOG_PRINTF
	#define LOG_PRINTF(level, message, ...) RTDB_pLogger->writeToLog((level), "RTDB --> " message, ## __VA_ARGS__);
	#define LOG_INFO(x, ...) LOG_PRINTF(CLogWriter::LOG_INFO, x, __VA_ARGS__);
	#define LOG_DEBUG(x, ...) LOG_PRINTF(CLogWriter::LOG_DEBUG, x, __VA_ARGS__);
	#define LOG_ERROR(x, ...) LOG_PRINTF(CLogWriter::LOG_ERR, "Error in %s:%d: " x "\n",__FILE__, __LINE__, __VA_ARGS__);
#else
	#define LOG_ERROR(x, ...) printf("Error in %s:%d: " x "\n",__FILE__, __LINE__, __VA_ARGS__);
	#define LOG_INFO(x, ...) printf(x "\n", __VA_ARGS__);
	#define LOG_DEBUG(x, ...) printf(x "\n", __VA_ARGS__);
#endif

#define TO_MBYTE(X) ((float)X)/(1024*1024)

//Functions
int32_t NonNullTerminatedSTRCMP(const char* sz1, size_t l1, const char* sz2, size_t l2);
void GetStringDateTimeFromEpoch(DATETYPE msEpoch, char *szOut, bool bMilliseconds = true);
DATETYPE GetEpochFromStringDateTime(const char *szDate, size_t nLen);
DATETYPE GetEpochFromSQLDateTime2(const char *szDate, size_t nLen);
void EpochToSYSTEMTIME(DATETYPE msEpoch, SYSTEMTIME* pSystemTime);
void SYSTEMTIMEToEpoch(LPSYSTEMTIME pSystemTime, DATETYPE* msEpoch);
void TIMEToEpoch(__timeb64* pTime, DATETYPE* msEpoch);