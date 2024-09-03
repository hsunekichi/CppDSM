/**=================================================================================================
*												SICE
**==================================================================================================
* PROJECT: LibRTDB
* FILE NAME: RTDBUtils.cpp
* AUTHOR: Yujin Kang
* CREATION DATE: 2019/03/11
* DESCRIPTION: Implementation of utility functions used within the RTDB
* ================================================================================================*/
#include <RTDBUtils.h>
#include <string.h>
#include <algorithm>
#include <chrono>
#include <ctime>

/**************************************************************************************************/
//	NAME:			NonNullTerminatedSTRCMP
//	DESCRIPTION:	Compares 2 non null terminated strings
//	IN PARAMETERS:	s1, s2: strings to compare
//					l1, l2: size of respective strings
//	OUT PARAMETERS: Negative when s1 < s2, 0 when equal, positive when s2 > s1
//	RETURNS:		Boolean, TRUE if it opens or creates correctly, FALSE otherwise.
//	HISTORY:
//		DATE		AUTHOR	DESCRIPTION
//		2019/01/31	YKP		Creation
/**************************************************************************************************/
int32_t NonNullTerminatedSTRCMP(const char* sz1, size_t l1, const char* sz2, size_t l2)
{
	int32_t nDiff = strncmp(sz1, sz2, (std::min)(l1, l2));

	if (nDiff != 0) return nDiff;
	if (l1 == l2) return 0;
	return (l1 < l2) ? -1 : 1;
}

//https://support.microsoft.com/es-es/help/167296/how-to-convert-a-unix-time-t-to-a-win32-filetime-or-systemtime

 // This function converts the 32bit Unix time structure to the FILETIME
// structure.
// The time_t is a 32-bit value for the number of seconds since January 1,
// 1970. A FILETIME is a 64-bit for the number of 100-nanosecond periods
// since January 1, 1601. Convert by multiplying the time_t value by 1e+7
// to get to the same base granularity, then add the numeric equivalent
// of January 1, 1970 as FILETIME.
void UnixTimeToFileTime(time_t t, LPFILETIME pft)
{
	// Note that LONGLONG is a 64-bit value
	LONGLONG ll;

	ll = Int32x32To64(t, 10000000) + 116444736000000000;
	pft->dwLowDateTime = (DWORD)ll;
	pft->dwHighDateTime = ll >> 32;
}

// This function converts the FILETIME structure to the 32 bit
// Unix time structure.
// The time_t is a 32-bit value for the number of seconds since
// January 1, 1970. A FILETIME is a 64-bit for the number of
// 100-nanosecond periods since January 1, 1601. Convert by
// subtracting the number of 100-nanosecond period betwee 01-01-1970
// and 01-01-1601, from time_t the divide by 1e+7 to get to the same
// base granularity.
void FileTimeToUnixTime(LPFILETIME pft, time_t* pt)
{
	LONGLONG ll; // 64 bit value
	ll = (((LONGLONG)(pft->dwHighDateTime)) << 32) + pft->dwLowDateTime;
	*pt = (time_t)((ll - 116444736000000000) / 10000000);
}

// This function converts the 32 bit Unix time structure to
// the SYSTEMTIME structure
void UnixTimeToSystemTime(time_t t, LPSYSTEMTIME pst)
{
	FILETIME ft;

	UnixTimeToFileTime(t, &ft);
	FileTimeToSystemTime(&ft, pst);

}

// This function coverts the SYSTEMTIME structure to
// the 32 bit Unix time structure
void SystemTimeToUnixTime(LPSYSTEMTIME pst, time_t* pt)
{
	FILETIME ft;
	SystemTimeToFileTime(pst, &ft);
	FileTimeToUnixTime(&ft, pt);
}

void EpochToSYSTEMTIME(DATETYPE msEpoch, SYSTEMTIME* pSystemTime)
{
	std::chrono::milliseconds durationMs(msEpoch);
	std::chrono::time_point<std::chrono::system_clock> timepoint(durationMs);
	auto in_time_t = std::chrono::system_clock::to_time_t(timepoint);

	UnixTimeToSystemTime(in_time_t, pSystemTime);
	pSystemTime->wMilliseconds = msEpoch % 1000;
}

void SYSTEMTIMEToEpoch(LPSYSTEMTIME pSystemTime, DATETYPE* msEpoch)
{
	time_t tm;
	SystemTimeToUnixTime(pSystemTime, &tm);
	auto out_time_t = std::chrono::system_clock::from_time_t(tm);
	*msEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(out_time_t.time_since_epoch()).count() + pSystemTime->wMilliseconds;
}

void TIMEToEpoch(__timeb64* pTime, DATETYPE* msEpoch)
{
	SYSTEMTIME time;
	tm* strTime;

	strTime = localtime(&pTime->time);

	time.wYear = strTime->tm_year + 1900;
	time.wMonth = strTime->tm_mon + 1;
	time.wDay = strTime->tm_mday;
	time.wHour = strTime->tm_hour;
	time.wMinute = strTime->tm_min;
	time.wSecond = strTime->tm_sec;
	time.wMilliseconds = pTime->millitm;

	SYSTEMTIMEToEpoch(&time, msEpoch);
}

/**************************************************************************************************/
//	NAME:			GetStringDateTimeFromEpoch
//	DESCRIPTION:	Given a datetime in milliseconds, print in to text format
//					dd/MM/yyyy HH:mm:ss SSSS
//	IN PARAMETERS:	msEpoch			Milliseconds since epoch
//					bMilliseconds	Indicates if use milliseconds
//	OUT PARAMETERS: sOut		Output string
//	RETURNS:		None
//	HISTORY:
//		DATE		AUTHOR	DESCRIPTION
//		2019/01/31	YKP		Creation
/**************************************************************************************************/
void GetStringDateTimeFromEpoch(DATETYPE msEpoch, char *szOut, bool bMilliseconds)
{
	std::chrono::milliseconds durationMs(msEpoch);
	std::chrono::time_point<std::chrono::system_clock> timepoint(durationMs);

	auto in_time_t = std::chrono::system_clock::to_time_t(timepoint);
	//Convert to tm as UTC time_t
	auto in_tm = std::gmtime(&in_time_t);

	if (bMilliseconds)
	{
		if (msEpoch == WRONG_DATE)
			sprintf(szOut, "01/01/1970 00:00:00 0000"); // #117956
		else
			sprintf(szOut, "%02d/%02d/%04d %02d:%02d:%02d %04lld",
				in_tm->tm_mday, in_tm->tm_mon + 1, in_tm->tm_year + 1900,
				in_tm->tm_hour, in_tm->tm_min, in_tm->tm_sec,
				msEpoch % 1000);
	}
	else
	{
		if (msEpoch == WRONG_DATE)
			sprintf(szOut, "01/01/1970 00:00:00"); // #117956
		else
			sprintf(szOut, "%02d/%02d/%04d %02d:%02d:%02d",
				in_tm->tm_mday, in_tm->tm_mon + 1, in_tm->tm_year + 1900,
				in_tm->tm_hour, in_tm->tm_min, in_tm->tm_sec);
	}
}

/**************************************************************************************************/
//	NAME:			GetEpochFromStringDateTime
//	DESCRIPTION:	Given date in format dd/MM/yyyy HH:mm:ss SSSS
//					get ms since epoch
//	IN PARAMETERS:	szDate		Formatted string
//					nLen		szDate length
//	RETURNS:		DATETYPE	Ms since epoch
//	HISTORY:
//		DATE		AUTHOR	DESCRIPTION
//		2019/01/31	YKP		Creation
/**************************************************************************************************/
DATETYPE GetEpochFromStringDateTime(const char *szDate, size_t nLen)
{
	//Reverse process from GetFormattedDateTime
	DATETYPE msDate = 0;
	std::tm in_tm;
	std::time_t in_time_t;

	if (strstr(szDate, "00/00/0000 00:00:00") != NULL || strstr(szDate, "01/01/1970 00:00:00") != NULL)
	{
		return WRONG_DATE; //Wrong format
	}

	if (sscanf(szDate, "%02d/%02d/%04d %02d:%02d:%02d %04lld", &in_tm.tm_mday, &in_tm.tm_mon,
		&in_tm.tm_year, &in_tm.tm_hour, &in_tm.tm_min, &in_tm.tm_sec, &msDate) < 6)
	{
		return WRONG_DATE; //Wrong format
	}

	in_tm.tm_year -= 1900;
	in_tm.tm_mon -= 1;

	//Convert to time_t (epoch secs) without time zone (UTC)
	//Non portable function ... (timegm in unix)
	in_time_t = _mkgmtime(&in_tm);

	std::chrono::time_point<std::chrono::system_clock> timepoint = std::chrono::system_clock::from_time_t(in_time_t);

	return msDate + std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();
}

/**************************************************************************************************/
//	NAME:			GetEpochFromSQLDateTime2
//	DESCRIPTION:	Given date in format YYYY/MM/dd HH:mm:ss
//					get ms since epoch
//	IN PARAMETERS:	szDate		Formatted string as provided by datetime2 type in SQL
//					nLen		szDate length
//	RETURNS:		DATETYPE	Ms since epoch
//	HISTORY:
//		DATE		AUTHOR	DESCRIPTION
//		2019/01/31	YKP		Creation
/**************************************************************************************************/
DATETYPE GetEpochFromSQLDateTime2(const char *szDate, size_t nLen)
{
	//Reverse process from GetFormattedDateTime
	std::tm in_tm;
	std::time_t in_time_t;
	std::chrono::time_point<std::chrono::system_clock> timepoint;

	if (nLen < 19 || sscanf(szDate, "%04d/%02d/%02d %02d:%02d:%02d", &in_tm.tm_year, &in_tm.tm_mon, 
		&in_tm.tm_mday,	&in_tm.tm_hour, &in_tm.tm_min, &in_tm.tm_sec) != 6)
	{
		return WRONG_DATE;
	}

	in_tm.tm_year -= 1900;
	in_tm.tm_mon -= 1;

	//Convert to time_t (epoch secs) without time zone (UTC)
	//Non portable function ... (timegm in unix)
	in_time_t = _mkgmtime(&in_tm);

	timepoint = std::chrono::system_clock::from_time_t(in_time_t);

	return std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();
}