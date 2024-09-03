#include <string>
#include <list>
#include <chrono>
#include <optional>
#include <thread>
#include <unordered_set>
#include "redis++.h"
#include "Redis_cache.cpp"
#include "RTDBInterfaceTypes.h"
#include "RTDB.h"
//#include <RTDBAssetManager.h>
//#include <RTDBUtils.h>
//#include <RTDBMode.h>

#pragma once


class Redis_handler
{
protected:
	// Configuration
	const int HOST_DB = 0;							// Database to connect inside redis server
	const int MAX_CONNECTION_LIFETIME = 0;			// Time in miliseconds after which each connection is reseted
	const std::string HOST_IP = "169.254.226.41";	// Default ip for redis server
	const std::string HOST_PASSWD = "";				// Redis server password
	const int DEFAULT_PORT = 7000;

	/**************************************** Data structures stored in Redis *************************************/

	const std::string sASSETS = "ASSETS";					// Asset set

	// Sets of IDs							
	const std::string sTypeListSep = ":TYPELIST";
	const std::string sDeviceListSep = ":DEVICELIST";
	const std::string sTypeVarsSep = ":TYPEVARS:";

	// Keys for the data
	const std::string sDeviceSep = ":DEVICE:";
	const std::string sDeviceTypeListSep = ":DEVICETYPELIST:";
	const std::string sTypeInfoSep = ":TYPEINFO:";

	/********************************************* General variable info *****************************************************/
	const std::string sFormat = ":sFormat";
	const std::string sChangeEvent = ":nChangeEvent";
	const std::string sAlarmEvent = ":nAlarmEvent";
	const std::string sHistEvent = ":nHistEvent";
	const std::string sType = ":cType";

	/************************************************** Variable data *******************************************************/
	const std::string sValue = ":sValue";
	const std::string sDeviceType = "sDeviceType";
	const std::string sDataTimestamp = ":nDataTimestamp";
	const std::string sWriteTimestamp = ":nWriteTimestamp";
	const std::string sQuality = ":nQuality";

	// Performance of the cache, change it to see how does affect performance
	const int PIPE_SIZE = 1000;					// Batch size for redis commands
	const int BUFFER_LATENCY = 10;				// Each n ms the buffer sends all the writes
	const int CACHE_LATENCY = 100;				// Each n ms the cache updates the local data
	const bool USE_BUFFER = true;				// Whether the writes should be buffered or immediate
	const bool USE_CACHE = true;				// Whether the reads should be cached (bringing several reads in block so they will already be local the next time)
	const bool DEBUG = false;					// Whether the handler should check incorrect use of the operations (e.g. writing a variable before creating its device)
	const int nConcurrencyCache = 1;			// Number of concurrent connections the cache should hold
												// One for each thread that will use the cache (e.g. one for each asset manager)

	using OptionalString = std::optional<std::string>;
	using OptionalStringSet = std::set<OptionalString>;
	using StringSet = std::set<std::string>;
	using DeviceSet = std::list<ST_DEVICE>;
	using VariableSet = std::list<ST_DEVICE_TYPE_VARIABLE>;
	using TypeSet = std::list<ST_DEVICE_TYPE>;
	using AssetSet = std::list<std::string>;

	/******************************************************************** Internal data *******************************************************************************/
	
	sw::redis::ConnectionOptions options;							// Redis initializing options 

	// Local variables
	//std::shared_ptr<CLogWriter> pRTDB_Logger;						// Logger
	Redis_cache dbCache;							// Write buffer

	/**************************************************************************************************/
	//	NAME:			scanSet
	//	DESCRIPTION:	Gets a full set using sscan calls
	//	IN PARAMETERS:	pRedis		Shared pointer to a redis object
	//					sKey		Key of the set to be scaned
	//	OUT PARAMETERS: out			Inserter to store the output (same as Redis++ syntax)
	//	RETURNS:		NONE
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/11	HMT		Creation
	/**************************************************************************************************/
	template<typename Output>
	void scanSet(Redis_cache &pRedis, const std::string& sKey, Output& out)
	{
		long long cursor = 0;
		do
		{															// Gets all devices in the block
			cursor = dbCache.sscan(sKey, cursor, PIPE_SIZE, out);
		} while (cursor > 0);
	}

	
	/**************************************************************************************************/
	//	NAME:			initializeOptions
	//	DESCRIPTION:	Auxiliar function to initialize a redis ConnectionOptions object
	//	IN PARAMETERS:	host		String with the ip of the redis server (format: "127.0.0.1")
	//					port		Port where the redis server is listening
	//					passwd		Password of the redis server
	//					db			Id of the database to be used
	//	OUT PARAMETERS: NONE
	//	RETURNS:		Initialized connection options
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/25	HMT		Creation
	/**************************************************************************************************/
	sw::redis::ConnectionOptions initializeOptions(const std::string& host, const int& port, const std::string& passwd, const int& db)
	{
		sw::redis::ConnectionOptions opt;
		opt.host = host;
		opt.port = port;
		opt.password = passwd;
		opt.db = db;

		return opt;
	}

public:

	/**************************************************************************************************/
	//	NAME:			Redis_handler
	//	DESCRIPTION:	Class constructor, initializes connection with the redis server
	//	IN PARAMETERS:	szConnectionString		ODBC connection string
	//					bCreateUpdateThread		Indicates if a thread to periodically update assets is created.
	//					pLogger					Pointer to the logger the class will use
	//					host					Redis server ip (format: "127.0.0.1")
	//					port					Redis server port
	//					passwd					Redis server password
	//					db						Database id inside the redis server
	//	OUT PARAMETERS: NONE
	//	RETURNS:		NONE
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/11	HMT		Creation
	/**************************************************************************************************/
	Redis_handler(bool bCreateUpdateThread, const std::string& _host, const int& _port, const std::string& _passwd, const int& _db) :
		options (initializeOptions(_host, _port, _passwd, _db)),
		dbCache(Redis_cache(options, 
						BUFFER_LATENCY, CACHE_LATENCY, PIPE_SIZE, 
						USE_BUFFER, USE_CACHE,
						nConcurrencyCache))		// Creates the buffer
	{
		//********************************************************
		//
		// Load configuration from database (substitute global constants)
		//
		//
		//
		// ******************************************************
		std::cout << dbCache.ping() << std::endl;
	}

	/**************************************************************************************************/
	//	NAME:			Redis_handler
	//	DESCRIPTION:	Class constructor, initializes connection with the redis server. Loads redis server data from default configuration.
	//	IN PARAMETERS:	szConnectionString		ODBC connection string
	//					bCreateUpdateThread		Indicates if a thread to periodically update assets is created.
	//					pLogger					Pointer to the logger the class will use
	//	OUT PARAMETERS: NONE
	//	RETURNS:		NONE
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/11	HMT		Creation
	/**************************************************************************************************/
	Redis_handler(bool bCreateUpdateThread) :
		Redis_handler(bCreateUpdateThread, HOST_IP, DEFAULT_PORT, HOST_PASSWD, HOST_DB) {}
	
	// Redis_handler is not copyable (restriction of Redis++)
	Redis_handler(const Redis_handler& rh) = delete;

	~Redis_handler() = default;

	void printBlockTime()
	{
		//std::cout << "tiempo de bloque: " << dbCache.getBlockTime() << std::endl;
		//std::cout << "hits: " << dbCache.gethits() << ", misses: " << dbCache.getmisses() << std::endl;
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_AddAsset
	//	DESCRIPTION:	Add a new asset
	//	IN PARAMETERS:	sAsset				Asset name
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE if asset is added, FALSE otherwise
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/14	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_AddAsset(const std::string &sAsset)
	{
		return (dbCache.sadd(sASSETS, sAsset) == 1);			// Adds asset to the asset list
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_RemoveAsset
	//	DESCRIPTION:	Removes an existing asset
	//	IN PARAMETERS:	sAsset				Asset name
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE if the asset is removed, FALSE otherwise
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/14	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_RemoveAsset(const std::string &sAsset)
	{
		if (dbCache.srem(sASSETS, sAsset) == 1)						// The asset does exist
		{
			StringSet devices;
			scanSet(dbCache, sAsset + sDeviceListSep, std::inserter(devices, devices.begin()));		// Gets devices in the asset

			StringSet types;
			scanSet(dbCache, sAsset + sTypeListSep, std::inserter(types, types.begin()));			// Gets types in the asset
			
			dbCache.del(sAsset + sDeviceListSep);					// Deletes the asset device list
			dbCache.del(sAsset + sTypeListSep);						// Deletes the asset type list

			for (auto dev : devices)								// Deletes all devices in the asset
			{
				dbCache.del(sAsset + sDeviceSep + dev);
			}
			
			for (auto sType : types)								// For each type in the asset
			{
				dbCache.del(sAsset + sDeviceTypeListSep + sType);			// Removes the type devices list 
				dbCache.del(sAsset + sTypeInfoSep + sType);					// Deletes all type info
				dbCache.del(sAsset + sTypeVarsSep + sType);					// Deletes all type variables
			}

			return true;
		}

		return false;			
	}


	/**************************************************************************************************/
	//	NAME:			RTDB_GetUsedEntries
	//	DESCRIPTION:	Get the number of devices inside the asset
	//	IN PARAMETERS:	NONE
	//	OUT PARAMETERS: NONE
	//	RETURNS:		NONE
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/11	HMT		Creation
	/**************************************************************************************************/
	uint32_t RTDB_GetUsedEntries(const std::string &sAsset)
	{
		return (uint32_t)dbCache.scard(sAsset + sDeviceListSep);						// Gets the size of the asset device list set
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_GetDeviceTypes
	//	DESCRIPTION:	Get the list of device types in the RTDB
	//	IN PARAMETERS:  sAsset			Asset name
	//	OUT PARAMETERS: NONE
	//	RETURNS:		Shared pointer to list of device types
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/11	HMT		Creation
	/**************************************************************************************************/
	TEQ_LIST_PTR RTDB_GetDeviceTypes(const std::string &sAsset)
	{
		StringSet sSet;
		TEQ_LIST_PTR typeSet (new TEQ_LIST ());

		scanSet(dbCache, sAsset + sTypeListSep, std::inserter(sSet, sSet.begin()));		// Gets the types in the asset

		for (auto &sType : sSet)															// For each type
		{	
			ST_DEVICE_TYPE temp;
			
			temp.sDeviceTypeName	= sType;												// Stores the obtained values
			temp.nNumberDevices 	= dbCache.scard(sAsset + sDeviceTypeListSep + sType);	// Gets the number of devices of the type
			temp.nNumberVariables	= dbCache.scard(sAsset + sTypeVarsSep + sType);			// Gets the number of variables of the type

			typeSet->push_front(temp);														// Adds the type to the set
		}		

		return typeSet;																	// Returns the set
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_GetVariables
	//	DESCRIPTION:	Get the list of variables for the given type of device
	//	IN PARAMETERS:	sAsset			Asset name
	//					stDeviceType	Reference to device type struct
	//	OUT PARAMETERS: NONE
	//	RETURNS:		Shared pointer to list of variables for the given device type.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/13	HMT		Creation
	/**************************************************************************************************/
	VAR_LIST_PTR RTDB_GetVariables(const std::string &sAsset, const std::string &sTypeOfDevice)
	{
		StringSet sSet;
		VAR_LIST_PTR vSet (new VAR_LIST ());
		std::string typeInfoKey = sAsset + sTypeInfoSep + sTypeOfDevice;									// Builds the key of the type info

		scanSet(dbCache, sAsset + sTypeVarsSep + sTypeOfDevice, std::inserter(sSet, sSet.begin())); 		// Obtains all variables
		
		for (auto &varName : sSet)															// For each variable
		{
			ST_DEVICE_TYPE_VARIABLE var;

			var.sVariableName 	= varName;															// Variable name
			var.sFormat			= *dbCache.hget(typeInfoKey, varName + sFormat);					// Variable format
			var.nChangeEvent 	= stoi(*dbCache.hget(typeInfoKey, varName + sChangeEvent));			// Variable nChangeEvent
			var.nAlarmEvent 	= stoi(*dbCache.hget(typeInfoKey, varName + sAlarmEvent));			// Variable nAlarmEvent
			var.nHistEvent 		= stoi(*dbCache.hget(typeInfoKey, varName + sHistEvent));			// Variable nHistEvent
			var.cType 			= (dbCache.hget(typeInfoKey, varName + sType))->c_str()[0];			// Variable cType, gets the first character of the string
			
			vSet->push_front(var);																	// Stores the variable info in the output set
		}

		return vSet;
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_GetDevice
	//	DESCRIPTION:	Get a device info by its name
	//	IN PARAMETERS:  sAsset			Asset name	
	//	sDevice			Device name
	//	OUT PARAMETERS: stDevice		Output device struct
	//	RETURNS:		TRUE if device exists and could be retrieved, false otherwise
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/13	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_GetDevice(const std::string &sAsset, const std::string &sDevice, ST_DEVICE &stDevice)
	{
		OptionalString deviceType = 
				dbCache.hget(sAsset + sDeviceSep + sDevice, sDeviceType + sValue);	// Gets device type

		if (deviceType.has_value())														// Checks if the string has value (Device exists)
		{
			stDevice.sDeviceType + sValue = *deviceType;									// Device type
			stDevice.sDeviceName = sDevice;													// Device name
			stDevice.sAsset = sAsset;														// Device asset

			return true;
		}
		
		return false;																// Device didn't exist
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_GetDevices
	//	DESCRIPTION:	Get the list of devices for a given device type
	//	IN PARAMETERS:	sAsset			Asset name
	//					sDeviceType + sValue		String identifier for the device type
	//	OUT PARAMETERS: NONE
	//	RETURNS:		Shared pointer to list of devices
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/13	HMT		Creation
	/**************************************************************************************************/
	EQ_LIST_PTR RTDB_GetDevices(const std::string &sAsset, const std::string &sDeviceType)
	{
		StringSet sSet;
		EQ_LIST_PTR dvSet (new EQ_LIST ());

		scanSet(dbCache, sAsset + sDeviceTypeListSep + sDeviceType + sValue, std::inserter(sSet, sSet.begin()));

		for (auto &sDevice : sSet)								// For each device
		{
			ST_DEVICE temp;

			temp.sDeviceName = sDevice;								// Device name
			temp.sDeviceType = sDeviceType;							// Device type
			temp.sAsset = sAsset;									// Device asset

			dvSet->push_front(temp);								// Adds the device to the results set
		}
		
		return dvSet;
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_GetValue
	//	DESCRIPTION:	Get value associated to a device variable
	//	IN PARAMETERS:	sAsset			Asset name
	//					sDevice			Device name
	//					szVariable		Variable name
	//	OUT PARAMETERS: Output			Reference to the destination structure
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/13	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_GetValue(const std::string &sAsset, const std::string &sDevice, const std::string &sVariable, ST_TAG_VALUE &Output)
	{
		auto value = dbCache.hget(sAsset + sDeviceSep + sDevice, sVariable + sValue);												// Gets variable value
		if (value)																													// Checks if the variable exists
		{
			Output.sValue			= *value;																							// Stores value
			Output.nDataTimestamp	= (int64_t)stoi(*dbCache.hget(sAsset + sDeviceSep + sDevice, sVariable + sDataTimestamp));			// Stores nDataTimestamp (int64_t)stoi(dataTimestamp)
			Output.nWriteTimestamp	= (int64_t)stoi(*dbCache.hget(sAsset + sDeviceSep + sDevice, sVariable + sWriteTimestamp));			// Stores nWriteTimestamp (int64_t)stoi(writeTimestamp)
			Output.nQuality			= (int8_t)stoi(*dbCache.hget(sAsset + sDeviceSep + sDevice, sVariable + sQuality));					// Stores nQuality (int8_t)stoi(sQuality)

			return true;
		}
		
		return false;																													// Device didn't exist
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_SetValue
	//	DESCRIPTION:	Set value associated to a device variable
	//	IN PARAMETERS:	sAsset			Asset name
	//					sDevice		Device name
	//					szVariable		Variable name
	//					pInput			Pointer of source value from which the value will be copied
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/13	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_SetValue(const std::string& sAsset, const std::string& sDevice, const std::string& sVariable, const ST_TAG_VALUE& Input)
	{
		if (sVariable == sDeviceType)																					// Shouldn't change device info variables
			return false;

		if (DEBUG)																										// Checks that the previous structures exist, huge performance penalty
		{
			if(!dbCache.sismember(sAsset + sDeviceListSep, sDevice))															// Checks that the device exists
				return false;

			auto deviceType = dbCache.hget(sAsset + sDeviceSep + sDevice, sDeviceType + sValue);								// Gets variable time

			if(!dbCache.sismember(sAsset + sTypeVarsSep + *deviceType, sVariable))												// Checks variable exists in the type	
				return false;	
		}

		dbCache.hset(sAsset + sDeviceSep + sDevice, sVariable + sDataTimestamp, std::to_string(Input.nDataTimestamp));			// Sets sDataTimestamp
		dbCache.hset(sAsset + sDeviceSep + sDevice, sVariable + sWriteTimestamp, std::to_string(Input.nDataTimestamp));			// Sets sWriteTimestamp
		dbCache.hset(sAsset + sDeviceSep + sDevice, sVariable + sQuality, std::to_string(Input.nQuality));						// Sets sQuality

		return dbCache.hset(sAsset + sDeviceSep + sDevice, sVariable + sValue, Input.sValue);									// Sets value	
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_AddDevice
	//	DESCRIPTION:	Add a device to the specified device type
	//	IN PARAMETERS:	hRTDB			Handle to the RTDB
	//					sAsset			Asset name
	//					sTypeOfDevice	Name of type of device
	//					sDevice			Name of new device
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/13	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_AddDevice(const std::string& sAsset, const std::string& sTypeOfDevice, const std::string& sDevice)
	{
		if (DEBUG)																						// Checks that the previous structures exist, huge performance penalty
		{
			if(!dbCache.sismember(sASSETS, sAsset))																// Checks that the asset exists
				return false;

			if(!dbCache.sismember(sAsset + sTypeListSep, sTypeOfDevice))										// Checks that the type exist
				return false;
		}

		dbCache.sadd(sAsset + sDeviceListSep, sDevice);													// Adds device to asset devices set
		dbCache.sadd(sAsset + sDeviceTypeListSep + sTypeOfDevice, sDevice);								// Adds device to type devices set
		
		return dbCache.hset(sAsset + sDeviceSep + sDevice, sDeviceType + sValue, sTypeOfDevice);		// Sets device type
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_DelDevice
	//	DESCRIPTION:	Delete a device
	//	IN PARAMETERS:	sAsset			Asset name
	//					sDevice		Name of new device
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/14	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_DelDevice(const std::string& sAsset, const std::string& sDevice)
	{
		auto deviceType = dbCache.hget(sAsset + sDeviceSep + sDevice, sDeviceType + sValue);		// Gets device type

		if (deviceType)																				// Device existed
		{
			dbCache.srem(sAsset + sDeviceTypeListSep + *deviceType, sDevice);						// Removes device from the type device set
			dbCache.srem(sAsset + sDeviceListSep, sDevice);											// Removes device from the asset device set

			return dbCache.del(sAsset + sDeviceSep + sDevice);										// Removes device variables
		}
		
		return false;																		// Device didn't exist
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_DelVarToTypeOfDevice
	//	DESCRIPTION:	Delete a variable of a device type
	//	IN PARAMETERS:	sAsset			Name of asset
	//					sTypeOfDevice	Name of device type
	//					szVariable		Name of variable
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/14	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_DelVarToTypeOfDevice(const std::string& sAsset, const std::string& sTypeOfDevice, const std::string& sVariable)
	{		
		OptionalStringSet oSet;
		StringSet sSet;

		if (dbCache.srem(sAsset + sTypeVarsSep + sTypeOfDevice, sVariable) == 0)							// Removes variable from the device type variables list
			return false;																							// Variable or type didn't exist

		scanSet(dbCache, sAsset + sDeviceTypeListSep + sTypeOfDevice, std::inserter(oSet, oSet.begin()));	// Gets all devices of the given type

		std::string aux = sAsset + sTypeInfoSep + sTypeOfDevice;											// Builds key of the type info
		dbCache.hdel(aux, sVariable + sFormat);																// Deletes variable info
		dbCache.hdel(aux, sVariable + sChangeEvent);
		dbCache.hdel(aux, sVariable + sAlarmEvent);
		dbCache.hdel(aux, sVariable + sHistEvent);
		dbCache.hdel(aux, sVariable + sType);

		for (auto elem : oSet)																				// For each device of the type sTypeOfDevice
		{
			sSet.insert(sVariable + sValue);																		// Tag atributes
			sSet.insert(sVariable + sDataTimestamp);		
			sSet.insert(sVariable + sWriteTimestamp);
			sSet.insert(sVariable + sQuality);
			
			dbCache.hdel(sAsset + sDeviceSep + *elem, sSet.begin(), sSet.end());									// Deletes all tags of the variable

			sSet.clear();
		}

		return true;					
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_AddVarToTypeOfDevice
	//	DESCRIPTION:	Add a variable to device type
	//	IN PARAMETERS:	hRTDB			Handle to the RTDB
	//					sAsset			Asset name
	//					sTypeOfDevice	Name of device type
	//					newVar			Struct with new var
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/14	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_AddVarToTypeOfDevice(const std::string &sAsset, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
	{
		std::string aux = sAsset + sTypeInfoSep + sTypeOfDevice;											// Builds key of the type info
		dbCache.hset(aux, newVar.sVariableName + sFormat, newVar.sFormat);									// Sets variable info
		dbCache.hset(aux, newVar.sVariableName + sChangeEvent, std::to_string(newVar.nChangeEvent));
		dbCache.hset(aux, newVar.sVariableName + sAlarmEvent, std::to_string(newVar.nAlarmEvent));
		dbCache.hset(aux, newVar.sVariableName + sHistEvent, std::to_string(newVar.nHistEvent));
		dbCache.hset(aux, newVar.sVariableName + sType, std::to_string(newVar.cType));

		return (dbCache.sadd(sAsset + sTypeVarsSep + sTypeOfDevice, newVar.sVariableName) == 1);			// Adds the variable to the type variables list
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_ModifyVarToTypeOfDevice
	//	DESCRIPTION:	Modify a variable to device type
	//	IN PARAMETERS:	sAsset			Name of asset
	//					sTypeOfDevice	Name of device type
	//					newVar			Struct with new var parameters
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/14	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_ModifyVarToTypeOfDevice(const std::string &sAsset, const std::string &sTypeOfDevice, const ST_DEVICE_TYPE_VARIABLE &newVar)
	{	
		if (DEBUG)			
		{
			if (!dbCache.sismember(sAsset + sTypeVarsSep + sTypeOfDevice, newVar.sVariableName))	// Checks if variable already exist
				return false;
		}

		return RTDB_AddVarToTypeOfDevice(sAsset, sTypeOfDevice, newVar);														// Overwrites variable

	}

	/**************************************************************************************************/
	//	NAME:			RTDB_AddTypeOfDevice
	//	DESCRIPTION:	Adds the device to the known devices in the asset
	//	IN PARAMETERS:	sAsset			Asset name
	//					sTypeOfDevice  Device name
	//	OUT PARAMETERS: NONE
	//	RETURNS:		True if the device didn't exist and has been added, false otherwise
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/12	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_AddTypeOfDevice(const std::string& sAsset, const std::string& sTypeOfDevice)
	{
		return (dbCache.sadd(sAsset + sTypeListSep, sTypeOfDevice) == 1);			// Adds the element
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_AddTypeOfDevice
	//	DESCRIPTION:	Adds the device to the known devices in the asset
	//	IN PARAMETERS:	sAsset			Asset name
	//					begin			Iterator to the begginning of the data
	//					end				Iterator to the end of the data
	//	OUT PARAMETERS: NONE
	//	RETURNS:		True if at least one device didn't exist and has been added, false otherwise
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/25	HMT		Creation
	/**************************************************************************************************/
	template <typename Input>
	bool RTDB_AddTypeOfDevice(const std::string& sAsset, Input begin, Input end)
	{
		return (dbCache.sadd(sAsset + sTypeListSep, begin, end) > 1);				// Adds all elements
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_DelTypeOfDevice
	//	DESCRIPTION:	Delete device type
	//	IN PARAMETERS:	sAsset			Name of Asset
	//					sTypeOfDevice	Name of device type
	//	OUT PARAMETERS: NONE
	//	RETURNS:		TRUE on success, FALSE on failure.
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/12	HMT		Creation
	/**************************************************************************************************/
	bool RTDB_DelTypeOfDevice(const std::string &sAsset, const std::string &sTypeOfDevice)
	{		
		if (dbCache.srem(sAsset + sTypeListSep, sTypeOfDevice) == 1)												// The type was removed from the asset type list
		{
			StringSet sSet;
			scanSet(dbCache, sAsset + sDeviceTypeListSep + sTypeOfDevice, std::inserter(sSet, sSet.begin())); 		// Gets all devices of sTypeOfDevice type

			dbCache.del(sAsset + sTypeInfoSep + sTypeOfDevice);														// Deletes all type info
			dbCache.del(sAsset + sTypeVarsSep + sTypeOfDevice);														// Deletes all type variables
			dbCache.del(sAsset + sDeviceTypeListSep + sTypeOfDevice);												// Removes the type devices list 

			for (auto device : sSet)																				// For each device of type sTypeOfDevice
			{
				dbCache.del(sAsset + sDeviceSep + device);																	// Deletes all variables of the device								
				dbCache.srem(sAsset + sDeviceListSep, device);																// Removes the device from the asset device list
			}

			return true;
		}

		return false;
	}

	/**************************************************************************************************/
	//	NAME:			RTDB_GetAssets
	//	DESCRIPTION:	Gets the list of assets
	//	IN PARAMETERS:  NONE
	//	OUT PARAMETERS: NONE
	//	RETURNS:		A list of assets
	//	HISTORY:
	//		DATE		AUTHOR	DESCRIPTION
	//		2022/07/12	HMT		Creation
	/**************************************************************************************************/
	ASSET_LIST RTDB_GetAssets()
	{
		ASSET_LIST sSet;
		scanSet(dbCache, sASSETS, std::inserter(sSet, sSet.begin()));		// Gets all asset names

		return sSet;
	}	
};