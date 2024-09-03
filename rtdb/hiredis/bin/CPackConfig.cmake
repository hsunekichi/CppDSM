# This file will be configured to contain variables for CPack. These variables
# should be set in the CMake list file of the project before CPack module is
# included. The list of available CPACK_xxx variables and their associated
# documentation may be obtained using
#  cpack --help-variable-list
#
# Some variables are common to all generators (e.g. CPACK_PACKAGE_NAME)
# and some are specific to a generator
# (e.g. CPACK_NSIS_EXTRA_INSTALL_COMMANDS). The generator specific variables
# usually begin with CPACK_<GENNAME>_xxxx.


set(CPACK_BINARY_7Z "OFF")
set(CPACK_BINARY_IFW "OFF")
set(CPACK_BINARY_NSIS "ON")
set(CPACK_BINARY_NUGET "OFF")
set(CPACK_BINARY_WIX "OFF")
set(CPACK_BINARY_ZIP "OFF")
set(CPACK_BUILD_SOURCE_DIRS "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/hiredis;C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/hiredis/bin")
set(CPACK_CMAKE_GENERATOR "Visual Studio 15 2017")
set(CPACK_COMPONENTS_ALL "")
set(CPACK_COMPONENT_UNSPECIFIED_HIDDEN "TRUE")
set(CPACK_COMPONENT_UNSPECIFIED_REQUIRED "TRUE")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS "ON")
set(CPACK_DEFAULT_PACKAGE_DESCRIPTION_FILE "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericDescription.txt")
set(CPACK_DEFAULT_PACKAGE_DESCRIPTION_SUMMARY "hiredis built using CMake")
set(CPACK_GENERATOR "NSIS")
set(CPACK_INSTALL_CMAKE_PROJECTS "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/hiredis/bin;hiredis;ALL;/")
set(CPACK_INSTALL_PREFIX "C:/Program Files (x86)/hiredis")
set(CPACK_MODULE_PATH "")
set(CPACK_NSIS_DISPLAY_NAME "hiredis 1.0.3")
set(CPACK_NSIS_INSTALLER_ICON_CODE "")
set(CPACK_NSIS_INSTALLER_MUI_ICON_CODE "")
set(CPACK_NSIS_INSTALL_ROOT "$PROGRAMFILES")
set(CPACK_NSIS_PACKAGE_NAME "hiredis 1.0.3")
set(CPACK_NSIS_UNINSTALL_NAME "Uninstall")
set(CPACK_OUTPUT_CONFIG_FILE "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/hiredis/bin/CPackConfig.cmake")
set(CPACK_PACKAGE_CONTACT "michael dot grunder at gmail dot com")
set(CPACK_PACKAGE_DEFAULT_LOCATION "/")
set(CPACK_PACKAGE_DESCRIPTION "Hiredis is a minimalistic C client library for the Redis database.

It is minimalistic because it just adds minimal support for the protocol, but at the same time it uses a high level printf-alike API in order to make it much higher level than otherwise suggested by its minimal code base and the lack of explicit bindings for every Redis command.

Apart from supporting sending commands and receiving replies, it comes with a reply parser that is decoupled from the I/O layer. It is a stream parser designed for easy reusability, which can for instance be used in higher level language bindings for efficient reply parsing.

Hiredis only supports the binary-safe Redis protocol, so you can use it with any Redis version >= 1.2.0.

The library comes with multiple APIs. There is the synchronous API, the asynchronous API and the reply parsing API.")
set(CPACK_PACKAGE_DESCRIPTION_FILE "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericDescription.txt")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "hiredis built using CMake")
set(CPACK_PACKAGE_FILE_NAME "hiredis-1.0.3-win32")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/redis/hiredis")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "hiredis 1.0.3")
set(CPACK_PACKAGE_INSTALL_REGISTRY_KEY "hiredis 1.0.3")
set(CPACK_PACKAGE_NAME "hiredis")
set(CPACK_PACKAGE_RELOCATABLE "true")
set(CPACK_PACKAGE_VENDOR "Redis")
set(CPACK_PACKAGE_VERSION "1.0.3")
set(CPACK_PACKAGE_VERSION_MAJOR "1")
set(CPACK_PACKAGE_VERSION_MINOR "0")
set(CPACK_PACKAGE_VERSION_PATCH "3")
set(CPACK_RESOURCE_FILE_LICENSE "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericLicense.txt")
set(CPACK_RESOURCE_FILE_README "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericDescription.txt")
set(CPACK_RESOURCE_FILE_WELCOME "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericWelcome.txt")
set(CPACK_RPM_PACKAGE_AUTOREQPROV "ON")
set(CPACK_SET_DESTDIR "OFF")
set(CPACK_SOURCE_7Z "ON")
set(CPACK_SOURCE_GENERATOR "7Z;ZIP")
set(CPACK_SOURCE_OUTPUT_CONFIG_FILE "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/hiredis/bin/CPackSourceConfig.cmake")
set(CPACK_SOURCE_ZIP "ON")
set(CPACK_SYSTEM_NAME "win32")
set(CPACK_THREADS "1")
set(CPACK_TOPLEVEL_TAG "win32")
set(CPACK_WIX_SIZEOF_VOID_P "4")

if(NOT CPACK_PROPERTIES_FILE)
  set(CPACK_PROPERTIES_FILE "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/hiredis/bin/CPackProperties.cmake")
endif()

if(EXISTS ${CPACK_PROPERTIES_FILE})
  include(${CPACK_PROPERTIES_FILE})
endif()
