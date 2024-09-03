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


set(CPACK_BUILD_SOURCE_DIRS "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/redis-plus-plus;C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/redis-plus-plus")
set(CPACK_CMAKE_GENERATOR "Visual Studio 15 2017")
set(CPACK_COMPONENT_UNSPECIFIED_HIDDEN "TRUE")
set(CPACK_COMPONENT_UNSPECIFIED_REQUIRED "TRUE")
set(CPACK_DEBIAN_FILE_NAME "DEB-DEFAULT")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libstdc++6, libhiredis-dev")
set(CPACK_DEBIAN_PACKAGE_GENERATE_SHLIBS "ON")
set(CPACK_DEBIAN_PACKAGE_SOURCE "https://github.com/sewenew/redis-plus-plus")
set(CPACK_DEBIAN_PACKAGE_VERSION "1.3.5")
set(CPACK_DEFAULT_PACKAGE_DESCRIPTION_FILE "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericDescription.txt")
set(CPACK_DEFAULT_PACKAGE_DESCRIPTION_SUMMARY "redis++ built using CMake")
set(CPACK_GENERATOR "DEB")
set(CPACK_INSTALL_CMAKE_PROJECTS "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/redis-plus-plus;redis++;ALL;/")
set(CPACK_INSTALL_PREFIX "C:/Program Files (x86)/redis++")
set(CPACK_MODULE_PATH "")
set(CPACK_NSIS_DISPLAY_NAME "libredis++-dev 1.3.5")
set(CPACK_NSIS_INSTALLER_ICON_CODE "")
set(CPACK_NSIS_INSTALLER_MUI_ICON_CODE "")
set(CPACK_NSIS_INSTALL_ROOT "$PROGRAMFILES")
set(CPACK_NSIS_PACKAGE_NAME "libredis++-dev 1.3.5")
set(CPACK_NSIS_UNINSTALL_NAME "Uninstall")
set(CPACK_OUTPUT_CONFIG_FILE "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/redis-plus-plus/CPackConfig.cmake")
set(CPACK_PACKAGE_CONTACT "anonymous")
set(CPACK_PACKAGE_DEFAULT_LOCATION "/")
set(CPACK_PACKAGE_DESCRIPTION "A pure C++ client for Redis, based on hiredis.")
set(CPACK_PACKAGE_DESCRIPTION_FILE "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericDescription.txt")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "redis++ built using CMake")
set(CPACK_PACKAGE_FILE_NAME "libredis++-dev-1.3.5-win32")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "libredis++-dev 1.3.5")
set(CPACK_PACKAGE_INSTALL_REGISTRY_KEY "libredis++-dev 1.3.5")
set(CPACK_PACKAGE_NAME "libredis++-dev")
set(CPACK_PACKAGE_RELOCATABLE "true")
set(CPACK_PACKAGE_VENDOR "Humanity")
set(CPACK_PACKAGE_VERSION "1.3.5")
set(CPACK_PACKAGE_VERSION_MAJOR "1")
set(CPACK_PACKAGE_VERSION_MINOR "3")
set(CPACK_PACKAGE_VERSION_PATCH "5")
set(CPACK_RESOURCE_FILE_LICENSE "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericLicense.txt")
set(CPACK_RESOURCE_FILE_README "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericDescription.txt")
set(CPACK_RESOURCE_FILE_WELCOME "C:/Program Files/CMake/share/cmake-3.24/Templates/CPack.GenericWelcome.txt")
set(CPACK_SET_DESTDIR "OFF")
set(CPACK_SOURCE_7Z "ON")
set(CPACK_SOURCE_GENERATOR "7Z;ZIP")
set(CPACK_SOURCE_OUTPUT_CONFIG_FILE "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/redis-plus-plus/CPackSourceConfig.cmake")
set(CPACK_SOURCE_ZIP "ON")
set(CPACK_SYSTEM_NAME "win32")
set(CPACK_THREADS "1")
set(CPACK_TOPLEVEL_TAG "win32")
set(CPACK_WIX_SIZEOF_VOID_P "4")

if(NOT CPACK_PROPERTIES_FILE)
  set(CPACK_PROPERTIES_FILE "C:/SVN/SIDERA_VS2017/Librerias/Librerias_Externas/redis-plus-plus/CPackProperties.cmake")
endif()

if(EXISTS ${CPACK_PROPERTIES_FILE})
  include(${CPACK_PROPERTIES_FILE})
endif()
