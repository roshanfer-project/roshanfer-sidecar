cmake_minimum_required(VERSION 3.15)
project(sidecar LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# generate compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -gdwarf-3 -fno-omit-frame-pointer")
# options: "-fsanitize=address": to see memory leaks, stack corruption, etc.

# enable clang-tidy
#set(CMAKE_CXX_CLANG_TIDY
#  clang-tidy;
#  -header-filter=.;)

# append -Werror=format to CMAKE_CXX_FLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=format -Wshadow -Wnull-dereference -Wuninitialized -Wconversion -Wreorder -Wsign-compare -Wnon-virtual-dtor -Woverloaded-virtual -Wall -Wextra")
# -Wrange-loop-analysis

# find glog
find_package (glog 0.7.1 REQUIRED)

# find liburing
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

# build yaml-cpp as external project
set(YAML_CPP_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/external/yaml-cpp)
set(YAML_CPP_BUILD_DIR ${YAML_CPP_ROOT}/build)
set(YAML_CPP_LIB ${YAML_CPP_BUILD_DIR}/libyaml-cpp.a)

# Build yaml-cpp if not already built
if(NOT EXISTS ${YAML_CPP_LIB})
  message(STATUS "Building yaml-cpp...")
  execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/external/build-yaml-cpp.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/external
    RESULT_VARIABLE YAML_BUILD_RESULT
  )
  if(NOT YAML_BUILD_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to build yaml-cpp")
  endif()
endif()

# Create imported target for yaml-cpp
add_library(yaml-cpp STATIC IMPORTED)
set_target_properties(yaml-cpp PROPERTIES
  IMPORTED_LOCATION ${YAML_CPP_LIB}
  INTERFACE_INCLUDE_DIRECTORIES ${YAML_CPP_ROOT}/include
)

# find nghttp2
pkg_check_modules(NGHTTP2 REQUIRED libnghttp2)

# add hdrhistogram
add_subdirectory(external/HdrHistogram_c)

# add NanoLog
include_directories(include external/NanoLog/runtime)

file(GLOB_RECURSE SOURCES src/*.cc src/*.c)

add_executable(sidecar ${SOURCES})
# link glog
target_link_libraries (sidecar glog::glog)
# link liburing
target_link_libraries(sidecar ${LIBURING_LIBRARIES})
# link yaml-cpp
target_link_libraries(sidecar yaml-cpp)
# link nghttp2
target_link_libraries(sidecar ${NGHTTP2_LIBRARIES})
# link nanolog
target_link_libraries(sidecar ${CMAKE_CURRENT_SOURCE_DIR}/external/NanoLog/runtime/libNanoLog.a)
# link pthread
target_link_libraries(sidecar pthread)
# link rt
target_link_libraries(sidecar rt)
# link HdrHistogram
target_link_libraries(sidecar hdr_histogram_static)