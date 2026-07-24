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

option(SIDECAR_ENABLE_NANOLOG "Compile sidecar with NANO_LOG_ENABLED (metric export via NanoLog)" OFF)
# NABO_LOG_TRACE_ENABLED: optional trace; keep off unless needed

# enable hardening
add_compile_definitions(_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_EXTENSIVE)

# Use libc++ (pin LLVM 18 headers when libc++-22 is also installed)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
set(LIBCXX_ROOT "/usr/lib/llvm-18" CACHE PATH "libc++ root matching Clang 18")
if(NOT EXISTS "${LIBCXX_ROOT}/include/c++/v1")
  message(FATAL_ERROR "libc++ headers not found at ${LIBCXX_ROOT}. Install: apt install libc++-18-dev libc++abi-18-dev")
endif()
add_compile_options(-nostdinc++ -isystem "${LIBCXX_ROOT}/include/c++/v1")
add_link_options(-L"${LIBCXX_ROOT}/lib" -Wl,-rpath,"${LIBCXX_ROOT}/lib")

# enable clang-tidy
#set(CMAKE_CXX_CLANG_TIDY
#  clang-tidy;
#  -header-filter=.;)

# append -Werror=format to CMAKE_CXX_FLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=format -Wshadow -Wnull-dereference -Wuninitialized -Wconversion -Wreorder -Wsign-compare -Wnon-virtual-dtor -Woverloaded-virtual -Wall -Wextra")
# -Wrange-loop-analysis

# Build dependencies (glog, googletest, NanoLog, yaml-cpp)
execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/external/setup_deps.sh WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} RESULT_VARIABLE DEPS_RESULT)
if(NOT DEPS_RESULT EQUAL 0)
  message(FATAL_ERROR "Failed to build dependencies")
endif()

# glog
set(glog_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/glog_install/lib/cmake/glog)
find_package(glog REQUIRED)

# googletest
set(GTest_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external/googletest_install/lib/cmake/GTest)
find_package(GTest REQUIRED)

# find liburing
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

# yaml-cpp
set(YAML_CPP_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/external/yaml-cpp)
add_library(yaml-cpp STATIC IMPORTED)
set_target_properties(yaml-cpp PROPERTIES
  IMPORTED_LOCATION ${YAML_CPP_ROOT}/build/libyaml-cpp.a
  INTERFACE_INCLUDE_DIRECTORIES ${YAML_CPP_ROOT}/include
)

# find nghttp2
pkg_check_modules(NGHTTP2 REQUIRED libnghttp2)

# add hdrhistogram
add_subdirectory(external/HdrHistogram_c)

# add NanoLog
include_directories(include)
include_directories(SYSTEM external/NanoLog/runtime)

file(GLOB_RECURSE SOURCES src/*.cc src/*.c)

add_executable(sidecar ${SOURCES})
if(SIDECAR_ENABLE_NANOLOG)
  target_compile_definitions(sidecar PRIVATE NANO_LOG_ENABLED)
endif()
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

# unit tests
enable_testing()
include(GoogleTest)
add_executable(flexible_priority_queue_test tests/flexible_priority_queue_test.cc)
target_link_libraries(flexible_priority_queue_test GTest::gtest_main glog::glog)
gtest_discover_tests(flexible_priority_queue_test)