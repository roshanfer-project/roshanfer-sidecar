cmake_minimum_required(VERSION 3.15)
project(sidecar LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# generate compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer")
# options: "-fsanitize=address": to see memory leaks, stack corruption, etc.

# append -Werror=format to CMAKE_CXX_FLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=format")

# find glog
find_package (glog 0.7.1 REQUIRED)

# find liburing
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

# include yaml-cpp
include(FetchContent)

FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
  GIT_TAG 0.8.0
)
FetchContent_MakeAvailable(yaml-cpp)

# find nghttp2
pkg_check_modules(NGHTTP2 REQUIRED libnghttp2)

include_directories(include NanoLog/runtime)

file(GLOB_RECURSE SOURCES src/*.cc src/*.c)

add_executable(sidecar ${SOURCES})
# link glog
target_link_libraries (sidecar glog::glog)
# link liburing
target_link_libraries(sidecar ${LIBURING_LIBRARIES})
# link yaml-cpp
target_link_libraries(sidecar yaml-cpp::yaml-cpp)
# link nghttp2
target_link_libraries(sidecar ${NGHTTP2_LIBRARIES})
# link nanolog
target_link_libraries(sidecar ${CMAKE_CURRENT_SOURCE_DIR}/NanoLog/runtime/libNanoLog.a)
# link pthread
target_link_libraries(sidecar pthread)
# link rt
target_link_libraries(sidecar rt)