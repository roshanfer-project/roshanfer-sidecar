cmake_minimum_required(VERSION 3.15)
project(sidecar LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# generate compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# find glog
find_package (glog 0.7.1 REQUIRED)

# find liburing
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)

include_directories(include)

file(GLOB_RECURSE SOURCES src/*.cc)

add_executable(sidecar ${SOURCES})
# link glog
target_link_libraries (sidecar glog::glog)
# link liburing
target_link_libraries(sidecar ${LIBURING_LIBRARIES})