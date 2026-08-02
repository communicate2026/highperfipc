cmake_minimum_required(VERSION 3.20)
project(IPCLibrary 
    VERSION 1.0.0
    DESCRIPTION "High-performance IPC library for Linux using Unix Domain Sockets"
    LANGUAGES CXX
)

# C++20 Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags for production
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG -fsanitize=address,undefined")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=address,undefined")

# Build type default to Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Linux x64 only
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "This library only supports Linux")
endif()

if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64")
    message(WARNING "This library is optimized for x86_64 architecture")
endif()

# Options
option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build examples" ON)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(ENABLE_SANITIZERS "Enable sanitizers in debug mode" ON)

# Find pthread
find_package(Threads REQUIRED)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party/concurrentqueue
)

# Source files
set(IPC_SOURCES
    src/common/Message.cpp
    src/common/Utils.cpp
    src/server/IPCServer.cpp
    src/client/IPCClient.cpp
)

# Header files
set(IPC_HEADERS
    include/ipc/common/Message.h
    include/ipc/common/Types.h
    include/ipc/common/Utils.h
    include/ipc/common/Constants.h
    include/ipc/server/IPCServer.h
    include/ipc/client/IPCClient.h
    include/ipc/IPC.h
)

# Create library
add_library(ipc STATIC ${IPC_SOURCES} ${IPC_HEADERS})

target_include_directories(ipc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(ipc PUBLIC
    Threads::Threads
    rt
)

# Set compile options
target_compile_options(ipc PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    -fPIC
)

# Install targets
install(TARGETS ipc
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include)

# Tests
if(BUILD_TESTS)
    enable_testing()
    
    add_executable(ipc_tests
        tests/test_server.cpp
        tests/test_client.cpp
        tests/test_integration.cpp
    )
    
    target_link_libraries(ipc_tests PRIVATE ipc)
    
    add_test(NAME IPCTests COMMAND ipc_tests)
endif()

# Examples
if(BUILD_EXAMPLES)
    add_executable(server_example examples/server_example.cpp)
    target_link_libraries(server_example PRIVATE ipc)
    
    add_executable(client_example examples/client_example.cpp)
    target_link_libraries(client_example PRIVATE ipc)
    
    add_executable(benchmark examples/benchmark.cpp)
    target_link_libraries(benchmark PRIVATE ipc)
endif()

# Documentation
find_package(Doxygen QUIET)
if(DOXYGEN_FOUND)
    set(DOXYGEN_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/docs)
    doxygen_add_docs(docs
        ${CMAKE_SOURCE_DIR}/include
        COMMENT "Generate documentation"
    )
endif()

# Print configuration summary
message(STATUS "========================================")
message(STATUS "IPC Library Configuration Summary")
message(STATUS "========================================")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Build tests: ${BUILD_TESTS}")
message(STATUS "Build examples: ${BUILD_EXAMPLES}")
message(STATUS "Install prefix: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "========================================")
