
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.21.0)
include(CMakeDependentOption)

set(CMAKE_VERBOSE_MAKEFILE OFF)
set(VCPKG_BINARY_VERSION v27)
# Activate self hosting, such that the compiler from the dependencies is taken
option(NES_SELF_HOSTING "use self hosting compiler" ON)
# Enable native mtune/march for optimizations
option(NES_BUILD_NATIVE "Override mtune/march to load native support" OFF)
# Enable local dependency building (this may take a long time)
option(NES_BUILD_DEPENDENCIES_LOCAL "Build all dependencies locally" OFF)
# Enable prebuild dependencies only if NES_BUILD_DEPENDENCIES_LOCAL is OFF and CMAKE_TOOLCHAIN_FILE is not set.
cmake_dependent_option(NES_USE_PREBUILD_DEPENDENCIES "Use pre-build dependencies" ON "NOT NES_BUILD_NATIVE; NOT CMAKE_TOOLCHAIN_FILE" OFF)

#toggle import and usage of s2 library
option(NES_USE_S2 "Toggle use of s2 library" OFF)
option(NES_USE_TF "Toggle tensorflow library" OFF)
option(CMAKE_EXPORT_COMPILE_COMMANDS "Write JSON compile commands database" ON)
option(CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS "Build using tuple buffer leak detector" OFF)
option(CMAKE_USE_ONE_QUEUE_PER_NUMA_NODE "Build using NUMA optimization" OFF)
option(CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE "Build using folly and lock-free MPMC queue" ON)
option(CMAKE_USE_LATCH_FREE_BUFFER_MANAGER "Build using folly and lock-free MPMC queue for the buffer manager" ON)
option(CMAKE_USE_PAPI_PROFILER "Build using PAPI Profiler" OFF)
option(CMAKE_NES_TRACE_NODE_CREATION "Debug flag such that we track the creation of specific operator nodes" OFF)
option(ENABLE_IWYU "Enable include-what-you-use suggestions (if found on the system)" OFF)
option(ENABLE_CLANG_TIDY OFF)
option(NES_COMPUTE_COVERAGE "Compute test coverage" OFF)
option(NES_ENABLES_TESTS "Compute test coverage" ON)
option(NES_USE_LLD "Use LLD" OFF)
option(NES_CODE_COVERAGE "Enable Code Coverage" OFF)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_ENGINE "Enables the experimental execution engine (Nautilus)." ON)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR "Enables the MLIR backend." ON)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_FLOUNDER "Build FLOUNDER Backend" OFF)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_JNI "Build JNI Backend" OFF)
option(NES_ENABLE_EXPERIMENTAL_EXECUTION_BABELFISH "Build BABELFISH Backend" OFF)
option(NES_BUILD_BENCHMARKS "Build benchmarks" ON)
# TODO #3351 Do not enable Kafka by default
option(NES_USE_KAFKA "Build with Kafka support" ON)

# Enable exception interception (for debugging purpose)
option(NES_ENABLE_CXA_THROW_HOOK "Enable exception interception" OFF)

set(NES_TEST_PARALLELISM 1 CACHE STRING "Set test parallelism")
set(NES_LOG_LEVEL "TRACE" CACHE STRING "LogLevel")

if (NOT NES_TEST_PARALLELISM MATCHES "^[0-9]+$")
    set(NES_TEST_PARALLELISM "1")
endif ()

if (NOT CMAKE_USE_LATCH_FREE_BUFFER_MANAGER)
    message(FATAL_ERROR "Disabling CMAKE_USE_LATCH_FREE_BUFFER_MANAGER is not supported for now, please enable it")
endif ()

if (NOT CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
    message(FATAL_ERROR "Disabling CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE is not supported for now, please enable it")
endif ()

# for macOS or iOS, watchOS, tvOS (since 3.10.3), and other UNIXes
# The system architecture is normally set in CMAKE_HOST_PROCESSOR,
# which is set by the PROJECT command. However, we cannot call PROJECT
# at this point because we want to use a custom toolchain file.
execute_process(COMMAND uname -m OUTPUT_VARIABLE NES_HOST_PROCESSOR)


set(CMAKE_THREAD_LIBS_INIT "-lpthread")
include(cmake/ImportDependencies.cmake)
project(NES CXX)
set(VCPKG_POLICY_ALLOW_RESTRICTED_HEADERS enabled)
message(STATUS "Going to use ${CMAKE_CXX_COMPILER}")

# Find project version
include(cmake/semver/GetSemanticVersion.cmake)

# Custom CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set NES Flags
set(NES_SPECIFIC_FLAGS)
if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "-DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif ()

if (CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif ()

if (NES_ENABLE_CXA_THROW_HOOK)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_ENABLE_CXA_THROW_HOOK=1")
endif ()

# this is an experimental feature; do not rely on it yet
#if(NES_COMPUTE_COVERAGE)
#    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
#endif

#if the flag is set we create one queue per query
if (NES_USE_ONE_QUEUE_PER_QUERY)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_ONE_QUEUE_PER_QUERY=1")
    if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
        message(STATUS "use NES_USE_ONE_QUEUE_PER_QUERY and unset CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE")
        message(STATUS "use CMAKE_USE_LATCH_FREE_BUFFER_MANAGER")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_LATCH_FREE_BUFFER_MANAGER=1")
    endif ()
endif ()

if (NES_USE_ONE_QUEUE_PER_QUERY)
    set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_ONE_QUEUE_PER_QUERY=1")
    if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
        message(STATUS "use NES_USE_ONE_QUEUE_PER_QUERY and unset CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE")
        message(STATUS "use CMAKE_USE_LATCH_FREE_BUFFER_MANAGER")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_LATCH_FREE_BUFFER_MANAGER=1")
    endif ()
endif ()

if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
    if (NOT NES_USE_ONE_QUEUE_PER_QUERY)
        message(STATUS "use CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE")
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_MPMC_BLOCKING_CONCURRENT_QUEUE=1")
        if (CMAKE_USE_LATCH_FREE_BUFFER_MANAGER)
            message(STATUS "use CMAKE_USE_LATCH_FREE_BUFFER_MANAGER")
            set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_LATCH_FREE_BUFFER_MANAGER=1")
        endif ()
    endif ()
endif ()

if (CMAKE_USE_ONE_QUEUE_PER_NUMA_NODE)
    message(STATUS "ENABLE CMAKE_USE_ONE_QUEUE_PER_NUMA_NODE")
    if (NOT NES_USE_ONE_QUEUE_PER_QUERY)
        set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_ONE_QUEUE_PER_NUMA_NODE=1")
        if (CMAKE_USE_LATCH_FREE_BUFFER_MANAGER)
            set(NES_SPECIFIC_FLAGS "${NES_SPECIFIC_FLAGS} -DNES_USE_LATCH_FREE_BUFFER_MANAGER=1")
        endif ()
    endif ()

    # Numa awareness
    find_package(NUMA)
    if (NUMA_FOUND)
        set(LIBRARIES ${LIBRARIES} -lnuma)
        message(STATUS "Using Numa")
    endif ()
    find_package(folly CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} Folly::folly Folly::folly_deps)
    include_directories(${FOLLY_INCLUDE_DIR})
endif ()

# enables tracing of stack traces if operator / expression nodes are created
if (CMAKE_NES_TRACE_NODE_CREATION)
    add_compile_definitions(NES_TRACE_NODE_CREATION)
    message(STATUS "Trace node creation")
endif ()

# Infers the log level based on the provided "NES_LOG_LEVEL" flag.
# Currently we support the following log levels:
# TRACE, DEBUG, INFO, WARN, ERROR, FATAL_ERROR, and NONE.
# Notice that these is a compilation time provided value.
# Consequently, it is not possible to activate at runtime a log level,
# which is lower then the log level that is provided here.
get_nes_log_level_value(NES_LOGGING_VALUE)

# Set Optimization Flags
set(NES_WARNINGS "-Wall -Wbitwise-instead-of-logical -Wextra -pedantic -Wno-null-character -Wno-dollar-in-identifier-extension -Werror=extra -Werror=exceptions -Werror=all -Werror=integer-overflow -Werror=return-type -Werror=return-stack-address -Werror=delete-non-virtual-dtor -Werror=deprecated -Werror=writable-strings -Werror=array-bounds -Werror=ignored-qualifiers -Werror=sign-compare -Wno-deprecated-copy-with-dtor -Wno-unused-variable -Wno-unused-but-set-variable -Wno-deprecated-declarations ")
set(CMAKE_CXX_FLAGS "${NES_WARNINGS} -fno-omit-frame-pointer -fstandalone-debug -fdebug-default-version=4 -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNES_DEBUG_MODE=1")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -fno-omit-frame-pointer -DNES_RELEASE_MODE=1")

if (NOT APPLE)
    if (NES_USE_LLD)
        message(STATUS "we use LLD for linking")
        add_link_options("-fuse-ld=lld")
    endif ()
    # as per backward.h
    add_link_options("-ldw")
endif ()

include(cmake/Sanitizers.cmake)

if (ENABLE_IWYU)
    find_program(iwyu_path NAMES include-what-you-use iwyu)
    if (iwyu_path)
        message(STATUS "IWYU enabled and found! Suggestions will be printed to stderr.")
    else ()
        message(WARNING "IWYU is enabled but not found! You will not get any suggestions")
    endif ()
endif ()

if (ENABLE_CLANG_TIDY)
    message(STATUS "clang tidy enabled")
    set(CMAKE_CXX_CLANG_TIDY clang-tidy)
else ()
    message(STATUS "clang tidy not enabled")
endif ()

if (NES_BUILD_BENCHMARKS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNES_BENCHMARKS_FLAG_MODE=1")
    message("Build with all flags on, only use for benchmarking")
    if (NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -DNES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT=1")
        message("use latency measurement")
    endif ()
    #-fno-omit-frame-pointer
endif ()

# Add Python UDF support
option(ENABLE_PYTHON_UDF "Build supporting Python UDF" OFF)
message(STATUS "Python UDF support: ${ENABLE_PYTHON_UDF}")
if (ENABLE_PYTHON_UDF)
    add_compile_definitions(PYTHON_UDF_ENABLED)
    find_package(PythonLibs 3 REQUIRED)
    include_directories(${PYTHON_INCLUDE_DIRS})
    # Python needs to link against libutil. "python3-config --ldflags" will give the recommended flags
    set(LIBRARIES ${LIBRARIES} ${PYTHON_LIBRARIES} -lutil)
endif ()

# Native/Generic march support
if (NES_BUILD_NATIVE)
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
    if (COMPILER_SUPPORTS_MARCH_NATIVE)
        message(STATUS "CMAKE detects native arch support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mtune=native -march=native -DNES_BENCHMARKS_NATIVE_MODE=1")
        # AVX detection
        find_package(AVX)
        if (${AVX2_FOUND})
            message(STATUS "CMAKE detects AVX2 support")
            add_compile_definitions(HAS_AVX)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
        endif ()
    else ()
        message(FATAL_ERROR "NES_BUILD_NATIVE was true but the compiler does not support it on this architecture.")
    endif ()
else ()
    # Compiler should produce specific code for system architecture
    MESSAGE("CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
        message(STATUS "CMAKE detects generic x86_64 arch support")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=x86-64 -mtune=generic")
    elseif (APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
        message(STATUS "CMAKE detects APPLE ARM64 support")
        # Clang has the exact same macros for A14 as M1.
        # Current Clang master has -mcpu=apple-m1 but
        # not yet on latest release. Overall, arm64/aarch64
        # offer so many different configuration parameters
        # that compilers prefer -mcpu=XXX instead.
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=apple-a14")
    elseif (NOT APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
        message(STATUS "CMAKE detects generic ARM64 support")
        # Arm themselves suggest using -mcpu=native, or in general,
        # to use -mcpu=CPU_TYPE. For more info, here:
        # https://community.arm.com/developer/tools-software/tools/b/tools-software-ides-blog/posts/compiler-flags-across-architectures-march-mtune-and-mcpu
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=native")
    else ()
        message(FATAL_ERROR "CMAKE_SYSTEM_PROCESSOR was ${CMAKE_SYSTEM_PROCESSOR} this is a unsupported architecture.")
    endif ()
endif ()


message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")

# folly and cpprestsdk relies on boost so we set the right configuration flag
set(Boost_NO_WARN_NEW_VERSIONS TRUE)

# spdlog
find_package(spdlog REQUIRED)
set(LIBRARIES ${LIBRARIES} spdlog::spdlog)

# jemalloc
option(CMAKE_ENABLE_JEMALLOC "Build using jemalloc" OFF)
message(STATUS "Using jemalloc: ${CMAKE_ENABLE_JEMALLOC}")
if (CMAKE_ENABLE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC jemalloc)
    pkg_search_module(JEMALLOC REQUIRED jemalloc)
    include_directories(${JEMALLOC_INCLUDE_DIRS})
    set(LIBRARIES ${LIBRARIES} ${JEMALLOC_LIBRARIES})
endif ()
# lcryto is required for asio to communicate
if (APPLE)
    find_library(SSL_LIB libssl.a REQUIRED)
    find_library(CRYPTO_LIB libcrypto.a REQUIRED)
    set(LIBRARIES ${LIBRARIES} -ldl -liconv ${CRYPTO_LIB} ${SSL_LIB})
endif ()
if (UNIX AND NOT APPLE)
    find_package(OpenSSL REQUIRED)
    set(LIBRARIES ${LIBRARIES} OpenSSL::SSL OpenSSL::Crypto)
endif ()

# GRPC and protobuf
set(gRPC_RELEASE "v1.28.1")
# This assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
if (CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else ()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif ()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if (CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else ()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif ()
set(GRPC_LIBRARIES ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})
set(LIBRARIES ${LIBRARIES} ${GRPC_LIBRARIES})


# LibZMQ
message(STATUS "Libzmq")
find_package(ZeroMQ CONFIG REQUIRED)
if (NOT ${ZeroMQ_FOUND})
    message(FATAL_ERROR "Unable to find libzmq")
endif ()
set(LIBRARIES ${LIBRARIES} libzmq libzmq-static)

# CppZMQ
message(STATUS "cppzmq")
find_package(cppzmq CONFIG REQUIRED)
if (NOT ${CPPZMQ_FOUND})
    message(FATAL_ERROR "Unable to find cppzmq")
endif ()
set(LIBRARIES ${LIBRARIES} cppzmq cppzmq-static)

# Z3 Solver
message(STATUS "Z3")
find_package(Z3 REQUIRED)
if (NOT ${Z3_FOUND})
    message(FATAL_ERROR "Unable to find Z3")
endif ()
set(LIBRARIES ${LIBRARIES} ${Z3_LIBRARIES})
message(STATUS "Using z3: " ${Z3_LIBRARIES})

# Backward
include(cmake/BackwardConfig.cmake)
include_directories(${BACKWARD_INCLUDE_DIRS})
add_compile_definitions(${BACKWARD_DEFINITIONS})
link_directories(${BACKWARD_LIBRARIES})
set(LIBRARIES ${LIBRARIES} ${BACKWARD_LIBRARIES})

# folly
if (CMAKE_USE_MPMC_BLOCKING_CONCURRENT_QUEUE)
    include(CMakeFindDependencyMacro)
    find_dependency(gflags CONFIG REQUIRED) # required on macos
    find_package(folly CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} Folly::folly Folly::folly_deps)
    include_directories(${FOLLY_INCLUDE_DIR})
endif ()

# Library containing dlopen and dlcose.
set(LIBRARIES ${LIBRARIES} ${CMAKE_DL_LIBS})

#Oatpp
find_package(oatpp REQUIRED)
set(LIBRARIES ${LIBRARIES} oatpp::oatpp oatpp::oatpp-test)

# CPR
find_package(cpr REQUIRED)
if (cpr_FOUND)
    find_package(CURL REQUIRED)
    message(STATUS "Found CURL : ${CURL_LIBRARIES}")
    message(STATUS "Found cpr: ${cpr_CONFIG} (found version ${cpr_VERSION})")
endif ()

find_package(nlohmann_json REQUIRED)
set(LIBRARIES ${LIBRARIES} nlohmann_json::nlohmann_json)

# Eigen3 linear alg library
find_package(Eigen3 CONFIG REQUIRED NO_MODULE)
set(LIBRARIES ${LIBRARIES} Eigen3::Eigen)

# s2 Library
if (NES_USE_S2)
    message(STATUS "s2 is enabled")
    find_package(s2 REQUIRED)
    if (NOT s2_FOUND)
        message(FATAL_ERROR "Unable to find s2 library")
    endif ()
    set(LIBRARIES ${LIBRARIES} s2::s2)
    add_compile_definitions(S2DEF)
else ()
    message(STATUS "s2 is disabled")
endif ()

# Library containing Kafka client
if (NES_USE_KAFKA)
    # TODO #3351 The commands should be moved into a separate CMake config file.
    if (NOT APPLE)
        include_directories(${NES_DEPENDENCIES_BINARY_ROOT}/include/cppkafka)
        set(LIBRARIES ${LIBRARIES} ${NES_DEPENDENCIES_BINARY_ROOT}/lib/libcppkafka.so.0.3.1)
        add_compile_definitions(ENABLE_KAFKA_BUILD)
        add_library(cppkafka::cppkafka SHARED IMPORTED)
        set_target_properties(cppkafka::cppkafka
                PROPERTIES
                IMPORTED_LOCATION ${NES_DEPENDENCIES_BINARY_ROOT}/lib/libcppkafka.so.0.3.1
                INTERFACE_INCLUDE_DIRECTORIES "${NES_DEPENDENCIES_BINARY_ROOT}/include/cppkafka"
                )
        install(IMPORTED_RUNTIME_ARTIFACTS cppkafka::cppkafka DESTINATION lib)
        message(STATUS "Enabled ENABLE_KAFKA_BUILD flag")
    endif ()
endif ()

# Library containing OPC client
if (NES_USE_OPC)
    find_package(open62541 CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} open62541::open62541)
    add_compile_definitions(ENABLE_OPC_BUILD)
endif (NES_USE_OPC)

# Library containing MQTT client
if (NES_USE_MQTT)
    message("VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
    find_package(PahoMqttCpp CONFIG REQUIRED)
    set(LIBRARIES ${LIBRARIES} PahoMqttCpp::paho-mqttpp3-static)
    add_compile_definitions(ENABLE_MQTT_BUILD)
endif (NES_USE_MQTT)

if (NES_USE_GPU)
    find_package(CUDAToolkit REQUIRED)
    enable_language(CUDA)
    if (NOT ${CUDAToolkit_FOUND})
        message(FATAL_ERROR "Unable to find CUDAToolkit_FOUND")
    endif ()
    set(CMAKE_CUDA_STANDARD 17)
    set(LIBRARIES ${LIBRARIES} CUDA::cudart CUDA::nvrtc)
    message("CUDA dependencies OK")
endif (NES_USE_GPU)

if (NES_ENABLES_TESTS)
    find_package(GTest CONFIG REQUIRED)
    if (NOT ${GTest_FOUND})
        message(FATAL_ERROR "Unable to find GTest")
    endif ()
    set(GTEST_LIBRARIES ${GTEST_LIBRARIES} GTest::gtest GTest::gtest_main GTest::gmock GTest::gmock_main)
endif ()

# Antlr
# Currently antlr dose not come with an own cmake file
message(STATUS Antlr ${NES_DEPENDENCIES_BINARY_ROOT})
include_directories(${NES_DEPENDENCIES_BINARY_ROOT}/include/antlr4-runtime)
if (NOT APPLE)
    set(LIBRARIES ${LIBRARIES} ${NES_DEPENDENCIES_BINARY_ROOT}/lib/libuuid.a)
endif ()
set(LIBRARIES ${LIBRARIES} ${NES_DEPENDENCIES_BINARY_ROOT}/lib/libantlr4-runtime.a)

# PAPI
if (CMAKE_USE_PAPI_PROFILER)
    find_path(PAPI_PREFIX
            NAMES include/papi.h
            )

    find_library(PAPI_LIBRARIES
            # Pick the static library first for easier run-time linking.
            NAMES libpapi.so libpapi.a papi
            HINTS ${PAPI_PREFIX}/lib
            )

    find_path(PAPI_INCLUDE_DIRS
            NAMES papi.h
            HINTS ${PAPI_PREFIX}/include
            )

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(PAPI DEFAULT_MSG
            PAPI_LIBRARIES
            PAPI_INCLUDE_DIRS
            )

    mark_as_advanced(
            PAPI_PREFIX_DIRS
            PAPI_LIBRARIES
            PAPI_INCLUDE_DIRS
    )
    message(STATUS "Papi found at ${PAPI_LIBRARIES}")
    message(STATUS "Papi found at ${PAPI_INCLUDE_DIRS}")
    set(LIBRARIES ${LIBRARIES} ${PAPI_LIBRARIES})
    add_compile_definitions(ENABLE_PAPI_PROFILER)
    include_directories(${PAPI_INCLUDE_DIRS})
endif ()

# Print all used include directories
message(STATUS "INCLUDE_DIRS: ${INCLUDE_DIRECTORIES}")
set(PATH_TO_NES_SOURCE_CODE ${PROJECT_SOURCE_DIR})
get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach (dir ${dirs})
    message(STATUS "  - ${dir}")
endforeach ()

message(STATUS " Libraries: ${LIBRARIES}")
message(STATUS " GRPC Libraries: ${GRPC_LIBRARIES}")

# Definition of runtime variables ######################################################################################
# LLVM
find_package(LLVM 14 CONFIG REQUIRED)
if (LLVM_FOUND)
    message("-- LLVM version ${LLVM_PACKAGE_VERSION}")
    message("-- LLVM binary dir ${LLVM_BINARY_DIR}")
    message("-- LLVM tool dir ${LLVM_TOOLS_INSTALL_DIR}")
    set(CLANG_EXECUTABLE ${LLVM_BINARY_DIR}/${LLVM_TOOLS_INSTALL_DIR}/clang-14)
    set(CLANG_INCLUDE_DIR ${LLVM_BINARY_DIR}/${LLVM_TOOLS_INSTALL_DIR}/lib/clang/${LLVM_PACKAGE_VERSION}/include/)
    set(CLANG_FORMAT_EXECUTABLE ${LLVM_BINARY_DIR}/${LLVM_TOOLS_INSTALL_DIR}/clang-format)
else ()
    message(FATAL_ERROR "Cannot find llvm")
endif ()
message("-- LLVM version ${LLVM_VERSION_STRING}")
message("-- LLVM version ${LLVM_ROOT_DIR}")

# Set definition values.
set(GENERATED_CODE_DIRECTORY ${PROJECT_BINARY_DIR}/generated-code)
set(PATH_TO_NES_SOURCE_CODE ${PROJECT_SOURCE_DIR})
set(PATH_TO_DEB_SOURCE_CODE ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET})
set(PATH_TO_BINARY_DIR ${CMAKE_BINARY_DIR})
message("PATH_TO_DEB_SOURCE_CODE Dir: ${PATH_TO_DEB_SOURCE_CODE}")

# Print definition values.
message(STATUS "Clang Executable: ${CLANG_EXECUTABLE}")
message(STATUS "Clang-Format Executable: ${CLANG_FORMAT_EXECUTABLE}")
message(STATUS "Generated Code Directory: ${GENERATED_CODE_DIRECTORY}")

# Add definitions as compile flag.
add_definitions(-D 'CLANG_EXECUTABLE=\"${CLANG_EXECUTABLE}\"')
add_definitions(-D 'CLANG_FORMAT_EXECUTABLE=\"${CLANG_FORMAT_EXECUTABLE}\"')
add_definitions(-D 'GENERATED_CODE_DIRECTORY=\"${GENERATED_CODE_DIRECTORY}\"')
add_definitions(-D 'PATH_TO_NES_SOURCE_CODE=\"${PATH_TO_NES_SOURCE_CODE}\"')
add_definitions(-D 'PATH_TO_DEB_SOURCE_CODE=\"${PATH_TO_DEB_SOURCE_CODE}\"')
add_definitions(-D 'PATH_TO_BINARY_DIR=\"${PATH_TO_BINARY_DIR}\"')
if (APPLE)
    add_definitions(-D 'NES_OSX_SYSROOT=\"${CMAKE_OSX_SYSROOT}\"')
endif ()

add_definitions(-D 'QUERY_COMPILATION_CC=\"${QUERY_COMPILATION_CC}\"')
add_definitions(-D 'CLANG_INCLUDE_DIRS=\"${CLANG_INCLUDE_DIRS}\"')

# fix compile error with llvm (https://stackoverflow.com/a/18966387)
add_definitions(-D '__STDC_CONSTANT_MACROS')
add_definitions(-D '__STDC_LIMIT_MACROS')


# Check if we build with TensorFlow Lite
if (NES_USE_TF)
    find_package(tensorflow-lite-c REQUIRED)
    if (tensorflowlite_c_FOUND)
        message(STATUS "Found tensorflow")
        set(LIBRARIES ${LIBRARIES} tensorflowlite_c::tensorflowlite_c)
        message(INFO "${LIBRARIES}")
    endif ()

    # Include tensorflow library in the deb package and install it in the lib folder
    install(IMPORTED_RUNTIME_ARTIFACTS tensorflowlite_c::tensorflowlite_c DESTINATION lib)
    add_compile_definitions(TFDEF)
endif ()

# $ORIGIN is to read the shared object from the installation directory
if (ORIGIN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath='$ORIGIN'")
endif ()
#https://cmake.org/cmake/help/latest/variable/CMAKE_INSTALL_RPATH_USE_LINK_PATH.html
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH "true")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")

###########################################################################################

# Cross compilation
if (CMAKE_CROSSCOMPILING)
    set(QEMU_EMULATOR_PATH /usr/bin/qemu-aarch64)
    message(STATUS "Current Qemu Executable: ${QEMU_EMULATOR_PATH}")
    add_definitions(-D 'QEMU_EMULATOR_PATH=\"${QEMU_EMULATOR_PATH}\"')
endif ()

###########################################################################################

# Add tests with command
include(cmake/NebulaStreamTest.cmake)
if (NES_ENABLES_TESTS)
    set(GTEST_LIBRARIES ${GTEST_LIBRARIES} ${LIBRARIES} cpr::cpr ${CURL_LIBRARIES})
    # Add tests with command
    enable_testing()
    message(STATUS "Tests are enabled")
    if (NES_CODE_COVERAGE)
        set(CODE_COVERAGE ON)
        include(cmake/CodeCoverage.cmake)
        enable_extra_test_features()
        message(STATUS "Tests are enabled with code coverage")
    endif ()
else ()
    message(STATUS "Tests are disabled")
endif ()

# code cov

# Add target for common lib, which contains a minimal set
# of shared functionality used by all components of nes
add_subdirectory(nes-common)
# Add target for all data types
add_subdirectory(nes-data-types)
# Add target for the compiler
add_subdirectory(nes-compiler)
# Add target for the runtime
add_subdirectory(nes-runtime)
# Add target for core, which contains the nes lib
add_subdirectory(nes-core)
# Add target for client
add_subdirectory(nes-client)

# Add targets for benchmarks
if (NES_BUILD_BENCHMARKS)
    add_subdirectory(nes-benchmark)
endif (NES_BUILD_BENCHMARKS)


# Documentation
add_subdirectory(cmake/docs)

# if the include what you use path is set configure it for the nes target.
if (iwyu_path)
    set_property(TARGET nes PROPERTY CXX_INCLUDE_WHAT_YOU_USE ${iwyu_path})
endif ()


# Custom test commands --------------------------------------------------------
message(STATUS "Adding test_debug target with parallelism=${NES_TEST_PARALLELISM}")
add_custom_target(test_debug COMMAND ${CMAKE_CTEST_COMMAND} -j${NES_TEST_PARALLELISM} --output-on-failure DEPENDS ${UNITTESTS})

message(STATUS "INSTALL NES CLANG ${CLANG_EXECUTABLE}")
install(PROGRAMS ${CLANG_EXECUTABLE} TYPE BIN COMPONENT nesLibrary RENAME nes-clang)
install(SCRIPT "${CMAKE_SOURCE_DIR}/cmake/flatten.cmake")

include(CMakePackageConfigHelpers)
# Create the Config.cmake files, which enables find_package(NebulaStream) from external CMAKE projects.
configure_package_config_file(cmake/NebulaStreamConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaStreamConfig.cmake
        INSTALL_DESTINATION ${LIB_INSTALL_DIR}/NebulaStream/cmake)

# Create Targets.cmake file, which exposes NebulaStream targets to external CMAKE projects
export(EXPORT NebulaStreamTargets
        FILE "${CMAKE_CURRENT_BINARY_DIR}/NebulaStreamTargets.cmake"
        NAMESPACE NebulaStream::)

# Create version_file to enable specific version imports
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/NebulaStreamConfigVersion.cmake
        VERSION "${${PROJECT_NAME}_VERSION_MAJOR}.${${PROJECT_NAME}_VERSION_MINOR}.${${PROJECT_NAME}_VERSION_PATCH}"
        COMPATIBILITY AnyNewerVersion)

# Install NebulaStreamConfig, NebulaStreamConfigVersion, and NebulaStreamTargets to exported package
set(ConfigPackageLocation lib/NebulaStream/cmake)
install(EXPORT NebulaStreamTargets
        FILE
        NebulaStreamTargets.cmake
        NAMESPACE
        NebulaStream::
        EXPORT_LINK_INTERFACE_LIBRARIES
        DESTINATION
        ${ConfigPackageLocation}
        )
install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaStreamConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/NebulaStreamConfigVersion.cmake
        DESTINATION ${LIB_INSTALL_DIR} ${ConfigPackageLocation})

# Configure CPackage
SET(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_VERSION ${${PROJECT_NAME}_VERSION})
set(CPACK_PACKAGE_NAME "NebulaStream")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_STRIP_FILES TRUE)
set(CPACK_PACKAGE_CONTACT "nebulastream@dima.tu-berlin.de")
set(CPACK_PACKAGE_DESCRIPTION "NebulaStream is a general purpose, end-to-end data management system for the IoT. It provides an out-of-the box experience with rich data processing functionalities and a high ease-of-use.")
set(CPACK_PACKAGE_VENDOR "Team NebulaStream")
set(CPACK_PACKAGE_HOMEPAGE_URL "http://nebula.stream")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_COMPONENTS_ALL nesExecutables nesLibrary nesHeaders)
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libdwarf-dev, libdwarf1, binutils-dev, libdw-dev")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}-${CMAKE_SYSTEM_NAME}.${CMAKE_SYSTEM_PROCESSOR}")

##This allows call to ldconfig to load the configuration after installation
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_LIST_DIR}/triggers")
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION TRUE)
INCLUDE(CPack)

# macros for guard once
include(cmake/GuardOnce.cmake)

#Other configurations
project_enable_clang_format()
project_enable_fixguards()
if (CMAKE_CROSSCOMPILING)
    project_enable_emulated_tests()
endif ()
project_enable_release()
project_enable_version()
instrument_codebase()