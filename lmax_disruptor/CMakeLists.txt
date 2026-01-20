cmake_minimum_required(VERSION 3.14)
project(lmax_disruptor VERSION 2.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler flags for performance
if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -DNDEBUG")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
endif()

# Include directories
include_directories(include)
include_directories(..)  # For SPSCQueue.h comparison

# Header-only library (interface target)
add_library(lmax_disruptor INTERFACE)
target_include_directories(lmax_disruptor INTERFACE include)

# SPSC Test
add_executable(spsc_test test/spsc_test.cc)
target_link_libraries(spsc_test PRIVATE lmax_disruptor pthread)

# Pipeline Test
add_executable(pipeline_test test/pipeline_test.cc)
target_link_libraries(pipeline_test PRIVATE lmax_disruptor pthread)

# Multi-Producer Test
add_executable(multi_producer_test test/multi_producer_test.cc)
target_link_libraries(multi_producer_test PRIVATE lmax_disruptor pthread)

# Processor Test (BatchEventProcessor, Pipeline)
add_executable(processor_test test/processor_test.cc)
target_link_libraries(processor_test PRIVATE lmax_disruptor pthread)

# Benchmark (compares with SPSCQueue)
add_executable(benchmark test/benchmark.cc)
target_link_libraries(benchmark PRIVATE lmax_disruptor pthread)
# Apply optimizations specifically to this target
target_compile_options(benchmark PRIVATE
    -O3
    -march=native
    -ffast-math
    -flto
)

# MPMC Benchmark (multi-producer, pipeline, batch processor)
add_executable(mpmc_benchmark test/mpmc_benchmark.cc)
target_link_libraries(mpmc_benchmark PRIVATE lmax_disruptor pthread)
target_compile_options(mpmc_benchmark PRIVATE
    -O3
    -march=native
    -ffast-math
    -flto
)

# Enable testing
enable_testing()
add_test(NAME spsc_test COMMAND spsc_test)
add_test(NAME pipeline_test COMMAND pipeline_test)
add_test(NAME multi_producer_test COMMAND multi_producer_test)
add_test(NAME processor_test COMMAND processor_test)

# Custom target to run all tests
add_custom_target(run_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS spsc_test pipeline_test multi_producer_test processor_test
)

# Custom target for benchmark
add_custom_target(run_benchmark
    COMMAND benchmark
    DEPENDS benchmark
)

# Custom target for MPMC benchmark
add_custom_target(run_mpmc_benchmark
    COMMAND mpmc_benchmark
    DEPENDS mpmc_benchmark
)
