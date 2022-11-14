cmake_minimum_required(VERSION 3.0.2)
SET(CMAKE_CXX_COMPILER "/usr/bin/g++-11")
project(MyIndex)
set(CMAKE_CXX_FLAGS "-O3 -DMKL_ILP64 -march=native")
set(CMAKE_CXX_STANDARD 20)

include_directories(
  ${catkin_INCLUDE_DIRS}
  /opt/intel/oneapi/mkl/latest/include
  "./include"
)

link_directories(
  ${catkin_LIB_DIRS}
  /opt/intel/oneapi/mkl/latest/lib/intel64
 )

AUX_SOURCE_DIRECTORY ("./src" src)
AUX_SOURCE_DIRECTORY ("./test" test)

set(libs "-lmkl_intel_ilp64 -lmkl_sequential -lmkl_core -lpthread -lm -ldl -libverbs -lpthread" )

add_executable(bench test/bench.cc ${src})
target_link_libraries(bench ${libs})

add_executable(ser_cli test/ser_cli.cc ${src})
target_link_libraries(ser_cli ${libs})
