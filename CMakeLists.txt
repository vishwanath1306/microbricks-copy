cmake_minimum_required(VERSION 3.5.1)

project(HindsightGrpc C CXX)

set (CMAKE_CXX_STANDARD 11)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

find_package(Threads REQUIRED)

# find opentelemetry installation
find_package(opentelemetry-cpp CONFIG REQUIRED)
message(STATUS "Using opentelemetry-cpp ${OPENTELEMETRY_VERSION}")

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")
set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
set(_GRPC_GRPCPP gRPC::grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

# Proto file
get_filename_component(hs_proto "proto/hindsightgrpc.proto" ABSOLUTE)
get_filename_component(hs_proto_path "${hs_proto}" PATH)

# Generated sources
set(hs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/hindsightgrpc.pb.cc")
set(hs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/hindsightgrpc.pb.h")
set(hs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/hindsightgrpc.grpc.pb.cc")
set(hs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/hindsightgrpc.grpc.pb.h")
add_custom_command(
      OUTPUT "${hs_proto_srcs}" "${hs_proto_hdrs}" "${hs_grpc_srcs}" "${hs_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hs_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hs_proto}"
      DEPENDS "${hs_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# Hindsight include
include_directories(/usr/local/include/tracer/)

# External includes
include_directories(external)

# Source directory
file (GLOB CPP_FILES src/hindsightgrpc/*.cc src/tracing/*.cc) 
file (GLOB H_FILES src/hindsightgrpc/*.h src/tracing/*.h) 
set (SOURCE_FILES ${CPP_FILES} ${H_FILES})

# hs_grpc_proto
add_library(hs_grpc_proto
  ${hs_grpc_srcs}
  ${hs_grpc_hdrs}
  ${hs_proto_srcs}
  ${hs_proto_hdrs})
target_link_libraries(hs_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# curl and thrift for jaeger backend
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")
find_package(CURL REQUIRED)
find_package(Thrift REQUIRED)
find_package(nlohmann_json REQUIRED)

# Targets greeter_[async_](client|server)
foreach(_target
  client server process)
  add_executable(${_target} "src/${_target}.cc" ${CPP_FILES})
  target_include_directories(${_target} PRIVATE ${OPENTELEMETRY_CPP_INCLUDE_DIRS})
  target_link_libraries(${_target}
    hs_grpc_proto
    /usr/local/lib/libtracer.a
    ${OPENTELEMETRY_CPP_LIBRARIES}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})
endforeach()
