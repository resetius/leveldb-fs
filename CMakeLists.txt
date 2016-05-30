if (EXISTS "${CMAKE_BINARY_DIR}/settings.cmake")
  include(${CMAKE_BINARY_DIR}/settings.cmake)
endif ()

cmake_minimum_required(VERSION 2.8)

project(leveldb-fs)

find_package(PkgConfig)

pkg_search_module(FUSE REQUIRED fuse)

add_custom_command(OUTPUT
  ${CMAKE_CURRENT_BINARY_DIR}/messages.pb.h
  ${CMAKE_CURRENT_BINARY_DIR}/messages.pb.cc
  COMMAND protoc ${CMAKE_CURRENT_SOURCE_DIR}/messages.proto -I${CMAKE_CURRENT_SOURCE_DIR} --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/messages.proto)

add_executable(leveldb-fs
  leveldb-fs.cpp
  dentry.cpp
  dentry.h
  fentry.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/messages.pb.h
  ${CMAKE_CURRENT_BINARY_DIR}/messages.pb.cc
  )

add_executable(test-writer test-writer.cpp)

target_link_libraries(test-writer
  pthread)

add_executable(test-leveldb leveldb-test.cpp)

target_link_libraries(test-leveldb
  pthread leveldb)

target_link_libraries(leveldb-fs
  ${FUSE_LIBRARIES} leveldb uuid protobuf boost_system)
target_include_directories(leveldb-fs PUBLIC
  ${FUSE_INCLUDE_DIRS};${CMAKE_CURRENT_BINARY_DIR})
target_compile_options(leveldb-fs PUBLIC ${FUSE_CFLAGS_OTHER})
