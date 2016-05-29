if (EXISTS "${CMAKE_BINARY_DIR}/settings.cmake")
  include(${CMAKE_BINARY_DIR}/settings.cmake)
endif ()

cmake_minimum_required(VERSION 2.8)

project(leveldb-fs)

find_package(PkgConfig)

pkg_search_module(FUSE REQUIRED fuse)

add_executable(leveldb-fs leveldb-fs.cpp dentry.cpp dentry.h)

target_link_libraries(leveldb-fs
  ${FUSE_LIBRARIES} leveldb uuid)
target_include_directories(leveldb-fs PUBLIC ${FUSE_INCLUDE_DIRS})
target_compile_options(leveldb-fs PUBLIC ${FUSE_CFLAGS_OTHER})
