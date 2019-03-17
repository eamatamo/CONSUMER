cmake_minimum_required(VERSION 2.8.7)

set(PROJECT
    consumer.cpp)

set(RDKAFKA_MIN_VERSION 0x00090400)
add_definitions(-std=c++11)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")


set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")


# Set output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/lib)

find_package(OpenCV)
find_package(OpenCV COMPONENTS core imgproc highgui)
find_package(OpenCV REQUIRED core imgproc highgui)
find_package(OpenCV COMPONENTS core imgproc highgui REQUIRED)

option(CPPKAFKA_RDKAFKA_STATIC_LIB "Link with Rdkafka static library." ON)


if (CPPKAFKA_RDKAFKA_STATIC_LIB)
    add_definitions("-DLIBRDKAFKA_STATICLIB")
endif()

find_package(RdKafka REQUIRED ${FIND_PACKAGE_QUIET})

link_libraries(${RDKAFKA_LIBRARY})

include_directories(SYSTEM ${RDKAFKA_INCLUDE_DIR} /usr/local/include/librdkafka/)

add_executable( consumer consumer.cpp )
target_link_libraries( consumer ${OpenCV_LIBS} ${RdKafka_LIBS} /usr/local/lib/librdkafka++.so.1)