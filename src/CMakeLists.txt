add_library(leveldb STATIC)

add_library(leveldb::leveldb ALIAS leveldb)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    set(LEVELDB_COMMON_COMPILE_OPTIONS
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wsign-conversion
        -Wshadow
        -Wnon-virtual-dtor
        -Wold-style-cast
        -Woverloaded-virtual
    )
elseif(MSVC)
    set(LEVELDB_COMMON_COMPILE_OPTIONS
        /W4
        /permissive-
    )
endif()

file(GLOB_RECURSE LEVELDB_MODULE_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cppm")

file(GLOB_RECURSE LEVELDB_IMPL_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.test.cpp")

target_sources(leveldb
    PUBLIC
        FILE_SET CXX_MODULES TYPE CXX_MODULES
        FILES
            ${LEVELDB_MODULE_SOURCES}
    PRIVATE
        ${LEVELDB_IMPL_SOURCES}
)

target_compile_features(leveldb PUBLIC cxx_std_23)
target_compile_options(leveldb PRIVATE ${LEVELDB_COMMON_COMPILE_OPTIONS})

target_link_libraries(leveldb
    PRIVATE
        xin::base
        doctest::doctest
)

target_include_directories(leveldb
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
)


if(${PROJECT_NAME}_BUILD_TESTS)
    add_executable(leveldb.test unit_test.cpp)

    target_link_libraries(leveldb.test
        PRIVATE
            $<LINK_LIBRARY:WHOLE_ARCHIVE,leveldb::leveldb>
            doctest::doctest
    )

    target_compile_features(leveldb.test PRIVATE cxx_std_23)
    target_compile_options(leveldb.test PRIVATE ${LEVELDB_COMMON_COMPILE_OPTIONS})
    add_test(NAME leveldb.test COMMAND leveldb.test)
endif()