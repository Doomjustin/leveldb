if(${PROJECT_NAME}_BUILD_TESTS)
    add_executable(leveldb.test unit_test.cpp)

    target_link_libraries(leveldb.test
        PRIVATE
            doctest::doctest
    )

    target_compile_features(leveldb.test PRIVATE cxx_std_23)
    add_test(NAME leveldb.test COMMAND leveldb.test)
endif()