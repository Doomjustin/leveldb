function(copy_resource _target _dir)
    add_custom_command(TARGET ${_target} POST_BUILD
        COMMAND ${CMAKE_COMMAND}
                -E copy_directory_if_different
                ${_dir}
                $<TARGET_FILE_DIR:${_target}>
    )
endfunction()

function(copy_single_file _target _file)
    add_custom_command(TARGET ${_target} POST_BUILD
        COMMAND ${CMAKE_COMMAND}
                -E copy_if_different
                ${_file}
                $<TARGET_FILE_DIR:${_target}>
    )
endfunction()
