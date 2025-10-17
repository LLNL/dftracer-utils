macro(add_rpath)
  set(CMAKE_SKIP_BUILD_RPATH OFF)
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH ON)
  list(REMOVE_DUPLICATES DEPENDENCY_LIBRARY_DIRS)
  set(CMAKE_INSTALL_RPATH "${DEPENDENCY_LIBRARY_DIRS}")
  set(CMAKE_BUILD_RPATH "${DEPENDENCY_LIBRARY_DIRS}")
  if(APPLE)
    set(CMAKE_MACOSX_RPATH ON)
    set(CMAKE_BUILD_WITH_INSTALL_RPATH OFF)
  else()
    set(CMAKE_BUILD_WITH_INSTALL_RPATH ON)
  endif()
endmacro()

# function(normalize_rpath rpaths output)
#   set(result_str "")
#   if(APPLE)
#     # in APPLE, we use ;
#     foreach(rpath IN LISTS rpaths)
#       string(REPLACE "$ORIGIN" "@loader_path" rpath "${rpath}")
#       set(result_str "${result_str};${rpath}")
#     endforeach()
#   else()
#     foreach(rpath IN LISTS rpaths)
#       string(REPLACE "$ORIGIN" "@loader_path" rpath "${rpath}")
#       set(result_str "${result_str}:${rpath}")
#     endforeach()
#   endif()
#   set(output
#       "${result_str}"
#       PARENT_SCOPE)
# endfunction()

# function(target_add_rpath target_name)
#   set(rpath_list "")
#   set(additional_paths ${ARGN})
#   if(APPLE)
#     list(APPEND rpath_list "@loader_path" "@loader_path/lib")
#     foreach(path IN LISTS additional_paths)
#       list(APPEND rpath_list "${path}")
#     endforeach()
#     normalize_rpath(rpath_list rpath_list)
#   else()
#     list(APPEND rpath_list "$ORIGIN" "$ORIGIN/lib" "$ORIGIN/../lib"
#          "$ORIGIN/../lib64")
#     foreach(path IN LISTS additional_paths)
#       list(APPEND rpath_list "${path}")
#     endforeach()
#   endif()
#   message(STATUS "RPATH for target ${target_name}: ${rpath_list}")
#   set_target_properties(
#     ${target_name}
#     PROPERTIES INSTALL_RPATH "${rpath_list}"
#                BUILD_RPATH "${rpath_list}"
#                BUILD_WITH_INSTALL_RPATH TRUE
#                INSTALL_RPATH_USE_LINK_PATH TRUE)
# endfunction()
