# cmake/_flatten_topdir.cmake
if(NOT DEFINED DEST OR NOT DEFINED SRC)
    message(FATAL_ERROR "DEST and SRC must be set")
endif()

file(GLOB _children RELATIVE "${SRC}" "${SRC}/*")
list(LENGTH _children _n)

# Clean destination
if(EXISTS "${DEST}")
    file(REMOVE_RECURSE "${DEST}")
endif()
file(MAKE_DIRECTORY "${DEST}")

if(_n EQUAL 1 AND IS_DIRECTORY "${SRC}/${_children}")
    file(COPY "${SRC}/${_children}/" DESTINATION "${DEST}")
else()
    file(COPY "${SRC}/" DESTINATION "${DEST}")
endif()

file(REMOVE_RECURSE "${SRC}")
