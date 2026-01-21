# cmake/_download.cmake
# usage: cmake -DURL="..." -DOUT="..." -P cmake/_download.cmake

if(NOT DEFINED URL)
    message(FATAL_ERROR "URL not set. Pass -DURL=...")
endif()
if(NOT DEFINED OUT)
    message(FATAL_ERROR "OUT not set. Pass -DOUT=...")
endif()

set(_attempts 3)
set(_ok FALSE)
foreach(i RANGE 1 ${_attempts})
    message(STATUS "[${i}/${_attempts}] Downloading: ${URL}")
    file(DOWNLOAD
        "${URL}" "${OUT}"
        INACTIVITY_TIMEOUT 60
        TIMEOUT 600
        SHOW_PROGRESS
        TLS_VERIFY ON
        HTTPHEADER "User-Agent: CMakeDownloader"
        STATUS _st
    )
    list(GET _st 0 _code)
    list(GET _st 1 _msg)
    if(_code EQUAL 0)
        set(_ok TRUE)
        break()
    else()
        message(WARNING "Download failed (code=${_code}): ${_msg}")
        # sleep
        execute_process(COMMAND ${CMAKE_COMMAND} -E sleep 2)
    endif()
endforeach()

if(NOT _ok)
    message(FATAL_ERROR "Failed to download: ${URL}")
endif()

# check size
file(SIZE "${OUT}" _sz)
if(_sz LESS 1024)
    message(WARNING "Downloaded file is suspiciously small (${_sz} bytes): ${OUT}")
endif()

message(STATUS "Saved to: ${OUT}")
