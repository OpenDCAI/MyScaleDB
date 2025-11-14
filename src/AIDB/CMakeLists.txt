add_headers_and_sources(clickhouse_vector_index .)
add_headers_and_sources(clickhouse_vector_index Common)
add_headers_and_sources(clickhouse_vector_index Common/SkipIndex)
add_headers_and_sources(clickhouse_vector_index Interpreters)
add_headers_and_sources(clickhouse_vector_index Parsers)
add_headers_and_sources(clickhouse_vector_index Processors)
add_headers_and_sources(clickhouse_vector_index Storages)
add_headers_and_sources(clickhouse_vector_index Utils)
add_headers_and_sources(clickhouse_vector_index Cache)
add_headers_and_sources(clickhouse_vector_index Cache/SkipIndex)
add_headers_and_sources(clickhouse_vector_index TableFunctions)
add_headers_and_sources(clickhouse_vector_index Factory)
add_headers_and_sources(clickhouse_vector_index Store)

if (NOT ENABLE_RUST)
    remove_specific_files(clickhouse_interpreters 
        "Interpreters/TantivyFilter.h"
        "Interpreters/TantivyFilter.cpp"
        "Cache/SkipIndex/IndexStoreCache.h"
        "Cache/SkipIndex/MutateRecordCache.h" 
        "Cache/SkipIndex/SparseIndexStoreCache.h"
        "Cache/SkipIndex/TantivyIndexStoreCache.h"
        "Common/SkipIndex/StoreDirectoryHelper.cpp"
        "Common/SkipIndex/StoreDirectoryHelper.h"
        "Common/SkipIndex/IndexFileMeta.cpp"
        "Common/SkipIndex/IndexFileMeta.h"
        "Common/SkipIndex/IndexFilesManager.cpp"
        "Common/SkipIndex/IndexFilesManager.h"
        "Common/SkipIndex/IndexSettings.cpp"
        "Common/SkipIndex/IndexSettings.h"
        "Common/SkipIndex/IndexType.h"
        "Factory/IndexStoreFactory.h"
        "Factory/IndexStoreFactoryImpl.h"
        "Factory/SparseIndexFactory.h"
        "Factory/TantivyIndexFactory.h"
        "Store/IndexStore.cpp"
        "Store/IndexStore.h"
        "Store/SparseIndexStore.cpp"
        "Store/SparseIndexStore.h"
        "Store/TantivyIndexStore.cpp"
        "Store/TantivyIndexStore.h"
    )
endif()

add_library(clickhouse_vector_index ${clickhouse_vector_index_sources})
target_link_libraries(clickhouse_vector_index
    PUBLIC
	dbms
    PRIVATE
	ch_contrib::search_index
    daemon
)

target_include_directories(clickhouse_vector_index 
    SYSTEM PUBLIC 
    ${CMAKE_SOURCE_DIR}/contrib/search-index
)

if (TARGET ch_rust::supercrate)
    target_link_libraries(clickhouse_vector_index
        PRIVATE
        ch_rust::supercrate
    )
endif()