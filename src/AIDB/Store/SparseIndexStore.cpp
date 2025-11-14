/*
 * Copyright (2024) ORIGINHUB SINGAPORE PTE. LTD. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sparse_index.h>
#include <AIDB/Store/SparseIndexStore.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SPARSE_SEARCH_INTERNAL_ERROR;
    extern const int CUSTOM_SKIP_INDEX_BUILD_INTERNAL_ERROR;
}

SparseIndexStore::SparseIndexStore(
    const String & index_name_, const DataPartStoragePtr storage_, const MutableDataPartStoragePtr storage_builder_)
    : IndexStore(
        CustomIndexType::SparseIndex, index_name_, SPARSE_INDEX_META_FILE_SUFFIX, SPARSE_INDEX_DATA_FILE_SUFFIX, storage_, storage_builder_)
{
}

SparseIndexStore::~SparseIndexStore()
{
    this->freeIndexReaderAndWriter();
}

BoolWithMessage SparseIndexStore::freeIndexReaderImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_free_index_reader(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::freeIndexWriterImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_free_index_writer(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::commitIndexImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_commit_index(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::loadIndexReaderImpl(const String & full_index_path)
{
    SPARSE::FFIBoolResult free_status = SPARSE::ffi_load_index_reader(full_index_path);
    return FFI_BOOL_CONVERT(free_status);
}


BoolWithMessage SparseIndexStore::loadIndexWriterImpl(const String & full_index_path)
{
    SparseIndexSettingsPtr sparse_index_settings = std::dynamic_pointer_cast<SparseIndexSettings>(this->index_settings);

    SPARSE::FFIBoolResult create_status;
    if (sparse_index_settings->json_parameter == "{}" || sparse_index_settings->json_parameter.empty())
    {
        create_status = SPARSE::ffi_create_index(full_index_path);
    }
    else
    {
        create_status = SPARSE::ffi_create_index_with_parameter(full_index_path, sparse_index_settings->json_parameter);
    }
    return FFI_BOOL_CONVERT(create_status);
}

bool SparseIndexStore::indexSparseVector(
    uint64_t row_id, const std::vector<String> & /* column_names */, const std::vector<rust::Vec<SPARSE::TupleElement>> & sparse_vectors)
{
    // SparseIndexSettingsPtr sparse_index_settings = std::static_pointer_cast<SparseIndexSettings>(index_settings);
    // TODO: This func only support one column to be indexed currently!

    if (!this->getIndexWriterStatus())
        this->loadIndexWriter();

    String index_files_cache_path = this->index_files_manager->getFullIndexPathInStore();
    SPARSE::FFIBoolResult insert_status
        = SPARSE::ffi_insert_sparse_vector(index_files_cache_path, static_cast<uint32_t>(row_id), sparse_vectors.front());

    if (insert_status.error.is_error)
    {
        throw DB::Exception(ErrorCodes::CUSTOM_SKIP_INDEX_BUILD_INTERNAL_ERROR, "{}", std::string(insert_status.error.message));
    }

    if (!insert_status.result)
    {
        LOG_ERROR(log, "[indexOneRow] Error happend when SparseIndex indexing doc under index_cache:{}", index_files_cache_path);
        throw DB::Exception(
            ErrorCodes::CUSTOM_SKIP_INDEX_BUILD_INTERNAL_ERROR,
            "Error happend when SparseIndex indexing sparse_vector under index_cache:{}",
            index_files_cache_path);
    }

    return true;
}

rust::Vec<SPARSE::TupleElement> mapToVector(const std::unordered_map<uint32_t, float> & sparse_vector)
{
    rust::Vec<SPARSE::TupleElement> vector;
    vector.reserve(sparse_vector.size()); // Pre-allocate memory for efficiency

    for (const auto & pair : sparse_vector)
    {
        SPARSE::TupleElement element;
        element.dim_id = pair.first;
        element.weight_f32 = pair.second;
        element.weight_u8 = 0; // not used in this context
        element.weight_u32 = 0; // not used in this context
        element.value_type = 0; // indicating float type is used

        vector.push_back(element);
    }

    return vector;
}

rust::Vec<SPARSE::ScoredPointOffset> SparseIndexStore::sparseSearch(
    const std::unordered_map<uint32_t, float> & sparse_vector, uint32_t topk, bool is_brute_force)
{
    DB::OpenTelemetry::SpanHolder span("sparse_index_store::sparse_search");
    if (!this->index_reader_status)
        this->loadIndexReader();

    SPARSE::FFIScoreResult result = SPARSE::ffi_sparse_search(
        this->index_files_manager->getFullIndexPathInStore(), mapToVector(sparse_vector), {}, false, topk, is_brute_force);

    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::SPARSE_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}

rust::Vec<SPARSE::ScoredPointOffset> SparseIndexStore::sparseSearchWithFilter(
    const std::unordered_map<uint32_t, float> & sparse_vector, uint32_t topk, bool is_brute_force, const std::vector<uint8_t> & u8_alived_bitmap)
{
    DB::OpenTelemetry::SpanHolder span("sparse_index_store::sparse_search_with_filter");
    if (!this->index_reader_status)
        this->loadIndexReader();

    SPARSE::FFIScoreResult result = SPARSE::ffi_sparse_search(
        this->index_files_manager->getFullIndexPathInStore(), mapToVector(sparse_vector), u8_alived_bitmap, true, topk, is_brute_force);

    if (result.error.is_error)
    {
        throw DB::Exception(ErrorCodes::SPARSE_SEARCH_INTERNAL_ERROR, "{}", std::string(result.error.message));
    }
    return result.result;
}
}
