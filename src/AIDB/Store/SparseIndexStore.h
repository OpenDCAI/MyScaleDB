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

#pragma once

#include <sparse_index.h>
#include <AIDB/Store/IndexStore.h>

namespace DB
{

static constexpr auto SPARSE_INDEX_META_FILE_SUFFIX = ".sparse_meta";
static constexpr auto SPARSE_INDEX_DATA_FILE_SUFFIX = ".sparse_data";
static constexpr auto SPARSE_TEMP_DISK_NAME = "_tmp_sparse";

class SparseIndexStore : public IndexStore
{
public:
    SparseIndexStore(
        const String & index_name_, const DataPartStoragePtr storage_, const MutableDataPartStoragePtr storage_builder_ = nullptr);
    ~SparseIndexStore() override;

    BoolWithMessage freeIndexReaderImpl(const String & full_index_path) override;
    BoolWithMessage freeIndexWriterImpl(const String & full_index_path) override;

    BoolWithMessage commitIndexImpl(const String & full_index_path) override;

    BoolWithMessage loadIndexReaderImpl(const String & full_index_path) override;
    BoolWithMessage loadIndexWriterImpl(const String & full_index_path) override;

    bool indexSparseVector(
        uint64_t row_id, const std::vector<String> & column_names, const std::vector<rust::Vec<SPARSE::TupleElement>> & sparse_vectors);

    rust::Vec<SPARSE::ScoredPointOffset> sparseSearch(
        const std::unordered_map<uint32_t, float> & sparse_vector, uint32_t topk, bool is_brute_force);

    rust::Vec<SPARSE::ScoredPointOffset> sparseSearchWithFilter(
        const std::unordered_map<uint32_t, float> & sparse_vector, uint32_t topk, bool is_brute_force, const std::vector<uint8_t> & u8_alived_bitmap = {});
};


class SparseIndexStoreWeightFunc
{
public:
    size_t operator()(const SparseIndexStore & /* value */) const { return 0; }
};

using SparseIndexStorePtr = std::shared_ptr<SparseIndexStore>;

}
