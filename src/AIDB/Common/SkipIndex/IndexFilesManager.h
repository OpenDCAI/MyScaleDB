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

#include <array>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Disks/IDisk.h>
#include <Disks/IVolume.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <AIDB/Common/SkipIndex/StoreDirectoryHelper.h>
#include <AIDB/Common/SkipIndex/IndexType.h>

namespace DB
{

using ChecksumPairs = std::vector<std::pair<String, MergeTreeDataPartChecksums::Checksum>>;

static constexpr auto TEMP_DISK_NAME = "_tmp_skp_idx_disk";


class IndexFilesManager
{
public:
    IndexFilesManager(
        const CustomIndexType index_type_,
        const String & index_meta_file_suffix,
        const String & index_data_file_suffix,
        const String & skp_index_name_,
        const DataPartStoragePtr storage_,
        const MutableDataPartStoragePtr storage_builder_ = nullptr);

    /// @brief get current part index directory in store.
    /// @return example "/var/lib/clickhouse/xx_index_store/store/20a/ - uuid - /all_1_1_1_0/skp_idx_name/"
    String getFullIndexPathInStore();

    /// @brief update current part index directory in store.
    /// @param new_part_path_in_store example: `/xx/.../xx_index_store/store/xxx/xxx/all_1_1_0_2`
    String updateFullIndexPathInStore(const String & new_part_path_in_store);

    /// @brief remove current part index directory in store. forward stop at `store` or `data`.
    void removeFullIndexPathInStoreForward();

    /// @brief archive index files to part.
    ChecksumPairs archive();

    /// @brief extract index files from part to index store.
    void extract();

private:
    const CustomIndexType index_type;
    const String skp_idx_name;

    const DataPartStoragePtr storage;
    const MutableDataPartStoragePtr storage_builder;

    const String index_meta_file_name;
    const String index_data_file_name;

    const ContextPtr context;
    const Poco::Logger * log;
    const DiskPtr tmp_disk;


    /// example: "/var/lib/clickhouse/xx_index_store/store/20a/ - uuid - /all_1_1_1_0/skp_idx_name/"
    String full_index_path_in_store = "";

    mutable std::shared_mutex full_index_path_lock;

    /// @brief  init variable `full_index_path_in_store`.
    void initFullIndexPathInStore();
};


}
