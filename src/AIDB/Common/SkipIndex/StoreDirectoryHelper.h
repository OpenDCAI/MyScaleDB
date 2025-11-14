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

#include <filesystem>
#include <iostream>
#include <Core/Block.h>
#include <AIDB/Common/SkipIndex/IndexType.h>


namespace fs = std::filesystem;


namespace DB
{

static const auto TEMP_DISK_FOR_DIRECTORY_HELPER = "_tmp_disk_for_directory_helper";

class StoreDirectoryHelper
{
public:
    static void removeIndexPathWithCleanup(const String & full_index_path_in_store, bool end_with_skp_idx = true);
    static void removeIndexPathWithCleanup(
        const String & part_relative_path_in_store, const CustomIndexType index_type, const String & skp_index_name);

    static void removePartRelativePathInStoreForward(const String & part_relative_path_in_store, const CustomIndexType index_type);

    /// @brief get part full path in store directory.
    /// @param relative_data_part_in_store data part relative path, example: `store/xxx/all_1_1_1_0`.
    static std::optional<fs::path>
    convertPartRelativePathToFullPathInStore(const String & relative_data_part_in_store, const CustomIndexType index_type);

    static void removeDirectoryDirectly(const fs::path & directory);
    static void removeDirectoryIfEmpty(const fs::path & directory);
};

}
