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

#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <AIDB/Common/SkipIndex/StoreDirectoryHelper.h>
#include <AIDB/Common/SkipIndex/IndexFileMeta.h>

namespace DB
{

void StoreDirectoryHelper::removeIndexPathWithCleanup(const String & full_index_path_in_store_, bool end_with_skp_idx)
{
    try
    {
        fs::path full_index_path_in_store = fs::path(full_index_path_in_store_);
        auto tmp_disk = std::make_shared<DiskLocal>(TEMP_DISK_FOR_DIRECTORY_HELPER, Context::getGlobalContextInstance()->getPath());

        if (tmp_disk->isDirectory(full_index_path_in_store))
        {
            LOG_INFO(&Poco::Logger::get("StoreDirectoryHelper"), "try remove index store directory: `{}`, end_with_skp_idx: {}", full_index_path_in_store, end_with_skp_idx);
        }
        else
        {
            LOG_INFO(&Poco::Logger::get("StoreDirectoryHelper"), "index store directory: `{}` is not exists, skip remove it", full_index_path_in_store);
            return;
        }

        // data_part_full_path_in_store: `/var/lib/clickhouse/sparse_index_store/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/`
        fs::path data_part_full_path_in_store;
        if (end_with_skp_idx)
        {
            // index_full_path_in_store: `/var/lib/clickhouse/sparse_index_store/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/skp_idx_test_idx/`
            StoreDirectoryHelper::removeDirectoryDirectly(full_index_path_in_store);
            data_part_full_path_in_store = full_index_path_in_store.parent_path().parent_path();
        }
        else
        {
            // index_full_path_in_store: `/var/lib/clickhouse/sparse_index_store/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855/202406_60_60_0/`
            data_part_full_path_in_store = full_index_path_in_store.parent_path();
        }

        StoreDirectoryHelper::removeDirectoryIfEmpty(data_part_full_path_in_store);

        // table_uuid_directory_in_store: `/var/lib/clickhouse/sparse_index_store/store/6b0/6b0c995b-a94f-43f4-87f5-1c4f8d56c855`
        auto table_uuid_directory_in_store = data_part_full_path_in_store.parent_path();
        StoreDirectoryHelper::removeDirectoryIfEmpty(table_uuid_directory_in_store);

        // table_uuid_prefix_directory_in_store: `/var/lib/clickhouse/sparse_index_store/store/6b0`
        auto table_uuid_prefix_directory_in_store = table_uuid_directory_in_store.parent_path();
        StoreDirectoryHelper::removeDirectoryIfEmpty(table_uuid_prefix_directory_in_store);
    }
    catch (...)
    {
        LOG_ERROR(&Poco::Logger::get("StoreDirectoryHelper"), "Error happend when removing `{}` forwardly", full_index_path_in_store_);
    }
}

void StoreDirectoryHelper::removeIndexPathWithCleanup(
    const String & part_relative_path_in_store, const CustomIndexType index_type, const String & skp_index_name)
{
    try
    {
        std::optional<fs::path> res
            = StoreDirectoryHelper::convertPartRelativePathToFullPathInStore(part_relative_path_in_store, index_type);
        if (res.has_value())
        {
            fs::path data_part_full_path_in_store = res.value();
            fs::path index_full_path_in_store = data_part_full_path_in_store / skp_index_name / "";
            StoreDirectoryHelper::removeIndexPathWithCleanup(index_full_path_in_store);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            &Poco::Logger::get("StoreDirectoryHelper"),
            "Error happend when removing part: `{}` skp_idx_name:`{}` forwardly",
            part_relative_path_in_store,
            skp_index_name);
    }
}

void StoreDirectoryHelper::removePartRelativePathInStoreForward(const String & part_relative_path_in_store, const CustomIndexType index_type)
{
    try
    {
        // part_relative_path_in_store: `store/328/3284465c-f924-4e8e-a09d-50f192710683/all_5_5_0_6/`
        std::optional<fs::path> res
            = StoreDirectoryHelper::convertPartRelativePathToFullPathInStore(part_relative_path_in_store, index_type);
        if (res.has_value() && fs::exists(res.value()))
        {
            StoreDirectoryHelper::removeIndexPathWithCleanup(res.value(), false);
        }
    }
    catch (...)
    {
        LOG_ERROR(
            &Poco::Logger::get("StoreDirectoryHelper"),
            "[removePartRelativePathInStoreForward] Error happend when removing `{}` forwardly",
            part_relative_path_in_store);
    }
}


void StoreDirectoryHelper::removeDirectoryDirectly(const fs::path & directory)
{
    try
    {
        auto tmp_disk = std::make_shared<DiskLocal>(TEMP_DISK_FOR_DIRECTORY_HELPER, Context::getGlobalContextInstance()->getPath());

        if (tmp_disk->isDirectory(directory))
        {
            tmp_disk->clearDirectory(directory);
            tmp_disk->removeDirectory(directory);
        }
    }
    catch (...)
    {
        LOG_ERROR(&Poco::Logger::get("StoreDirectoryHelper"), "error happened when removing `{}` directly", directory);
    }
}

void StoreDirectoryHelper::removeDirectoryIfEmpty(const fs::path & directory)
{
    try
    {
        auto tmp_disk = std::make_shared<DiskLocal>(TEMP_DISK_FOR_DIRECTORY_HELPER, Context::getGlobalContextInstance()->getPath());

        if (tmp_disk->isDirectory(directory) && tmp_disk->isDirectoryEmpty(directory))
        {
            tmp_disk->removeRecursive(directory);
        }
    }
    catch (...)
    {
        LOG_ERROR(&Poco::Logger::get("StoreDirectoryHelper"), "error happened when removing empty `{}`", directory);
    }
}

std::optional<fs::path>
StoreDirectoryHelper::convertPartRelativePathToFullPathInStore(const String & relative_data_part_in_store, const CustomIndexType index_type)
{
    try
    {
        auto context = Context::getGlobalContextInstance();
        fs::path skp_index_store_prefix;
        if (index_type == CustomIndexType::SparseIndex)
        {
            skp_index_store_prefix = context->getSparseIndexStorePath();
        }
        else if (index_type == CustomIndexType::TantivyIndex)
        {
            skp_index_store_prefix = context->getTantivyIndexStorePath();
        }
        else
        {
            LOG_ERROR(
                &Poco::Logger::get("StoreDirectoryHelper"),
                "error happend when converting data part relative path to full_path_in_store, custom_index_type: `{}`",
                getCustomIndexName(index_type));
            return std::nullopt;
        }

        // example: /var/lib/clickhouse/sparse_index_store/store/ba1/ba1625f1-dbf2-4ad4-a06c-e6c4e611984a/all_1_1_1_2/
        auto data_part_full_store_path = skp_index_store_prefix / relative_data_part_in_store / "";

        // example-1: fs::path("store/ba1/ba1625f1-dbf2-4ad4-a06c-e6c4e611984a/all_1_1_1_2/") distance is 5
        // example-2: fs::path("store/ba1/ba1625f1-dbf2-4ad4-a06c-e6c4e611984a/all_1_1_1_2") distance is 4
        constexpr int required_depth = 4; // Corrected depth
        if (std::distance(data_part_full_store_path.begin(), data_part_full_store_path.end()) < required_depth)
        {
            return std::nullopt;
        }

        fs::path store_path = data_part_full_store_path;
        for (int i = 0; i < required_depth; ++i)
        {
            store_path = store_path.parent_path();
        }

        if (!store_path.has_filename() || (store_path.filename() != "store" && store_path.filename() != "data"))
        {
            return std::nullopt;
        }

        return data_part_full_store_path;
    }
    catch (...)
    {
        LOG_ERROR(
            &Poco::Logger::get("StoreDirectoryHelper"),
            "error happend when converting data part relative path to full_path_in_store, rel_data_part: `{}`",
            relative_data_part_in_store);
        return std::nullopt;
    }
}


}
