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

#include <AIDB/Factory/IndexStoreFactory.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
    extern const Event TantivyIndexRemoveCacheCounts;
    extern const Event TantivyIndexMutateCounts;
    extern const Event TantivyIndexRenamePartCounts;
    extern const Event TantivyIndexDropIndexCounts;

    extern const Event SparseIndexRemoveCacheCounts;
    extern const Event SparseIndexMutateCounts;
    extern const Event SparseIndexRenamePartCounts;
    extern const Event SparseIndexDropIndexCounts;
}

namespace DB
{


template <typename CacheType, typename StoreType, typename StoreTypePtr>
IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::IndexStoreFactory(
    const String & log_name, std::shared_ptr<CacheType> cache_, MutateRecordCachePtr mutate_to_from_)
    : log(&Poco::Logger::get(log_name + "IndexFactory")), cache(cache_), mutate_to_from(mutate_to_from_)
{
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::get(const StoreKey & store_key)
{
    return this->cache->get(store_key);
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr
IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::getForBuild(const String & skp_index_name, const DataPartStoragePtr storage)
{
    return this->cache->get(std::make_pair(storage->getRelativePath(), skp_index_name));
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr
IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::getOrLoadForSearch(const String & skp_index_name, const DataPartStoragePtr storage)
{
    DB::OpenTelemetry::SpanHolder span("index_store_factory::get_or_load_for_search");

    auto store_key = std::make_pair(storage->getRelativePath(), skp_index_name);
    // First check
    StoreTypePtr res = this->cache->get(store_key);
    if (res != nullptr)
    {
        LOG_DEBUG(
            this->log,
            "[getOrLoadForSearch] store_key: [{}, {}], full_index_path: {}",
            store_key.first,
            store_key.second,
            res->getFullIndexPathInStore());
        return res;
    }

    // Avoid multi-thread entry multi times.
    std::unique_lock<std::shared_mutex> lock(this->mutex_for_search);

    // Second check
    res = this->cache->get(store_key);
    if (res != nullptr)
    {
        return res;
    }

    // Ensure only one thread can generate store object, avoid index files corrupt.
    StoreTypePtr new_store = std::make_shared<StoreType>(skp_index_name, storage);
    this->cache->set(store_key, new_store);
    LOG_INFO(
        this->log,
        "[getOrLoadForSearch] store_key: [{}, {}], ref count : {}, `store` size: {}, "
        "`mutate_to_from` size: {}",
        store_key.first,
        store_key.second,
        new_store.use_count(),
        this->cache->count(),
        this->mutate_to_from->count());
    return new_store;
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
StoreTypePtr IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::getOrInitForBuild(
    const String & skp_index_name, const DataPartStoragePtr storage, MutableDataPartStoragePtr storage_builder)
{
    auto store_key = std::make_pair(storage->getRelativePath(), skp_index_name);

    // First check
    StoreTypePtr res = this->cache->get(store_key);
    if (res != nullptr)
    {
        LOG_DEBUG(
            this->log,
            "[getOrInitForBuild] store_key: [{}, {}], full_index_path: {}",
            store_key.first,
            store_key.second,
            res->getFullIndexPathInStore());
        return res;
    }

    // Avoid multi-thread entry multi times.
    std::unique_lock<std::shared_mutex> lock(this->mutex_for_build);

    // Second check
    res = this->cache->get(store_key);
    if (res != nullptr)
    {
        return res;
    }

    // Ensure only one thread can generate store object, avoid index files corrupt.
    StoreTypePtr new_store = std::make_shared<StoreType>(skp_index_name, storage, storage_builder);
    this->cache->set(store_key, new_store);
    LOG_INFO(
        this->log,
        "[getOrInitForBuild] store_key: [{}, {}], ref_count: {}, `store` size: {}, "
        "`mutate_to_from` size: {}",
        store_key.first,
        store_key.second,
        new_store.use_count(),
        this->cache->count(),
        this->mutate_to_from->count());
    return new_store;
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
size_t IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::remove(const String & data_part_relative_path, const DB::Names & index_names)
{
    if (typeid(CacheType) == typeid(TantivyIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::TantivyIndexRemoveCacheCounts);
    }
    else if (typeid(CacheType) == typeid(SparseIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::SparseIndexRemoveCacheCounts);
    }

    size_t hitted = 0;
    for (size_t i = 0; i < index_names.size(); i++)
    {
        auto store_key = std::make_pair(data_part_relative_path, SKP_PREFIX + index_names[i]);
        StoreTypePtr res = this->cache->get(store_key);
        if (res)
        {
            hitted++;
            this->cache->remove(store_key);
        }
    }

    LOG_INFO(
        this->log,
        "[remove] part_rel_path: {}, removed: {}, `stores` size: {}, `mutate_to_from` size: {}",
        data_part_relative_path,
        hitted,
        this->cache->count(),
        this->mutate_to_from->count());

    return hitted;
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::mutate(
    const String & source_part_relative_path, const String & target_part_relative_path)
{
    if (typeid(CacheType) == typeid(TantivyIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::TantivyIndexMutateCounts);
    }
    else if (typeid(CacheType) == typeid(SparseIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::SparseIndexMutateCounts);
    }

    this->mutate_to_from->set(target_part_relative_path, source_part_relative_path);
    LOG_INFO(
        this->log,
        "[mutate] from `{} to `{}`, `mutate_to_from` size: {}",
        source_part_relative_path,
        target_part_relative_path,
        this->mutate_to_from->count());
}

template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::renamePart(
    const String & data_part_relative_path_before_rename, const DataPartStoragePtr storage, const DB::Names & index_names)
{
    if (typeid(CacheType) == typeid(TantivyIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::TantivyIndexRenamePartCounts);
    }
    else if (typeid(CacheType) == typeid(SparseIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::SparseIndexRenamePartCounts);
    }

    auto context = Context::getGlobalContextInstance();
    auto data_part_relative_path_after_rename = storage->getRelativePath();

    String data_part_relative_path_before_mutate = this->mutate_to_from->get(data_part_relative_path_before_rename);

    if (!data_part_relative_path_before_mutate.empty())
    {
        // We were able to find the mutate record from `mutate_to_from` for that data part.
        // Indicates that LWD and materialize index may have occurred.
        // To make the `IndexStore` reusable, append a new key to the `IndexStore` and point to the old `IndexStore` shared pointer.
        bool is_lwd = updateStoresForMutate(data_part_relative_path_before_mutate, data_part_relative_path_after_rename, index_names);

        if (!is_lwd)
        {
            // After rename operation is complete, the `IndexStore` should update the index cache directory (starts with `tmp`)
            auto target_part_full_path_in_cache = this->innerCreateNewPathInStore(storage->getRelativePath());

            // The `data_part_relative_path_before_mutate` record before mutate occurred cannot be found in the stores keys,
            // indicates that the mutate operation may execute `MATERIALIZE INDEX`.
            updateStoresForBuild(
                data_part_relative_path_before_rename, data_part_relative_path_after_rename, target_part_full_path_in_cache, index_names);
        }
        // Remove mutate operation from `mutate_to_from` record.
        this->mutate_to_from->remove(data_part_relative_path_before_rename);

        LOG_INFO(
            this->log,
            "[renamePart] after mutate(lwd:{}), before_rename {}, after_rename {}, `stores` size: {}, "
            "`mutate_to_from` size: {}",
            is_lwd,
            is_lwd ? data_part_relative_path_before_mutate : data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            this->cache->count(),
            this->mutate_to_from->count());
    }
    else
    {
        // After rename operation is complete, the `IndexStore` should update the index cache directory (starts with `tmp`)
        auto target_part_full_path_in_cache = this->innerCreateNewPathInStore(storage->getRelativePath());


        // We can't find the mutate record from `mutate_to_from` for that data part.
        // Indicates that `tmp_insert`, `tmp_merge` and `tmp_clone` may have occurred.
        // These operations will generate a new `IndexStore`, we need to update the index cache directory in this tmp `IndexStore`.
        updateStoresForBuild(
            data_part_relative_path_before_rename, data_part_relative_path_after_rename, target_part_full_path_in_cache, index_names);

        LOG_INFO(
            this->log,
            "[renamePart] after set/merge/xxx, before_rename {}, after_rename {}, `stores` size: {}, "
            "`mutate_to_from` size: {}",
            data_part_relative_path_before_rename,
            data_part_relative_path_after_rename,
            this->cache->count(),
            this->mutate_to_from->count());
    }
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::dropIndex(const String & skp_index_name, const DataPartStoragePtr storage)
{
    if (typeid(CacheType) == typeid(TantivyIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::TantivyIndexDropIndexCounts);
    }
    else if (typeid(CacheType) == typeid(SparseIndexStoreCache))
    {
        ProfileEvents::increment(ProfileEvents::SparseIndexDropIndexCounts);
    }

    try
    {
        // storage->getRelativePath(): store/069/069cd2be-0a8f-4091-ad82-38015b19bdef/all_1_1_0
        StoreKey store_key = std::make_pair(storage->getRelativePath(), skp_index_name);
        this->cache->remove(store_key);
    }
    catch (Exception & e)
    {
        LOG_ERROR(
            this->log,
            "[dropIndex] Error happened when dropIndex, stores(build/search) may not be cleaned correctly, data part relative path {}, "
            "exception is {}",
            storage->getRelativePath(),
            e.what());
    }
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::updateStoresForBuild(
    const String & data_part_relative_path_before_rename,   // tmp_mut_all_20930_20950_1_x
    const String & data_part_relative_path_after_rename,    // all_30010_30010_1_x
    const String & target_part_full_path_in_cache,          // /xxx/sparse_index_store/store/6a5/xxx/all_47761_47823_1_47829
    const DB::Names & index_names)
{
    std::unordered_map<StoreKey, StoreTypePtr, KeyHash<StoreKey>> stores_need_append;
    std::vector<StoreKey> old_keys_to_remove;

    for (size_t i = 0; i < index_names.size(); i++)
    {
        StoreKey store_key = std::make_pair(data_part_relative_path_before_rename, SKP_PREFIX + index_names[i]);
        StoreTypePtr store_ptr = this->cache->get(store_key);
        if (store_ptr)
        {
            store_ptr->updateFullIndexPathInStore(target_part_full_path_in_cache);

            // Update store_key in this->stores.
            StoreKey new_key = std::make_pair(data_part_relative_path_after_rename, SKP_PREFIX + index_names[i]);
            stores_need_append[new_key] = store_ptr;
            old_keys_to_remove.push_back(store_key);
        }
    }

    // update stores in cache.
    for (auto & [key, store] : stores_need_append)
    {
        this->cache->set(key, store);
        LOG_INFO(
            this->log,
            "[updateStoresForBuild] set updated store into `stores`, store_key: [{}, {}], store_inner_index_path(new): {}, "
            "current cache size:{}",
            key.first,
            key.second,
            store->getFullIndexPathInStore(),
            this->cache->count());
    }

    for (auto & old_key : old_keys_to_remove)
    {
        this->cache->remove(old_key);
    }

    LOG_INFO(this->log, "[updateStoresForBuild] removed old stores count: {}, current cache size:{}", old_keys_to_remove.size(), this->cache->count());
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
bool IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::updateStoresForMutate(
    const String & data_part_relative_path_before_mutate, // [all_20930_20950_1_20960] -> mutate -> [tmp_mut_all_20930_20950_1_x]
    const String & data_part_relative_path_after_rename, // [tmp_mut_all_20930_20950_1_x] -> rename -> [all_20930_20950_1_x]
    const DB::Names & index_names)
{
    bool is_lwd = false;
    for (size_t i = 0; i < index_names.size(); i++)
    {
        StoreKey old_key = std::make_pair(data_part_relative_path_before_mutate, SKP_PREFIX + index_names[i]);
        StoreKey new_key = std::make_pair(data_part_relative_path_after_rename, SKP_PREFIX + index_names[i]);

        StoreTypePtr old_store_ptr = this->cache->get(old_key);
        if (old_store_ptr)
        {
            is_lwd = true;
            this->cache->set(new_key, old_store_ptr);
        }
        LOG_INFO(
            this->log,
            "[updateStoresForMutate] skp_idx_name: {}, is_lwd: {}, old_key: [{},{}], new_key: [{},{}], full_index_path(old): {}, "
            "mutate_size:{}",
            index_names[i],
            is_lwd,
            old_key.first,
            old_key.second,
            new_key.first,
            new_key.second,
            old_store_ptr == nullptr ? "" : old_store_ptr->getFullIndexPathInStore(),
            this->mutate_to_from->count());
    }
    return is_lwd;
}


template <typename CacheType, typename StoreType, typename StoreTypePtr>
void IndexStoreFactory<CacheType, StoreType, StoreTypePtr>::freeIdleStoreReader(
    const DataPartStoragePtr storage, const DB::Names & index_names)
{
    for (size_t i = 0; i < index_names.size(); i++)
    {
        StoreKey store_key = std::make_pair(storage->getRelativePath(), SKP_PREFIX + index_names[i]);
        StoreTypePtr store_ptr = this->cache->get(store_key);
        if (store_ptr)
        {
            // If the ref count of the store is 2, it means that the store is not used by any other part.
            if (store_ptr.use_count() == 2)
            {
                store_ptr->freeIndexReader();
            }
            LOG_INFO(
                this->log,
                "[freeIdleStoreReader] skp_idx_name: {}, key: [{},{}], current cache size: {}, mutate_size: {}",
                index_names[i],
                store_key.first,
                store_key.second,
                this->cache->count(),
                this->mutate_to_from->count());
        }
    }
}
}
