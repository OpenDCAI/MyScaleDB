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

#include <AIDB/Store/IndexStore.h>
#include <Common/CacheBase.h>

namespace DB
{

/// @brief Even if the memory limit is set here, Key-Value expulsion will never occur during runtime.
static constexpr UInt32 INDEX_STORE_CACHE_SIZE = 4294967295U; // 4GB
using StoreKey = std::pair<String, String>;

template <typename T>
struct KeyHash;

/// @brief template specialization
template <>
struct KeyHash<StoreKey>
{
public:
    size_t operator()(const StoreKey & p) const
    {
        return std::hash<std::string>{}(p.first + "|" + p.second);
    }
};

template <typename StoreType, typename WeightFunc, typename StoreTypePtr>
class IndexStoreCache
{
public:
    IndexStoreCache(size_t max_size, String log_name) : cache(max_size), log(&Poco::Logger::get(log_name + " Index Store Cache"))
    {
        LOG_INFO(log, "{} Index Store Cache initialized", log_name);
    }

    virtual ~IndexStoreCache() = default;

    /// avoid destory singleton.
    IndexStoreCache(const IndexStoreCache &) = delete;
    IndexStoreCache & operator=(const IndexStoreCache &) = delete;

    virtual void set(const StoreKey & key, const StoreTypePtr value)
    {
        this->cache.set(key, value);
        LOG_TRACE(log, "Inserted key-value pair into index store cache. Key: ({}, {})", key.first, key.second);
    }

    virtual StoreTypePtr get(const StoreKey & key)
    {
        StoreTypePtr value = this->cache.get(key);
        LOG_TRACE(log, "Index Store Cache Key: ({}, {}), value found in index store cache: {}", key.first, key.second, value ? "true" : "false");
        return value;
    }

    virtual void remove(const StoreKey & key)
    {
        this->cache.remove(key);
        LOG_TRACE(log, "Removed key-value pair from index store cache. Key: ({}, {})", key.first, key.second);
    }

    virtual size_t count() const { return cache.count(); }


protected:
    CacheBase<StoreKey, StoreType, KeyHash<StoreKey>, WeightFunc> cache;
    Poco::Logger * log;
};

template <typename StoreType, typename WeightFunc, typename StoreTypePtr>
using IndexStoreCachePtr = std::shared_ptr<IndexStoreCache<StoreType, WeightFunc, StoreTypePtr>>;

}
