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

#include <Common/CacheBase.h>

namespace DB
{

/// @brief Even if the memory limit is set here, Key-Value expulsion will never occur during runtime.
static constexpr UInt32 MUTATE_RECORD_CACHE_SIZE = 4294967295U; // 4GB


class MutateRecordWeightFunc
{
public:
    size_t operator()(const String & /* value */) const { return 1; }
};


class MutateRecordCache
{
public:
    MutateRecordCache(const MutateRecordCache &) = delete;
    MutateRecordCache & operator=(const MutateRecordCache &) = delete;

    explicit MutateRecordCache(size_t max_size) : cache(max_size) { }

    ~MutateRecordCache() = default;

    /// @brief key: part rel_path after mutate, value: part rel_path before mutate.
    void set(const String & key, const String & value) { this->cache.set(key, std::make_shared<String>(value)); }

    /// @brief get part rel_path before mutate, return empty string if not exists.
    String get(const String & key)
    {
        auto res = this->cache.get(key);
        return res ? *res : String{};
    }

    /// @brief remove mutate record.
    void remove(const String & key) { this->cache.remove(key); }

    size_t count() { return this->cache.count(); }


private:
    CacheBase<String, String, std::hash<String>, MutateRecordWeightFunc> cache;
};

using MutateRecordCachePtr = std::shared_ptr<MutateRecordCache>;


}
