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

#include <AIDB/Cache/SkipIndex/IndexStoreCache.h>
#include <AIDB/Store/SparseIndexStore.h>
#include <Common/CacheBase.h>

namespace DB
{

class SparseIndexStoreCache : public IndexStoreCache<SparseIndexStore, SparseIndexStoreWeightFunc, SparseIndexStorePtr>
{
public:
    SparseIndexStoreCache(size_t max_size, String log_name) : IndexStoreCache(max_size, log_name) {}

    ~SparseIndexStoreCache() override = default;

    SparseIndexStoreCache & getInstance()
    {
        static SparseIndexStoreCache sparse_cache(INDEX_STORE_CACHE_SIZE, "Sparse");
        return sparse_cache;
    }
};

using SparseIndexStoreCachePtr = std::shared_ptr<SparseIndexStoreCache>;


}
