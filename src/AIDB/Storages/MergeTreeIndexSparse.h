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
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <AIDB/Store/SparseIndexStore.h>

// using namespace SPARSE;

namespace DB
{

static inline constexpr auto SPARSE_INDEX_NAME = "sparse";

struct RowIdRange
{
    /// First row ID in the range [
    UInt64 range_start;

    /// Last row ID in the range (inclusive) ]
    UInt64 range_end;
};


struct MergeTreeIndexGranuleSparse final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleSparse(const String & index_name_, const Block & index_sample_block_, const String & json_parameter_);

    ~MergeTreeIndexGranuleSparse() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !has_elems; }

    void addRowIdRange(UInt64 left, UInt64 right)
    {
        if (!this->ranges.empty() && this->ranges.back().row_id_end + 1 == left)
        {
            this->ranges.back().row_id_end = right;
        }
        else
        {
            this->ranges.emplace_back(left, right);
        }
        this->has_elems = true;
    }

    String index_name;
    Block index_sample_block;
    String json_parameter;

    bool has_elems;

    struct RowIdRange
    {
        UInt64 row_id_start;
        UInt64 row_id_end;

        RowIdRange() = default;
        RowIdRange(UInt64 start, UInt64 end) : row_id_start(start), row_id_end(end) { }
    };

    using RowIdRanges = std::vector<RowIdRange>;

    RowIdRanges ranges;
};

using MergeTreeIndexGranuleSparsePtr = std::shared_ptr<MergeTreeIndexGranuleSparse>;


struct MergeTreeIndexAggregatorSparse final : IMergeTreeIndexAggregator
{
    // Should hold a SparseIndexStorePtr to interact with ffi.
    MergeTreeIndexAggregatorSparse(
        const String & index_name_, const Block & index_sample_block_, const String & json_parameter_, SparseIndexStorePtr store_);
    ~MergeTreeIndexAggregatorSparse() override = default;

    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
    bool empty() const override { return !granule || granule->empty(); }

    String index_name;
    Block index_sample_block;
    String json_parameter;
    SparseIndexStorePtr store;
    MergeTreeIndexGranuleSparsePtr granule;
};


class MergeTreeIndexConditionSparse final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionSparse() { }

    ~MergeTreeIndexConditionSparse() override = default;

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;
};


class MergeTreeIndexSparse : public IMergeTreeIndex
{
public:
    MergeTreeIndexSparse(const IndexDescription & index_, const String & json_parameter_)
        : IMergeTreeIndex(index_), json_parameter(json_parameter_)
    {
    }

    ~MergeTreeIndexSparse() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(SparseIndexStorePtr & store, const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG *, ContextPtr) const override;

private:
    const String json_parameter;
};


}
