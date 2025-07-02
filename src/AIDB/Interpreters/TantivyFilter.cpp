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

#include <algorithm>
#include <string>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Disks/DiskLocal.h>
#include <AIDB/Interpreters/TantivyFilter.h>
#include <Storages/MergeTree/MergeTreeIndexFullText.h>
#include <AIDB/Store/TantivyIndexStore.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

TantivyFilter::TantivyFilter(const TantivyFilterParameters & params_) : params(params_), query_terms()
{
}

void TantivyFilter::addRowRangeToTantivyFilter(UInt64 rowIDStart, UInt64 rowIDEnd)
{
    if (!rowid_ranges.empty())
    {
        TantivyRowIdRange & last_rowid_range = rowid_ranges.back();
        if (last_rowid_range.range_end + 1 == rowIDStart)
        {
            last_rowid_range.range_end = rowIDEnd;
            return;
        }
    }
    rowid_ranges.push_back({rowIDStart, rowIDEnd});
}

void TantivyFilter::addRowRangeToTantivyFilter(UInt32 rowIDStart, UInt32 rowIDEnd)
{
    addRowRangeToTantivyFilter(static_cast<UInt64>(rowIDStart), static_cast<UInt64>(rowIDEnd));
}

void TantivyFilter::clear()
{
    query_term.clear();
    query_terms.clear();
    rowid_ranges.clear();
    this->setQueryType(QueryType::UNSPECIFIC_QUERY);
}
}
