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

#include <base/types.h>
#include <Poco/String.h>
#include <Interpreters/Context_fwd.h>

namespace Search
{
enum class DataType;
}

namespace DB
{

const String BATCH_DISTANCE_FUNCTION = "batch_distance";
const String DISTANCE_FUNCTION = "distance";
const String TEXT_SEARCH_FUNCTION = "textsearch";
const String HYBRID_SEARCH_FUNCTION = "hybridsearch";
const String SPARSE_SEARCH_FUNCTION = "sparsesearch";

const String SCORE_COLUMN_NAME = "bm25_score";

/// Different search types
enum class HybridSearchFuncType
{
    UNKNOWN_FUNC = 0,
    VECTOR_SCAN,
    TEXT_SEARCH,
    HYBRID_SEARCH,
    SPARSE_SEARCH
};

inline String getSearchFuncName(HybridSearchFuncType type)
{
    switch (type)
    {
        case HybridSearchFuncType::VECTOR_SCAN:
            return "vector scan";
        case HybridSearchFuncType::TEXT_SEARCH:
            return "text search";
        case HybridSearchFuncType::HYBRID_SEARCH:
            return "hybrid search";
        case HybridSearchFuncType::SPARSE_SEARCH:
            return "sparse search";
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported hybrid search type: {}", type);
    }
}

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

inline bool isDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(DISTANCE_FUNCTION) == 0;
}

inline bool isBatchDistance(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(BATCH_DISTANCE_FUNCTION) == 0;
}

inline bool isVectorScanFunc(const String & func)
{
    return isDistance(func) || isBatchDistance(func);
}

inline bool isTextSearch(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(TEXT_SEARCH_FUNCTION) == 0;
}

inline bool isHybridSearch(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(HYBRID_SEARCH_FUNCTION) == 0;
}

inline bool isSparseSearch(const String & func)
{
    String func_to_low = Poco::toLower(func);
    return func_to_low.find(SPARSE_SEARCH_FUNCTION) == 0;
}

inline bool isHybridSearchFunc(const String & func)
{
    return isVectorScanFunc(func) || isTextSearch(func) || isHybridSearch(func) || isSparseSearch(func);
}

inline bool isRelativeScoreFusion(const String & fusion_type)
{
    String type = Poco::toLower(fusion_type);
    return type.find("rsf") == 0;
}

inline bool isRankFusion(const String & fusion_type)
{
    String type = Poco::toLower(fusion_type);
    return type.find("rrf") == 0;
}

inline bool isScoreColumnName(const String & col_name)
{
    return col_name == SCORE_COLUMN_NAME;
}

Search::DataType getSearchIndexDataType(DataTypePtr &data_type);

void checkVectorDimension(const Search::DataType & search_type, const uint64_t & dim);

void checkTextSearchColumnDataType(DataTypePtr &data_type, bool & is_mapKeys);

void checkSparseSearchColumnDataType(DataTypePtr & data_type);

#if USE_FTS_INDEX
void collectStatisticForBM25Calculation(ContextMutablePtr & context, String cluster_name, String database_name, String table_name, String query_column_name, String query_text);
void parseBM25StaisiticsInfo(const Block & block, size_t row, UInt64 & total_docs, std::map<UInt32, UInt64> & total_tokens_map, std::map<std::pair<UInt32, String>, UInt64> & terms_freq_map);
#endif

}
