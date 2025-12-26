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
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/IAST.h>
#include <AIDB/Storages/HybridSearchResult.h>
#include <AIDB/Storages/VSDescription.h>

namespace DB
{
using ASTPtr = std::shared_ptr<IAST>;

const String VECTOR_SEARCH_SCORE_COLUMN_NAME = "distance_func";
const String TEXT_SEARCH_SCORE_COLUMN_NAME = "textsearch_func";
const String SPARSE_SEARCH_SCORE_COLUMN_NAME = "sparsesearch_func";

const String HYBRID_SEARCH_SCORE_COLUMN_NAME = "hybridsearch_func";

/// Distributed Hybrid Search additional columns
const NameAndTypePair SCORE_TYPE_COLUMN{"_distributed_hybrid_search_score_type", std::make_shared<DataTypeUInt8>()};

const UInt8 VECTOR_SEARCH_SCORE_TYPE = 0;
const UInt8 TEXT_SEARCH_SCORE_TYPE = 1;
const UInt8 SPARSE_SEARCH_SCORE_TYPE = 2;

UInt8 getHybridSearchScoreType(HybridSearchFuncType search_type);

void splitHybridSearchAST(ASTPtr & hybrid_search_ast, HybridSearchInfoPtr & hybrid_search_info, std::vector<ASTPtr> & query_asts);

void RankFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & result_with_part_index_0,
    const ScoreWithPartIndexAndLabels & result_with_part_index_1,
    const UInt64 fusion_k,
    LoggerPtr log);

void RelativeScoreFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & result_with_part_index_0,
    const ScoreWithPartIndexAndLabels & result_with_part_index_1,
    const Float32 fusion_weight_0,
    LoggerPtr log);

void computeNormalizedScore(
    const ScoreWithPartIndexAndLabels & search_result_dataset, std::vector<Float32> & norm_score, LoggerPtr log);
}
