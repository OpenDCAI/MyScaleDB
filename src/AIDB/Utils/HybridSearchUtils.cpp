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

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <AIDB/Utils/CommonUtils.h>
#include <AIDB/Utils/HybridSearchUtils.h>
#include <Common/logger_useful.h>

namespace DB
{

UInt8 getHybridSearchScoreType(HybridSearchFuncType search_type)
{
    switch (search_type)
    {
        case HybridSearchFuncType::VECTOR_SCAN:
            return VECTOR_SEARCH_SCORE_TYPE;
        case HybridSearchFuncType::TEXT_SEARCH:
            return TEXT_SEARCH_SCORE_TYPE;
        case HybridSearchFuncType::SPARSE_SEARCH:
            return SPARSE_SEARCH_SCORE_TYPE;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported search type in HybridSearch function");
    }
}

/// Replace limit with num_candidates that is equal to limit * hybrid_search_top_k_multiple_base
inline void replaceLimitAST(ASTPtr & ast, UInt64 replaced_limit)
{
    const auto * select_query = ast->as<ASTSelectQuery>();
    if (!select_query->limitLength())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No limit in Distributed HybridSearch AST");

    /// limit ast may be a CAST function when new analyzer is enabled.
    select_query->limitLength() = std::make_shared<ASTLiteral>(replaced_limit);
}

/// Based on the original hybrid search query AST, create two separate ASTs
/// The returned ASTs in query_asts are consistent with the original hybrid search query AST
void splitHybridSearchAST(ASTPtr & hybrid_search_ast, HybridSearchInfoPtr & hybrid_search_info, std::vector<ASTPtr> & query_asts)
{
    query_asts.resize(2);
    size_t query_ast_idx = 0;

    /// Replace the ASTFunction, ASTOrderByElement and LimitAST for Vector Search
    auto makeVectorSearchAST = [&](size_t vector_column_idx, size_t query_vector_idx)
    {
        ASTPtr vector_search_ast = hybrid_search_ast->clone();

        const auto * select_vector_query = vector_search_ast->as<ASTSelectQuery>();

        for (auto & child : select_vector_query->select()->children)
        {
            auto function = child->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                child = makeASTFunction(
                    DISTANCE_FUNCTION,
                    function->arguments->children[vector_column_idx]->clone(),
                    function->arguments->children[query_vector_idx]->clone());
            }

            auto identifier = child->as<ASTIdentifier>();
            if (!identifier)
                continue;
            else if (identifier->name() == SCORE_TYPE_COLUMN.name)
            {
                /// Delete the SCORE_TYPE_COLUMN from the select list
                select_vector_query->select()->children.erase(
                    std::remove(select_vector_query->select()->children.begin(), select_vector_query->select()->children.end(), child),
                    select_vector_query->select()->children.end());
            }
        }

        /// Replace the HybridSearch function with DISTANCE_FUNCTION in the ORDER BY
        if (!select_vector_query->orderBy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No ORDER BY in Distributed HybridSearch AST");
        for (auto & child : select_vector_query->orderBy()->children)
        {
            auto * order_by_element = child->as<ASTOrderByElement>();
            if (!order_by_element || order_by_element->children.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad ORDER BY expression AST");

            auto function = order_by_element->children.at(0)->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                order_by_element->children.at(0) = makeASTFunction(
                    DISTANCE_FUNCTION,
                    function->arguments->children[vector_column_idx]->clone(),
                    function->arguments->children[query_vector_idx]->clone());
                order_by_element->direction = hybrid_search_info->vector_scan_info->vector_scan_descs[0].direction;
            }
        }

        replaceLimitAST(vector_search_ast, hybrid_search_info->vector_scan_info->vector_scan_descs[0].topk);
        query_asts[query_ast_idx++] = vector_search_ast;
    };

    /// Replace the ASTFunction, ASTOrderByElement and LimitAST for Text Search
    auto makeTextSearchAST = [&](size_t text_column_idx, size_t query_text_idx)
    {
        ASTPtr text_search_ast = hybrid_search_ast->clone();

        const auto * select_text_query = text_search_ast->as<ASTSelectQuery>();

        auto text_search_function_parameters = std::make_shared<ASTExpressionList>();
        text_search_function_parameters->children.push_back(
            std::make_shared<ASTLiteral>("enable_nlq=" + std::to_string(hybrid_search_info->text_search_info->enable_nlq)));
        text_search_function_parameters->children.push_back(
            std::make_shared<ASTLiteral>("operator=" + hybrid_search_info->text_search_info->text_operator));

        /// Replace the HybridSearch function with TEXT_SEARCH_FUNCTION in the select list
        for (auto & child : select_text_query->select()->children)
        {
            auto function = child->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                std::shared_ptr<ASTFunction> text_search_function = makeASTFunction(
                    TEXT_SEARCH_FUNCTION,
                    function->arguments->children[text_column_idx]->clone(),
                    function->arguments->children[query_text_idx]->clone());
                text_search_function->parameters = text_search_function_parameters->clone();
                text_search_function->children.push_back(text_search_function->parameters);
                child = text_search_function;
            }

            auto identifier = child->as<ASTIdentifier>();
            if (!identifier)
                continue;
            else if (identifier->name() == SCORE_TYPE_COLUMN.name)
            {
                /// Delete the SCORE_TYPE_COLUMN from the select list
                select_text_query->select()->children.erase(
                    std::remove(select_text_query->select()->children.begin(), select_text_query->select()->children.end(), child),
                    select_text_query->select()->children.end());
            }
        }

        /// Replace the HybridSearch function with TEXT_SEARCH_FUNCTION in the ORDER BY
        if (!select_text_query->orderBy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No ORDER BY in Distributed HybridSearch AST");
        for (auto & child : select_text_query->orderBy()->children)
        {
            auto * order_by_element = child->as<ASTOrderByElement>();
            if (!order_by_element || order_by_element->children.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad ORDER BY expression AST");

            auto function = order_by_element->children.at(0)->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                std::shared_ptr<ASTFunction> text_search_function = makeASTFunction(
                    TEXT_SEARCH_FUNCTION,
                    function->arguments->children[text_column_idx]->clone(),
                    function->arguments->children[query_text_idx]->clone());
                text_search_function->parameters = text_search_function_parameters->clone();
                text_search_function->children.push_back(text_search_function->parameters);

                order_by_element->children.at(0) = text_search_function;
                order_by_element->direction = -1;
            }
        }

        replaceLimitAST(text_search_ast, hybrid_search_info->text_search_info->topk);
        query_asts[query_ast_idx++] = text_search_ast;
    };

    /// Replace the ASTFunction, ASTOrderByElement and LimitAST for Sparse Search
    auto makeSparseSearchAST = [&](size_t sparse_column_idx, size_t query_sparse_idx)
    {
        ASTPtr sparse_search_ast = hybrid_search_ast->clone();

        const auto * select_sparse_query = sparse_search_ast->as<ASTSelectQuery>();

        /// Replace the HybridSearch function with SPARSE_SEARCH_FUNCTION in the select list
        for (auto & child : select_sparse_query->select()->children)
        {
            auto function = child->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                child = makeASTFunction(
                    SPARSE_SEARCH_FUNCTION,
                    function->arguments->children[sparse_column_idx]->clone(),
                    function->arguments->children[query_sparse_idx]->clone());
            }

            auto identifier = child->as<ASTIdentifier>();
            if (!identifier)
                continue;
            else if (identifier->name() == SCORE_TYPE_COLUMN.name)
            {
                /// Delete the SCORE_TYPE_COLUMN from the select list
                select_sparse_query->select()->children.erase(
                    std::remove(select_sparse_query->select()->children.begin(), select_sparse_query->select()->children.end(), child),
                    select_sparse_query->select()->children.end());
            }
        }

        /// Replace the HybridSearch function with TEXT_SEARCH_FUNCTION in the ORDER BY
        if (!select_sparse_query->orderBy())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No ORDER BY in Distributed HybridSearch AST");
        for (auto & child : select_sparse_query->orderBy()->children)
        {
            auto * order_by_element = child->as<ASTOrderByElement>();
            if (!order_by_element || order_by_element->children.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad ORDER BY expression AST");

            auto function = order_by_element->children.at(0)->as<ASTFunction>();
            if (function && isHybridSearchFunc(function->name))
            {
                order_by_element->children.at(0) = makeASTFunction(
                    SPARSE_SEARCH_FUNCTION,
                    function->arguments->children[sparse_column_idx]->clone(),
                    function->arguments->children[query_sparse_idx]->clone());
                order_by_element->direction = -1;
            }
        }

        replaceLimitAST(sparse_search_ast, hybrid_search_info->sparse_search_info->topk);
        query_asts[query_ast_idx++] = sparse_search_ast;
    };

    for (size_t i = 0; i < hybrid_search_info->search_func_list.size(); i++)
    {
        switch (hybrid_search_info->search_func_list[i])
        {
            case HybridSearchFuncType::VECTOR_SCAN:
                makeVectorSearchAST(i, i + 2);
                break;
            case HybridSearchFuncType::TEXT_SEARCH:
                makeTextSearchAST(i, i + 2);
                break;
            case HybridSearchFuncType::SPARSE_SEARCH:
                makeSparseSearchAST(i, i + 2);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported search type in HybridSearch function");
        }
    }
}

/// RRF_score = 1.0 / (fusion_k + rank(search_mode_1)) + 1.0 / (fusion_k + rank(search_mode_2))
void RankFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & search_result_dataset_0,
    const ScoreWithPartIndexAndLabels & search_result_dataset_1,
    const UInt64 fusion_k,
    LoggerPtr log)
{
    auto processDataset = [&](const ScoreWithPartIndexAndLabels & dataset)
    {
        size_t idx = 0;

        LOG_TRACE(log, "fusion process dataset size: {}", dataset.size());
        for (const auto & score_with_label : dataset)
        {
            auto fusion_id = std::make_tuple(score_with_label.shard_num, score_with_label.part_index, score_with_label.label_id);

            /// For new (shard_num, part_index, label_id) tuple, map will insert.
            /// fusion_id_with_score map saved the fusion score for a (shard_num, part_index, label_id) tuple.
            /// AS for single-shard hybrid search, shard_num is always 0.
            auto fusion_score = 1.0f / (fusion_k + idx + 1);
            fusion_id_with_score[fusion_id] += fusion_score;
            idx++;

            LOG_TRACE(
                log,
                "fusion_id: [{}, {}, {}], ranked_score: {}",
                score_with_label.shard_num,
                score_with_label.part_index,
                score_with_label.label_id,
                fusion_score);
        }
    };

    processDataset(search_result_dataset_0);
    processDataset(search_result_dataset_1);
}

/// RSF_score = fusion_weight * normalized_score_0 + (1 - fusion_weight) * normalized_score_1
void RelativeScoreFusion(
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> & fusion_id_with_score,
    const ScoreWithPartIndexAndLabels & search_result_dataset_0,
    const ScoreWithPartIndexAndLabels & search_result_dataset_1,
    const Float32 fusion_weight_0,
    LoggerPtr log)
{
    auto processDataset = [&](const ScoreWithPartIndexAndLabels & dataset, const Float32 fusion_weight)
    {
        std::vector<Float32> norm_score;
        computeNormalizedScore(dataset, norm_score, log);

        LOG_INFO(log, "fusion process dataset size: {}", dataset.size());
        for (size_t idx = 0; idx < dataset.size(); idx++)
        {
            auto fusion_id = std::make_tuple(dataset[idx].shard_num, dataset[idx].part_index, dataset[idx].label_id);

            /// For new (shard_num, part_index, label_id) tuple, map will insert.
            /// fusion_id_with_score map saved the fusion score for a (shard_num, part_index, label_id) tuple.
            /// AS for single-shard hybrid search, shard_num is always 0.
            Float32 fusion_score = fusion_weight * norm_score[idx];

            LOG_INFO(
                log,
                "fusion_id=[{}, {}, {}], origin_score={}, norm_score={}, fusion_score={}",
                dataset[idx].shard_num,
                dataset[idx].part_index,
                dataset[idx].label_id,
                dataset[idx].score,
                norm_score[idx],
                fusion_score);

            fusion_id_with_score[fusion_id] += fusion_score;
        }
    };

    processDataset(search_result_dataset_0, fusion_weight_0);
    processDataset(search_result_dataset_1, 1 - fusion_weight_0);
}

void computeNormalizedScore(const ScoreWithPartIndexAndLabels & search_result_dataset, std::vector<Float32> & norm_score, LoggerPtr log)
{
    const auto result_size = search_result_dataset.size();
    if (result_size == 0)
    {
        LOG_DEBUG(log, "search result is empty");
        return;
    }

    norm_score.reserve(result_size);

    /// The scores in the search_result_dataset are already ordered.
    Float32 first_score = search_result_dataset[0].score;
    Float32 last_score = search_result_dataset[result_size - 1].score;

    /// Check for division by zero: when all scores are the same
    if (std::abs(first_score - last_score) < std::numeric_limits<Float32>::epsilon())
    {
        /// When all scores are the same, assign normalized score as 1.0
        for (size_t idx = 0; idx < result_size; idx++)
            norm_score.emplace_back(1.0f);
        return;
    }

    /// Calculate the range for normalization
    /// We want: first element -> 1.0, last element -> 0.0
    Float32 score_range = first_score - last_score;
    for (size_t idx = 0; idx < result_size; idx++)
    {
        /// Normalize: (current_score - last_score) / (first_score - last_score)
        /// Normalize scores: first element -> 1.0, last element -> 0.0, regardless of original order
        Float32 normalizing_score = (search_result_dataset[idx].score - last_score) / score_range;
        norm_score.emplace_back(normalizing_score);
    }
}

}
