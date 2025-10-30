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

#include <AIDB/Processors/HybridSearchFusionTransform.h>
#include <AIDB/Storages/HybridSearchResult.h>
#include <AIDB/Utils/HybridSearchUtils.h>

namespace DB
{

Chunk HybridSearchFusionTransform::generate()
{
    if (chunks.empty())
        return {};

    Chunk merged_chunk = std::move(chunks.front());
    chunks.pop();
    while (!chunks.empty())
    {
        merged_chunk.append(std::move(chunks.front()));
        chunks.pop();
    }

    auto merged_chunk_columns = merged_chunk.getColumns();
    auto total_rows = merged_chunk.getNumRows();

    /// Row range is stored in the following format: {start_index, length}
    /// [vector_scan_row_range, text_search_row_range, sparse_search_row_range]
    std::array<std::pair<size_t, size_t>, 3> score_row_range{{{0, 0}, {0, 0}, {0, 0}}};

    /// Calculate the fusion score for each score type
    std::map<std::tuple<UInt32, UInt64, UInt64>, Float32> fusion_id_with_score;

    auto & merged_score_column = assert_cast<const ColumnFloat32 &>(*merged_chunk_columns[score_column_pos]);
    auto & merged_score_type_column = assert_cast<const ColumnUInt8 &>(*merged_chunk_columns[score_type_column_pos]);

    auto & merged_shard_num_column = assert_cast<const ColumnUInt32 &>(*merged_chunk_columns[fusion_shard_num_pos]);
    auto & merged_part_index_column = assert_cast<const ColumnUInt64 &>(*merged_chunk_columns[fusion_part_index_pos]);
    auto & merged_part_offset_column = assert_cast<const ColumnUInt64 &>(*merged_chunk_columns[fusion_part_offset_pos]);

    /// Get the score range for each score type
    {
        /// The merged chunk is sorted by score type, so we can find the gap between each score type
        for (size_t row = 0; row < total_rows; ++row)
        {
            if (merged_score_type_column.get64(row) == VECTOR_SEARCH_SCORE_TYPE)
            {
                if (score_row_range[VECTOR_SEARCH_SCORE_TYPE].second == 0)
                    score_row_range[VECTOR_SEARCH_SCORE_TYPE].first = row;
                score_row_range[VECTOR_SEARCH_SCORE_TYPE].second++;
            }
            else if (merged_score_type_column.get64(row) == TEXT_SEARCH_SCORE_TYPE)
            {
                if (score_row_range[TEXT_SEARCH_SCORE_TYPE].second == 0)
                    score_row_range[TEXT_SEARCH_SCORE_TYPE].first = row;
                score_row_range[TEXT_SEARCH_SCORE_TYPE].second++;
            }
            else if (merged_score_type_column.get64(row) == SPARSE_SEARCH_SCORE_TYPE)
            {
                if (score_row_range[SPARSE_SEARCH_SCORE_TYPE].second == 0)
                    score_row_range[SPARSE_SEARCH_SCORE_TYPE].first = row;
                score_row_range[SPARSE_SEARCH_SCORE_TYPE].second++;
            }
        }

        if (score_row_range[VECTOR_SEARCH_SCORE_TYPE].second > num_candidates)
        {
            if (vector_scan_order_direction == 1)
            {
                score_row_range[VECTOR_SEARCH_SCORE_TYPE].first = score_row_range[VECTOR_SEARCH_SCORE_TYPE].second - num_candidates;
            }
            score_row_range[VECTOR_SEARCH_SCORE_TYPE].second = num_candidates;
        }

        score_row_range[TEXT_SEARCH_SCORE_TYPE].second = std::min(score_row_range[TEXT_SEARCH_SCORE_TYPE].second, num_candidates);
        score_row_range[SPARSE_SEARCH_SCORE_TYPE].second = std::min(score_row_range[SPARSE_SEARCH_SCORE_TYPE].second, num_candidates);

        LOG_DEBUG(log, "vector_scan_row_range, start index: {}, count: {}", score_row_range[VECTOR_SEARCH_SCORE_TYPE].first, score_row_range[VECTOR_SEARCH_SCORE_TYPE].second);
        LOG_DEBUG(log, "text_search_row_range, start index: {}, count: {}", score_row_range[TEXT_SEARCH_SCORE_TYPE].first, score_row_range[TEXT_SEARCH_SCORE_TYPE].second);
        LOG_DEBUG(log, "sparse_search_row_range, start index: {}, count: {}", score_row_range[SPARSE_SEARCH_SCORE_TYPE].first, score_row_range[SPARSE_SEARCH_SCORE_TYPE].second);
    }

    /// Calculate the fusion score for each score type
    {
        ScoreWithPartIndexAndLabels score_datasets[3];
        score_datasets[VECTOR_SEARCH_SCORE_TYPE].reserve(score_row_range[VECTOR_SEARCH_SCORE_TYPE].second);
        score_datasets[TEXT_SEARCH_SCORE_TYPE].reserve(score_row_range[TEXT_SEARCH_SCORE_TYPE].second);
        score_datasets[SPARSE_SEARCH_SCORE_TYPE].reserve(score_row_range[SPARSE_SEARCH_SCORE_TYPE].second);

        for (size_t offset = 0; offset < score_row_range[VECTOR_SEARCH_SCORE_TYPE].second; ++offset)
        {
            size_t row;
            if (vector_scan_order_direction == -1)
                row = score_row_range[VECTOR_SEARCH_SCORE_TYPE].first + offset;
            else
                row = score_row_range[VECTOR_SEARCH_SCORE_TYPE].first + score_row_range[VECTOR_SEARCH_SCORE_TYPE].second - 1 - offset;

            score_datasets[VECTOR_SEARCH_SCORE_TYPE].emplace_back(
                merged_score_column.getFloat32(row),
                merged_part_index_column.get64(row),
                merged_part_offset_column.get64(row),
                merged_shard_num_column.get64(row));

            LOG_TRACE(log, "vector_scan_score_dataset: {}", score_datasets[VECTOR_SEARCH_SCORE_TYPE].back().dump());
        }
        for (size_t offset = 0; offset < score_row_range[TEXT_SEARCH_SCORE_TYPE].second; ++offset)
        {
            size_t row = score_row_range[TEXT_SEARCH_SCORE_TYPE].first + offset;
            score_datasets[TEXT_SEARCH_SCORE_TYPE].emplace_back(
                merged_score_column.getFloat32(row),
                merged_part_index_column.get64(row),
                merged_part_offset_column.get64(row),
                merged_shard_num_column.get64(row));

            LOG_TRACE(log, "text_search_score_dataset: {}", score_datasets[TEXT_SEARCH_SCORE_TYPE].back().dump());
        }
        for (size_t offset = 0; offset < score_row_range[SPARSE_SEARCH_SCORE_TYPE].second; ++offset)
        {
            size_t row = score_row_range[SPARSE_SEARCH_SCORE_TYPE].first + offset;
            score_datasets[SPARSE_SEARCH_SCORE_TYPE].emplace_back(
                merged_score_column.getFloat32(row),
                merged_part_index_column.get64(row),
                merged_part_offset_column.get64(row),
                merged_shard_num_column.get64(row));

            LOG_TRACE(log, "sparse_search_score_dataset: {}", score_datasets[SPARSE_SEARCH_SCORE_TYPE].back().dump());
        }

        /// Calculate the fusion score
        if (fusion_type == HybridSearchFusionType::RSF)
        {
            RelativeScoreFusion(
                fusion_id_with_score,
                score_datasets[getHybridSearchScoreType(search_func_list[0])],
                score_datasets[getHybridSearchScoreType(search_func_list[1])],
                fusion_weight,
                log);
        }
        else if (fusion_type == HybridSearchFusionType::RRF)
        {
            RankFusion(
                fusion_id_with_score,
                score_datasets[getHybridSearchScoreType(search_func_list[0])],
                score_datasets[getHybridSearchScoreType(search_func_list[1])],
                fusion_k,
                log);
        }
    }

    auto fusion_result_columns = merged_chunk.cloneEmptyColumns();
    auto & result_score_column = assert_cast<ColumnFloat32 &>(*fusion_result_columns[score_column_pos]);

    for (size_t i = 0; i < score_row_range.size(); ++i)
    {
        if (score_row_range[i].second > 0)
        {
            for (size_t offset = 0; offset < score_row_range[i].second; ++offset)
            {
                size_t row = score_row_range[i].first + offset;
                auto fusion_id = std::make_tuple(
                    static_cast<UInt32>(merged_shard_num_column.get64(row)),
                    merged_part_index_column.get64(row),
                    merged_part_offset_column.get64(row));

                if (fusion_id_with_score.find(fusion_id) != fusion_id_with_score.end())
                {
                    for (size_t position = 0; position < merged_chunk_columns.size(); ++position)
                    {
                        if (position == score_column_pos)
                        {
                            fusion_result_columns[position]->insert(fusion_id_with_score[fusion_id]);
                        }
                        else
                        {
                            fusion_result_columns[position]->insertFrom(*merged_chunk_columns[position], row);
                        }
                    }
                    fusion_id_with_score.erase(fusion_id);
                }
            }
        }
    }

    merged_chunk.setColumns(std::move(fusion_result_columns), result_score_column.size());
    return merged_chunk;
}

}
