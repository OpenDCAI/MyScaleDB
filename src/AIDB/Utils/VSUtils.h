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
#include <Analyzer/SortNode.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <AIDB/Storages/MergeTreeBaseSearchManager.h>
#include <AIDB/Common/VICommon.h>

namespace DB
{

/// if we has precompute search result, use it to filter mark ranges
void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeBaseSearchManagerPtr base_search_mgr, MarkRanges & mark_ranges);

/// if we has precompute search result, use it to filter mark ranges
void filterMarkRangesBySearchResult(MergeTreeData::DataPartPtr part, const Settings & settings, CommonSearchResultPtr common_search_result, MarkRanges & mark_ranges);

/// if we has labels from precompute search result, use it to filter mark ranges
void filterMarkRangesByLabels(MergeTreeData::DataPartPtr part, const Settings & settings, const std::set<UInt64> labels, MarkRanges & mark_ranges);

/// get topk from limit clause
UInt64 getTopKFromLimit(const ASTSelectQuery * select_query, ContextPtr context, bool is_batch = false);

/// support QueryTreeNode
UInt64 getTopKFromLimit(const QueryTreeNodePtr & query_tree, const ContextPtr & context);

AIDB::VIMetric getVSMetric(MergeTreeData::DataPartPtr part, const VSDescription & desc);

String getMetricType(const StorageMetadataPtr & metadata_snapshot, Search::DataType & vector_search_type, String & vec_index_name, ContextPtr context);

void checkOrderBySortDirection(String func_name, const ASTSelectQuery * select_query, int expected_direction, bool is_batch = false);

/// support QueryTreeNode (new analyzer)
void checkOrderBySortDirection(String func_name, const QueryTreeNodePtr & query_tree, SortDirection expected_direction, bool is_batch = false);

void checkTantivyIndex(const StorageMetadataPtr & metadata_snapshot, const String & text_column_name);
void checkSparseIndex(const StorageMetadataPtr & metadata_snapshot, const String & sparse_column_name);

std::pair<String, bool> getVectorIndexTypeAndParameterCheck(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, String & search_column_name, String & search_index_name);

/// Fill in dim and recognize VectorSearchType from metadata
void getAndCheckVectorScanInfoFromMetadata(
    const StorageMetadataPtr & metadata_snapshot,
    VSDescription & vector_scan_desc,
    ContextPtr context);

}
