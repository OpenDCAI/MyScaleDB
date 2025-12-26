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

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/StorageID.h>
#include <Planner/PlannerContext.h>
#include <AIDB/Storages/VSDescription.h>

namespace DB
{

/// The parameters that specify vector scan in HybridSearch() all have the same prefix.
static inline constexpr auto vector_scan_parameter_prefix = "dense_";

/// The parameters that specify sparse search in HybridSearch() all have the same prefix.
static inline constexpr auto sparse_search_parameter_prefix = "sparse_";

/** Collect hybrid search function nodes in node children.
  * Do not visit subqueries.
  */
QueryTreeNodes collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes * all_distance_funcs = nullptr);

/** Collect hybrid search function nodes in node children and add them into result.
  * Do not visit subqueries.
  */
void collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result, QueryTreeNodes * all_distance_funcs);

/** Returns true if there are hybrid search function nodes in node children, false otherwise.
  * Do not visit subqueries.
  */
bool hasHybridSearchFunctionNodes(const QueryTreeNodePtr & node);

/** Assert that there are no hybrid search function nodes in node children.
  * Do not visit subqueries.
  */
void assertNoHybridSearchFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_hybrids_place_message);

/// Analysis result for special searches: vector scan, text search, hybrid search, sparse search
struct SpecialSearchAnalysisResult
{
    VSDescriptions vector_scan_descriptions = {};
    TextSearchInfoPtr text_search_info = nullptr;
    HybridSearchInfoPtr hybrid_search_info = nullptr;
    SparseSearchInfoPtr sparse_search_info = nullptr;
    bool has_vector_scan = false;
    bool has_text_search = false;
    bool has_hybrid_search = false;
    bool has_sparse_search = false;

    QueryTreeNodeWeakPtr source_weak_pointer;
};

/// Get column source for search function
QueryTreeNodePtr getColumnSourceForSpecialSearchFunc(const QueryTreeNodePtr & node);

/// Get table storage from column source. The storage in TableNode doesn't contain remote or cluster info.
StoragePtr getTableStorageForSearchColumn(QueryTreeNodePtr query_column_source, String search_column_name, ContextPtr context);

std::optional<SpecialSearchAnalysisResult> analyzeSpecialSearch(
    const QueryTreeNodePtr & query_tree,
    const ContextPtr & context);

}
