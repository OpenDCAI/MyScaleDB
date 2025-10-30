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

#include <pdqsort.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <AIDB/Utils/VIUtils.h>
#include <AIDB/Utils/VSUtils.h>

#include <AIDB/Common/VICommon.h>
#include <AIDB/Interpreters/parseVSParameters.h>

#if USE_FTS_INDEX
#   include <AIDB/Storages/MergeTreeIndexTantivy.h>
#endif

#if USE_SPARSE_INDEX
#    include <AIDB/Storages/MergeTreeIndexSparse.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TEXT_SEARCH;
    extern const int ILLEGAL_SPARSE_SEARCH;
}

void filterMarkRangesByVectorScanResult(MergeTreeData::DataPartPtr part, MergeTreeBaseSearchManagerPtr base_search_mgr, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesByVectorScanResult()");
    MarkRanges res;

    if (!base_search_mgr || !base_search_mgr->preComputed())
    {
        mark_ranges = res;
        return;
    }

    filterMarkRangesBySearchResult(part, base_search_mgr->getSettings(), base_search_mgr->getSearchResult(), mark_ranges);
}

void filterMarkRangesBySearchResult(MergeTreeData::DataPartPtr part, const Settings & settings, CommonSearchResultPtr common_search_result, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesBySearchResult()");
    MarkRanges res;

    if (!common_search_result || !common_search_result->computed)
    {
        mark_ranges = res;
        return;
    }

    size_t marks_count = part->index_granularity.getMarksCount();
    /// const auto & index = part->index;
    /// marks_count should not be 0 if we reach here

    size_t min_marks_for_seek = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_seek,
        settings.merge_tree_min_bytes_for_seek,
        part->index_granularity_info.fixed_index_granularity,
        part->index_granularity_info.index_granularity_bytes);

    auto need_this_range = [&](MarkRange & range)
    {
        auto begin = range.begin;
        auto end = range.end;
        auto start_row = part->index_granularity.getMarkStartingRow(begin);
        auto end_row = start_row + part->index_granularity.getRowsCountInRange(range);

        const ColumnUInt32 * label_column
            = checkAndGetColumn<ColumnUInt32>(common_search_result->result_columns[0].get());
        for (size_t ind = 0; ind < label_column->size(); ++ind)
        {
            auto label = label_column->getUInt(ind);
            if (label >= start_row && label < end_row)
            {
                LOG_TRACE(
                    getLogger("MergeTreeVectorScanUtils"),
                    "Keep range: {}-{} in part: {}",
                    begin,
                    end,
                    part->name);
                return true;
            }
        }
        return false;
    };

    std::vector<MarkRange> ranges_stack = {{0, marks_count}};

    while (!ranges_stack.empty())
    {
        MarkRange range = ranges_stack.back();
        ranges_stack.pop_back();

        if (!need_this_range(range))
            continue;

        if (range.end == range.begin + 1)
        {
            if (res.empty() || range.begin - res.back().end > min_marks_for_seek)
                res.push_back(range);
            else
                res.back().end = range.end;
        }
        else
        {
            /// Break the segment and put the result on the stack from right to left.
            size_t step = (range.end - range.begin - 1) / settings.merge_tree_coarse_index_granularity + 1;
            size_t end;

            for (end = range.end; end > range.begin + step; end -= step)
                ranges_stack.emplace_back(end - step, end);

            ranges_stack.emplace_back(range.begin, end);
        }
    }

    mark_ranges = res;
}

void filterMarkRangesByLabels(MergeTreeData::DataPartPtr part, const Settings & settings, const std::set<UInt64> labels, MarkRanges & mark_ranges)
{
    OpenTelemetry::SpanHolder span("filterMarkRangesByLabels()");
    MarkRanges res;

    size_t marks_count = part->index_granularity.getMarksCount();
    /// marks_count should not be 0 if we reach here
    if (marks_count == 0)
        mark_ranges = res;

    size_t min_marks_for_seek = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
        settings.merge_tree_min_rows_for_seek,
        settings.merge_tree_min_bytes_for_seek,
        part->index_granularity_info.fixed_index_granularity,
        part->index_granularity_info.index_granularity_bytes);

    std::vector<UInt64> labels_vec(labels.size());
    for (const auto & label : labels)
        labels_vec.emplace_back(label);

    auto need_this_range = [&](MarkRange & range)
    {
        auto begin = range.begin;
        auto end = range.end;
        auto start_row = part->index_granularity.getMarkStartingRow(begin);
        auto end_row = start_row + part->index_granularity.getRowsCountInRange(range);

        /// Use binary search due to labels are sorted
        size_t low = 0;
        size_t high = labels_vec.size();
        while (low < high)
        {
            const size_t middle = low + (high - low) / 2;
            auto label_middle = labels_vec[middle];
            if (label_middle >= start_row && label_middle < end_row)
            {
                LOG_TRACE(
                    getLogger("filterMarkRangesByLabels"),
                    "Keep range: {}-{} in part: {}",
                    begin,
                    end,
                    part->name);
                return true;
            }
            else if (label_middle < start_row)
                low = middle + 1;
            else
                high = middle;
        }
        return false;
    };

    std::vector<MarkRange> ranges_stack = {{0, marks_count}};

    while (!ranges_stack.empty())
    {
        MarkRange range = ranges_stack.back();
        ranges_stack.pop_back();

        if (!need_this_range(range))
            continue;

        if (range.end == range.begin + 1)
        {
            if (res.empty() || range.begin - res.back().end > min_marks_for_seek)
                res.push_back(range);
            else
                res.back().end = range.end;
        }
        else
        {
            /// Break the segment and put the result on the stack from right to left.
            size_t step = (range.end - range.begin - 1) / settings.merge_tree_coarse_index_granularity + 1;
            size_t end;

            for (end = range.end; end > range.begin + step; end -= step)
                ranges_stack.emplace_back(end - step, end);

            ranges_stack.emplace_back(range.begin, end);
        }
    }

    mark_ranges = res;
}

UInt64 getTopKFromLimit(const ASTSelectQuery * select_query, ContextPtr context, bool is_batch)
{
    UInt64 topk = 0;

    if (!select_query)
        return topk;

    /// topk for search is sum of length and offset in limit
    UInt64 length = 0, offset = 0;
    ASTPtr length_ast = nullptr;
    ASTPtr offset_ast = nullptr;

    if (is_batch)
    {
        /// LIMIT m OFFSET n BY expressions
        length_ast = select_query->limitByLength();
        offset_ast = select_query->limitByOffset();
    }
    else
    {
        /// LIMIT m OFFSET n
        length_ast = select_query->limitLength();
        offset_ast = select_query->limitOffset();
    }

    if (length_ast)
    {
        const auto & [field, type] = evaluateConstantExpression(length_ast, context);

        if (isNativeNumber(type))
        {
            Field converted = convertFieldToType(field, DataTypeUInt64());
            if (!converted.isNull())
                length = converted.safeGet<UInt64>();
        }
    }

    if (offset_ast)
    {
        const auto & [field, type] = evaluateConstantExpression(offset_ast, context);

        if (isNativeNumber(type))
        {
            Field converted = convertFieldToType(field, DataTypeUInt64());
            if (!converted.isNull())
                offset = converted.safeGet<UInt64>();
        }
    }

    topk = length + offset;

    /// Check when offset n is provided
    if (offset > 0 && topk > context->getSettingsRef().max_search_result_window)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sum of m and n in limit ({}) should not exceed `max_search_result_window`({})", topk, context->getSettingsRef().max_search_result_window);

    return topk;
}

UInt64 getTopKFromLimit(const QueryTreeNodePtr & query_tree, const ContextPtr & context)
{
    UInt64 topk = 0;

    auto & query_node = query_tree->as<QueryNode &>();

    /// topk for search is sum of length and offset in limit
    UInt64 length = 0, offset = 0;

    /// LIMIT m OFFSET n
    if (query_node.hasLimit())
    {
        /// Constness of limit is validated during query analysis stage
        length = query_node.getLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();

        if (query_node.hasOffset())
            offset = query_node.getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();
    }
    else if (query_node.hasLimitByLimit()) /// LIMIT m OFFSET n BY expressions
    {
        length = query_node.getLimitByLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();

        if (query_node.hasLimitByOffset())
            offset = query_node.getLimitByOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();
    }

    topk = length + offset;
    auto max_topk = context->getSettingsRef().max_search_result_window;

    /// Check when offset n is provided
    if (offset > 0 && topk > max_topk)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sum of m and n in limit ({}) should not exceed `max_search_result_window`({})", topk, max_topk);

    return topk;
}

AIDB::VIMetric getVSMetric(MergeTreeData::DataPartPtr part, const VSDescription & desc)
{
    String metric_name = "L2";
    bool found = false;
    auto metadata_snapshot = part->storage.getInMemoryMetadataPtr();
    for (const auto & vi_desc : metadata_snapshot->getVectorIndices())
    {
        /// Support multiple vector indices on a single vector column
        if (vi_desc.name == desc.search_index_name)
        {
            if (vi_desc.parameters && vi_desc.parameters->has("metric_type"))
            {
                found = true;
                metric_name = vi_desc.parameters->getValue<String>("metric_type");
            }
            break;
        }
    }
    if (!found)
    {
        if (desc.vector_search_type == Search::DataType::FloatVector)
            metric_name = part->storage.getSettings()->float_vector_search_metric_type;
        else
            metric_name = part->storage.getSettings()->binary_vector_search_metric_type;
    }
    return Search::getMetricType(metric_name, desc.vector_search_type);
}

/// Get metric type based on vector index name, allow multiple vector indices on a single vector column
String getMetricType(const StorageMetadataPtr & metadata_snapshot, Search::DataType & vector_search_type, String & vec_index_name, ContextPtr context)
{
    /// The default value is float_vector_search_metric_type or binary_vector_search_metric_type in MergeTree, but we cannot get it here.
    String metric_type;

    if (metadata_snapshot)
    {
        /// Try to get from parameters of vector index on search vector column
        for (const auto & vector_index_desc : metadata_snapshot->getVectorIndices())
        {
            if (vector_index_desc.name == vec_index_name)
            {
                /// Get metric_type in index definition
                if (vector_index_desc.parameters && vector_index_desc.parameters->has("metric_type"))
                    metric_type = vector_index_desc.parameters->getValue<String>("metric_type");

                break;
            }
        }

        /// Try to get from storage settings in create table
        if (metric_type.empty() && metadata_snapshot->hasSettingsChanges())
        {
            const auto settings_changes = metadata_snapshot->getSettingsChanges()->as<const ASTSetQuery &>().changes;
            Field change_metric;
            /// TODO: Try not to use string literals directly
            if ((vector_search_type == Search::DataType::FloatVector && settings_changes.tryGet("float_vector_search_metric_type", change_metric)) ||
                (vector_search_type == Search::DataType::BinaryVector && settings_changes.tryGet("binary_vector_search_metric_type", change_metric)))
            {
                metric_type = change_metric.safeGet<String>();
            }
        }
    }

    /// Try to get from merge tree settings in context
    if (metric_type.empty())
    {
        const auto settings = context->getMergeTreeSettings();
        if (vector_search_type == Search::DataType::FloatVector)
            metric_type = settings.float_vector_search_metric_type.toString();
        else if (vector_search_type == Search::DataType::BinaryVector)
            metric_type = settings.binary_vector_search_metric_type.toString();
    }

    return metric_type;
}

void checkOrderBySortDirection(String func_name, const ASTSelectQuery * select_query, int expected_direction, bool is_batch)
{
    if (!select_query)
        return;

    auto order_by = select_query->orderBy();
    if (!order_by)
        return; /// order by is already checked and handled by getHybridSearchFunctions()

    int sort_direction = 0;
    bool find_search_function = false;

    /// Find the direction for hybrid search func
    for (const auto & child : order_by->children)
    {
        auto * order_by_element = child->as<ASTOrderByElement>();
        if (!order_by_element || order_by_element->children.empty())
            continue;
        ASTPtr order_expression = order_by_element->children.at(0);

        if (!is_batch)
        {
            /// Check cases when search function column is an argument of other functions
            if (isHybridSearchFunc(order_expression->getColumnName()))
            {
                sort_direction = order_by_element->direction;
                find_search_function = true;
                break;
            }
            else if (auto * function = order_expression->as<ASTFunction>())
            {
                const ASTs & func_arguments = function->arguments->as<ASTExpressionList &>().children;
                for (const auto & func_arg : func_arguments)
                {
                    if (isHybridSearchFunc(func_arg->getColumnName()))
                    {
                        sort_direction = order_by_element->direction;
                        find_search_function = true;
                        break;
                    }
                }
            }
        }
        else
        {
            /// order by batch_distance column name's 1 and 2, where 2 is distance column.
            if (auto * function = order_expression->as<ASTFunction>(); function && function->name == "tupleElement")
            {
                const ASTs & func_arguments = function->arguments->as<ASTExpressionList &>().children;
                if (func_arguments.size() >= 2 && isBatchDistance(func_arguments[0]->getColumnName()))
                {
                    if (func_arguments[1]->getColumnName() == "2")
                    {
                        sort_direction = order_by_element->direction;
                        find_search_function = true;
                        break;
                    }
                }
            }
        }
   }

    /// Only check the direction of search function when found in order by
    if (find_search_function && sort_direction != expected_direction)
    {
        String vector_scan_error_log = " when the metric type is ";
        if (expected_direction == -1) /// 1 - ascending, -1 - descending
            vector_scan_error_log = "`DESC`" + vector_scan_error_log +  "IP";
        else
            vector_scan_error_log = "`ASC`" + vector_scan_error_log +  "not IP";

        throw Exception(ErrorCodes::SYNTAX_ERROR,
                "The results returned by the {} function should be ordered by {}",
                func_name, vector_scan_error_log);
    }
}

void checkOrderBySortDirection(String func_name, const QueryTreeNodePtr & query_tree, SortDirection expected_direction, bool is_batch)
{
    auto * query_node_typed = query_tree->as<QueryNode>();

    if (!query_node_typed || !query_node_typed->hasOrderBy())
        return;

    SortDirection sort_direction = SortDirection::ASCENDING;
    bool find_search_function = false;

    /// Find the direction for search function
    for (auto & node : query_node_typed->getOrderBy().getNodes())
    {
        auto & sort_node = node->as<SortNode &>();
        auto * function_expression = sort_node.getExpression()->as<FunctionNode>();
        if (!function_expression)
            continue;

        String sort_func_name = function_expression->getFunctionName();

        /// checkOrderBySortDirection()
        if (!is_batch)
        {
            if (function_expression->isSpecialSearchFunction())
            {
                sort_direction = sort_node.getSortDirection();
                find_search_function = true;
                break;
            }
            else /// Check cases when search function column is an argument of other functions
            {
                for(const auto & arg: function_expression->getArguments().getNodes())
                {
                    if (auto * func_node = arg->as<FunctionNode>(); func_node && func_node->isSpecialSearchFunction())
                    {
                        sort_direction = sort_node.getSortDirection();
                        find_search_function = true;
                        break;
                    }
                }
            }
        }
        else /// is_batch
        {
            /// order by batch_distance column name's 1 and 2, where 2 is distance column.
            if (sort_func_name == "tupleElement")
            {
                const auto & args = function_expression->getArguments().getNodes();
                if (args.size() == 2 && args[0]->as<FunctionNode>() && isBatchDistance(args[0]->as<FunctionNode>()->getFunctionName()))
                {
                    auto * constant = args[1]->as<ConstantNode>();
                    if (constant && constant->getValue().safeGet<UInt8>() == 2)
                    {
                        sort_direction = sort_node.getSortDirection();
                        find_search_function = true;
                        break;
                    }
                }
            }
        }
    }

    /// Only check the direction of search function when found in order by
    if (find_search_function && sort_direction != expected_direction)
    {
        String vector_scan_error_log = " when the metric type is ";
        if (expected_direction == SortDirection::DESCENDING)
            vector_scan_error_log = "`DESC`" + vector_scan_error_log +  "IP";
        else
            vector_scan_error_log = "`ASC`" + vector_scan_error_log +  "not IP";

        throw Exception(ErrorCodes::SYNTAX_ERROR,
                "The results returned by the {} function should be ordered by {}",
                func_name, vector_scan_error_log);
    }
}

void checkTantivyIndex([[maybe_unused]]const StorageMetadataPtr & metadata_snapshot, [[maybe_unused]]const String & text_column_name)
{
    bool find_tantivy_index = false;
#if USE_FTS_INDEX
    if (metadata_snapshot)
    {
        for (const auto & index_desc : metadata_snapshot->getSecondaryIndices())
        {
            /// Find tantivy inverted index on the search column
            if (index_desc.type == TANTIVY_INDEX_NAME)
            {
                auto & column_names = index_desc.column_names;
                /// Support search on a column in a multi-columns index
                if (std::find(column_names.begin(), column_names.end(), text_column_name) != column_names.end())
                {
                    find_tantivy_index = true;
                    break;
                }
            }
        }
    }
#endif
    if (!find_tantivy_index)
    {
        throw Exception(ErrorCodes::ILLEGAL_TEXT_SEARCH, "The column {} has no fts index for text search", text_column_name);
    }
}

void checkSparseIndex([[maybe_unused]] const StorageMetadataPtr & metadata_snapshot, [[maybe_unused]] const String & sparse_column_name)
{
    bool find_sparse_index = false;
#if USE_SPARSE_INDEX
    if (metadata_snapshot)
    {
        for (const auto & index_desc : metadata_snapshot->getSecondaryIndices())
        {
            /// Find sparse index on the search column
            if (index_desc.type == SPARSE_INDEX_NAME && index_desc.column_names.size() == 1 && index_desc.column_names[0] == sparse_column_name)
            {
                find_sparse_index = true;
                break;
            }
        }
    }
#endif
    if (!find_sparse_index)
    {
        throw Exception(ErrorCodes::ILLEGAL_SPARSE_SEARCH, "The column {} has no sparse index for sparse search", sparse_column_name);
    }
}

std::pair<String, bool> getVectorIndexTypeAndParameterCheck(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, String & search_column_name, String & search_index_name)
{
    auto log = getLogger("getVectorIndexTypeAndParameterCheck");
    String index_type = "";
    /// Obtain the default value of the `use_parameter_check` in the MergeTreeSetting.
    std::unique_ptr<MergeTreeSettings> storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    bool use_parameter_check = storage_settings->vector_index_parameter_check;
    LOG_TRACE(log, "vector_index_parameter_check value in MergeTreeSetting: {}", use_parameter_check);

    /// Obtain the type of the vector index recorded in the meta_data.
    if (metadata_snapshot)
    {
        /// Support multiple vector indices on a vector column
        /// If search index name is specified, find the vector index description by the index name
        if (!search_index_name.empty())
        {
            bool search_index_exists = false;
            for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
            {
                if (vec_index_desc.name == search_index_name)
                {
                    /// Valid check for search index name and column name
                    if (vec_index_desc.column == search_column_name)
                    {
                        index_type = vec_index_desc.type;
                        search_index_exists = true;
                        break;
                    }
                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The vector column `{}` doesn't have a vector index with name `{}`",
                                            search_column_name, search_index_name);
                }
            }

            if (!search_index_exists)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The vector index with name `{}` doesn't exist", search_index_name);
        }
        else
        {
            /// Find vector index description in metadata based on search column name.
            /// If not specified (search_index_name is empty), use the most recently created vector index.
            const auto & vec_indices = metadata_snapshot->getVectorIndices();
            for (auto it = vec_indices.rbegin(); it != vec_indices.rend(); ++it)
            {
                if (it->column == search_column_name)
                {
                    search_index_name = it->name; /// Save vector index name
                    index_type = it->type;
                    break;
                }
            }
        }

        if (!index_type.empty())
            LOG_TRACE(log, "The vector index type used for the query is `{}`", Poco::toUpper(index_type));

        /// If not found, brute force search will be used.
    }

    /// Use the user-defined `vector_index_parameter_check`.
    if (metadata_snapshot && metadata_snapshot->hasSettingsChanges())
    {
        const auto current_changes = metadata_snapshot->getSettingsChanges()->as<const ASTSetQuery &>().changes;
        for (const auto & changed_setting : current_changes)
        {
            const auto & setting_name = changed_setting.name;
            const auto & new_value = changed_setting.value;
            if (setting_name == "vector_index_parameter_check")
            {
                use_parameter_check = new_value.safeGet<bool>();
                LOG_TRACE(
                    log, "vector_index_parameter_check value in sql definition: {}", use_parameter_check);
                break;
            }
        }
    }

    return std::make_pair(index_type, use_parameter_check);
}

void getAndCheckVectorScanInfoFromMetadata(
    const StorageMetadataPtr & metadata_snapshot,
    VSDescription & vector_scan_desc,
    ContextPtr context)
{
    if (metadata_snapshot)
    {
        /// vector column dim
        vector_scan_desc.search_column_dim = AIDB::getVectorDimension(vector_scan_desc.vector_search_type, *metadata_snapshot, vector_scan_desc.search_column_name);
        checkVectorDimension(vector_scan_desc.vector_search_type, vector_scan_desc.search_column_dim);

        /// Parameter check
        std::pair<String, bool> res = getVectorIndexTypeAndParameterCheck(metadata_snapshot, context, vector_scan_desc.search_column_name, vector_scan_desc.search_index_name);

        /// parse vector scan's params, such as: top_k, n_probe ...
        String param_str = parseVectorScanParameters(vector_scan_desc.parameters, Poco::toUpper(res.first), res.second);
        if (!param_str.empty())
        {
            try
            {
                Poco::JSON::Parser json_parser;
                vector_scan_desc.vector_parameters = json_parser.parse(param_str).extract<Poco::JSON::Object::Ptr>();
            }
            catch ([[maybe_unused]] const std::exception & e)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The input JSON's format is illegal");
            }
        }

        /// Pass the correct direction to vector_scan_desc according to metric_type
        String vector_scan_metric_type = getMetricType(metadata_snapshot, vector_scan_desc.vector_search_type, vector_scan_desc.search_index_name, context);
        vector_scan_desc.direction = Poco::toUpper(vector_scan_metric_type) == "IP" ? -1 : 1;
    }
}

}
