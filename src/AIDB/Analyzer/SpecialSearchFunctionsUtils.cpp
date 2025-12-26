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

#include <AIDB/Analyzer/SpecialSearchFunctionsUtils.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/TableNode.h>
#include <Core/Settings.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/Utils.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageDistributed.h>
#include <AIDB/Utils/CommonUtils.h>
#include <AIDB/Utils/VIUtils.h>
#include <AIDB/Utils/VSUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_AGGREGATION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_HYBRID_SEARCH;
    extern const int ILLEGAL_VECTOR_SCAN;
    extern const int SYNTAX_ERROR;
}

namespace
{

class CollectHybridSearchFunctionNodesVisitor : public ConstInDepthQueryTreeVisitor<CollectHybridSearchFunctionNodesVisitor>
{
public:
    explicit CollectHybridSearchFunctionNodesVisitor(QueryTreeNodes * hybrid_function_nodes_, QueryTreeNodes * all_multiple_distance_funcs_ = nullptr)
        : hybrid_function_nodes(hybrid_function_nodes_)
        , all_multiple_distance_funcs(all_multiple_distance_funcs_)
    {}

    explicit CollectHybridSearchFunctionNodesVisitor(String assert_no_hybrids_place_message_)
        : assert_no_hybrids_place_message(std::move(assert_no_hybrids_place_message_))
    {}

    explicit CollectHybridSearchFunctionNodesVisitor(bool only_check_)
        : only_check(only_check_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (only_check && has_hybrid_search_functions)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isSpecialSearchFunction())
            return;

        if (!assert_no_hybrids_place_message.empty())
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Hybrid search function {} is found {} in query",
                function_node->formatASTForErrorMessage(),
                assert_no_hybrids_place_message);

        String full_name = function_node->formatASTForErrorMessage();
        if (uniq_name_with_alias_map.count(full_name))
        {
            if (!function_node->hasAlias())
            {
                function_node->setAlias(uniq_name_with_alias_map[full_name]);

                /// Save all existing distance funcs
                if (isDistance(function_node->getFunctionName()))
                {
                    if (all_multiple_distance_funcs)
                        all_multiple_distance_funcs->push_back(node);
                }
            }

            return;
        }

        String alias_name;
        if (function_node->hasAlias())
            alias_name = function_node->getAlias();
        else
        {
            /// Default alias names: __score, __score_2, ....
            alias_name = "__score";

            if (auto funcs_size = uniq_name_with_alias_map.size())
                alias_name += "_" + toString(funcs_size + 1);

            function_node->setAlias(alias_name);
        }

        uniq_name_with_alias_map[full_name] = alias_name;

        if (hybrid_function_nodes)
            hybrid_function_nodes->push_back(node);

        /// Save all existing distance funcs
        if (isDistance(function_node->getFunctionName()))
        {
            if (all_multiple_distance_funcs)
                all_multiple_distance_funcs->push_back(node);
        }

        has_hybrid_search_functions = true;
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node) const
    {
        if (only_check && has_hybrid_search_functions)
            return false;

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

    bool hasHybridSearchFunctions() const
    {
        return has_hybrid_search_functions;
    }

private:
    String assert_no_hybrids_place_message;
    QueryTreeNodes * hybrid_function_nodes = nullptr;
    bool only_check = false;
    bool has_hybrid_search_functions = false;
    /// std::unordered_set<String> uniq_names {};
    /// special search function should have alias name, used for projection name or
    /// cases with two same functions in select and order by clauses.
    std::unordered_map<String, String> uniq_name_with_alias_map;

    /// Support multiple distance functions
    QueryTreeNodes * all_multiple_distance_funcs = nullptr;
};

}

QueryTreeNodes collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes * all_distance_funcs)
{
    QueryTreeNodes result;
    CollectHybridSearchFunctionNodesVisitor visitor(&result, all_distance_funcs);
    visitor.visit(node);

    return result;
}

void collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result, QueryTreeNodes * all_distance_funcs)
{
    CollectHybridSearchFunctionNodesVisitor visitor(&result, all_distance_funcs);
    visitor.visit(node);
}

bool hasHybridSearchFunctionNodes(const QueryTreeNodePtr & node)
{
    CollectHybridSearchFunctionNodesVisitor visitor(true /*only_check*/);
    visitor.visit(node);

    return visitor.hasHybridSearchFunctions();
}

void assertNoHybridSearchFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_hybrids_place_message)
{
    CollectHybridSearchFunctionNodesVisitor visitor(assert_no_hybrids_place_message);
    visitor.visit(node);
}

QueryTreeNodePtr getColumnSourceForSpecialSearchFunc(const QueryTreeNodePtr & node)
{
    auto * function_node = node->as<FunctionNode>();
    if (!function_node || !function_node->isSpecialSearchFunction())
        return nullptr;

    auto function_name = function_node->getFunctionName();

    /// Find column source of the source vector column in a search function
    const auto & arguments_nodes = function_node->getArguments().getNodes();
    const auto & search_column = arguments_nodes[0];
    if (!search_column)
        return nullptr;

    auto search_column_node_type = search_column->getNodeType();
    ColumnNode * search_column_node = nullptr;

    if (search_column_node_type == QueryTreeNodeType::COLUMN)
        search_column_node = search_column->as<ColumnNode>();
    else if (isTextSearch(function_name) && search_column_node_type == QueryTreeNodeType::FUNCTION)
    {
        const auto & text_arguments_nodes  = search_column->as<FunctionNode>()->getArguments().getNodes();
        if (text_arguments_nodes[0])
            search_column_node = text_arguments_nodes[0]->as<ColumnNode>();
    }

    if (!search_column_node)
        return nullptr;

    auto search_column_source_node = search_column_node->getColumnSource();
    auto column_source_node_type = search_column_source_node->getNodeType();

    if (column_source_node_type != QueryTreeNodeType::TABLE &&
        column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
        column_source_node_type != QueryTreeNodeType::QUERY &&
        column_source_node_type != QueryTreeNodeType::UNION &&
        column_source_node_type != QueryTreeNodeType::ARRAY_JOIN)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expected table, table function, array join, query or union column source. Actual {}",
            search_column_source_node->formatASTForErrorMessage());

    return search_column_source_node;
}

/// Get table Storage of table from vector or text column's column source
StoragePtr getTableStorageForSearchColumn(QueryTreeNodePtr query_column_source, String search_column_name, ContextPtr context)
{
    if (!query_column_source)
        return nullptr;

    auto column_source_type = query_column_source->getNodeType();

    if (column_source_type == QueryTreeNodeType::TABLE)
    {
        auto table_storage_id = query_column_source->as<TableNode &>().getStorageID();
        return DatabaseCatalog::instance().tryGetTable(table_storage_id, context);
    }
    else if (column_source_type == QueryTreeNodeType::QUERY)
    {
        /// Get column source from subquery's projection
        for (const auto & projection_node : query_column_source->as<QueryNode &>().getProjection().getNodes())
        {
            if (auto * column_node = projection_node->as<ColumnNode>())
            {
                if (column_node->getColumnName() == search_column_name)
                    return getTableStorageForSearchColumn(column_node->getColumnSourceOrNull(), search_column_name, context);
            }
        }
    }
    else
    {
        LOG_DEBUG(getLogger("getTableStorageForSearchColumn"), "Unhandled node type for column source `{}` for search column name `{}`",
                    toString(column_source_type), search_column_name);
    }

    return nullptr;
}

/// create vector scan description, used by HybridSearch and VectorScan
VSDescription commonMakeVectorScanDescription(
    const String & function_col_name,
    QueryTreeNodePtr query_column,
    QueryTreeNodePtr query_vector,
    int topk,
    const ContextPtr & context,
    const Array & parameters,
    const String & search_index_name)
{
    VSDescription vector_scan_desc;
    vector_scan_desc.topk = topk;
    vector_scan_desc.column_name = function_col_name;
    vector_scan_desc.parameters = parameters;

    auto logger = getLogger("commonMakeVectorScanDescription");

    if (query_column)
    {
        if (query_column->getNodeType() == QueryTreeNodeType::COLUMN)
        {
            const auto & query_column_typed = query_column->as<ColumnNode &>();
            vector_scan_desc.search_column_name = query_column_typed.getColumnName();

            DataTypePtr search_vector_column_type = query_column_typed.getColumnType();
            vector_scan_desc.vector_search_type = getSearchIndexDataType(search_vector_column_type);

            StorageMetadataPtr metadata_snapshot = nullptr;
            bool is_remote_storage = false;
            auto table_storage = getTableStorageForSearchColumn(query_column_typed.getColumnSourceOrNull(), vector_scan_desc.search_column_name, context);
            if (table_storage)
            {
                metadata_snapshot = table_storage->getInMemoryMetadataPtr();
                is_remote_storage = table_storage->isRemote();
            }

            /// Don't save index_name to vector scan description for distributed tables which don't have indexes defined.
            if (!is_remote_storage)
            {
                vector_scan_desc.search_index_name = search_index_name;
                getAndCheckVectorScanInfoFromMetadata(metadata_snapshot, vector_scan_desc, context);
            }
        }
        else
        {
            LOG_DEBUG(logger, "query column node dump tree: {}", query_column->dumpTree());
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unexpected node type for query column: {}", query_column->getNodeType());
        }
    }

    if (query_vector)
    {
        if (const auto * constant_node = query_vector->as<ConstantNode>())
        {
            /// Construct ColumnPtr from Constant Node
            vector_scan_desc.query_column = constant_node->getResultType()->createColumnConst(1, constant_node->getValue());
        }
        else if (const auto * get_scalar_function_node = query_vector->as<FunctionNode>();
                get_scalar_function_node && get_scalar_function_node->getFunctionName() == "__getScalar")
        {
            /// Allow constant folding through getScalar
            const auto * get_scalar_const_arg = get_scalar_function_node->getArguments().getNodes().at(0)->as<ConstantNode>();
            if (get_scalar_const_arg && context->hasQueryContext())
            {
                auto query_context = context->getQueryContext();
                auto scalar_string = toString(get_scalar_const_arg->getValue());
                if (query_context->hasScalar(scalar_string))
                {
                    auto scalar = query_context->getScalar(scalar_string);
                    vector_scan_desc.query_column = ColumnConst::create(scalar.getByPosition(0).column, 1);
                }
            }

            if(!vector_scan_desc.query_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong query vector type {} in distance function", query_vector->getNodeType());
        }
        else if (query_vector->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            /// In cases with lambda function, non-const columns require execution
            auto expression = query_vector->clone();
            Block temp_block = {{DataTypeUInt8().createColumnConst(1, 0), std::make_shared<DataTypeUInt8>(), "_dummy"}};

            try
            {
                auto execution_context = Context::createCopy(context);
                GlobalPlannerContextPtr global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
                auto planner_context = std::make_shared<PlannerContext>(execution_context, global_planner_context, SelectQueryOptions{});

                collectSourceColumns(expression, planner_context, false /*keep_alias_columns*/);
                collectSets(expression, *planner_context);

                auto actions_dag = buildActionsDAGFromExpressionNode(expression, {}, planner_context);
                auto expression_actions = std::make_shared<ExpressionActions>(std::move(actions_dag), ExpressionActionsSettings::fromContext(context, CompileExpressions::yes));
                expression_actions->execute(temp_block, false);
                auto & new_query_column = temp_block.safeGetByPosition(0);

                if(!new_query_column.column || !checkColumn<ColumnArray>(*new_query_column.column))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong query vector type for argument {} in distance function", query_vector->formatASTForErrorMessage());

                vector_scan_desc.query_column = new_query_column.column;
            }
            catch(const std::exception& e)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", e.what());
            }
            catch(...)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "An exception occurred while executing the query vector {} in distance function", query_vector->formatOriginalASTForErrorMessage());
            }
        }
        else
        {
            LOG_DEBUG(logger, "query vector node dump tree: {}", query_vector->dumpTree());
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unexpected node type for query vector: {}", query_vector->getNodeType());
        }
    }

    if (search_index_name.empty())
        LOG_DEBUG(logger, "search column: {}", vector_scan_desc.search_column_name);
    else
        LOG_DEBUG(logger, "search column: {}, search index name: {}", vector_scan_desc.search_column_name, vector_scan_desc.search_index_name);

    return vector_scan_desc;
}

VSDescriptions extractVectorScanDescriptions(const QueryTreeNodes & vector_scan_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    VSDescriptions vector_scan_descriptions;
    auto logger = getLogger("extractVectorScanDescriptions");

    for (size_t i = 0; i < vector_scan_func_nodes.size(); ++i)
    {
        const auto & vector_scan_func_node_typed = vector_scan_func_nodes[i]->as<FunctionNode &>();
        const auto special_search_func = vector_scan_func_node_typed.getSpecialSearchFunction();

        const auto & arguments_nodes = vector_scan_func_node_typed.getArguments().getNodes();

        const auto & parameters_nodes = vector_scan_func_node_typed.getParameters().getNodes();
        Array parameters(parameters_nodes.size());

        /// Function parameters constness validated during analysis stage
        for (size_t k = 0; k < parameters_nodes.size(); ++k)
            parameters[k] = parameters_nodes[k]->as<ConstantNode &>().getValue();

        /// Extract index_name parameter if exists, save it to search_index_name in description and remove it from parameters.
        FieldVector vec_search_parameters;
        String search_index_name; /// Save index name in parameters

        for (const auto & arg : parameters)
        {
            LOG_DEBUG(logger, "parameter field: {}", toString(arg));
            if (arg.getType() == Field::Types::String)
            {
                String param_str = arg.safeGet<String>();
                LOG_DEBUG(logger, "parameter string: {}", param_str);
                if (param_str.find("index_name") != std::string::npos)
                {
                    auto pos = param_str.find('=');
                    if (pos == std::string::npos || pos == 0 || pos == param_str.length())
                        throw Exception(ErrorCodes::ILLEGAL_VECTOR_SCAN, "The parameter {} inside {} function should be key-value format string, separated by `=`.", vector_scan_func_node_typed.getFunctionName(), param_str);

                    String param_key = param_str.substr(0, pos);
                    String param_value = param_str.substr(pos + 1);
                    LOG_DEBUG(logger, "param_key: {}, param_value: {}", param_key, param_value);
                    if (param_key == "index_name")
                    {
                        if (search_index_name.empty())
                            search_index_name = param_value;
                        else
                            throw Exception(ErrorCodes::ILLEGAL_VECTOR_SCAN, "Multiple {} parameters in the {} function.", param_key, vector_scan_func_node_typed.getFunctionName());

                        continue;
                    }
                }
            }
            /// Add parameter to vec_search_parameters
            vec_search_parameters.emplace_back(arg);
        }

        Array params_array(vec_search_parameters.size());
        for (size_t k = 0; k < vec_search_parameters.size(); ++k)
            params_array[k] = vec_search_parameters[k];

        String function_col_name = special_search_func->getResultColumnName();
        auto vector_scan_desc = commonMakeVectorScanDescription(function_col_name, arguments_nodes[0], arguments_nodes[1],
                                                                static_cast<int>(limit_length), context, params_array, search_index_name);

        vector_scan_descriptions.push_back(vector_scan_desc);
    }

    return vector_scan_descriptions;
}

TextSearchInfoPtr commonMakeTextSearchInfo(
    const String & search_name,
    const String & function_col_name,
    QueryTreeNodePtr query_column,
    QueryTreeNodePtr query_text,
    int topk,
    const ContextPtr & context,
    const Array & parameters)
{
    String text_column_name;

    if (query_column)
    {
        auto text_column_node = query_column;

        /// Handle mapKeys function for text column
        if (auto * text_column_function_node = query_column->as<FunctionNode>())
        {
            auto arguments_nodes = text_column_function_node->getArguments().getNodes();
            if (text_column_function_node->getFunctionName() == "mapKeys" && arguments_nodes.size() == 1)
            {
                text_column_name = query_column->formatASTForErrorMessage();
                text_column_node = arguments_nodes[0];
            }
        }
        else if (auto * text_column_typed = query_column->as<ColumnNode>())
            text_column_name = text_column_typed->getColumnName();

        if (text_column_node && text_column_node->getNodeType() == QueryTreeNodeType::COLUMN)
        {
            const auto & query_column_typed = text_column_node->as<ColumnNode &>();

            StorageMetadataPtr metadata_snapshot = nullptr;
            bool is_remote_storage = false;
            auto table_storage = getTableStorageForSearchColumn(query_column_typed.getColumnSourceOrNull(), text_column_name, context);
            if (table_storage)
            {
                metadata_snapshot = table_storage->getInMemoryMetadataPtr();
                is_remote_storage = table_storage->isRemote();
            }

            if (!is_remote_storage)
                checkTantivyIndex(metadata_snapshot, text_column_name);
        }
        else
        {
            LOG_DEBUG(getLogger("commonMakeTextSearchInfo"), "query column node dump tree: {}", query_column->dumpTree());
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unexpected node type for query column: {}", query_column->getNodeType());
        }
    }

    String query_text_value;
    if (query_text)
    {
        if (query_text->getNodeType() == QueryTreeNodeType::CONSTANT)
        {
            query_text_value = query_text->as<ConstantNode &>().getValue().safeGet<String>();
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unexpected node type for query text: {}", query_text->getNodeType());
    }

    LOG_DEBUG(getLogger("makeTextSearchInfo"), "text search column: {}, query text: {}", text_column_name, query_text_value);

    bool enable_natural_language_query = true;
    String text_operator = "OR";

    for (const auto & arg : parameters)
    {
        String param_str = arg.safeGet<String>();
        auto pos = param_str.find('=');
        if (pos == std::string::npos || pos == 0 || pos == param_str.length())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The parameter {} inside {} function should be key-value format string, separated by `=`.", param_str, search_name);

        String param_key = param_str.substr(0, pos);
        String param_value = param_str.substr(pos + 1);

        if (param_key == "enable_nlq")
        {
            if (param_value.size() == 1)
            {
                /// 0 / 1
                std::stringstream param_ss(param_value);
                param_ss >> enable_natural_language_query;
                if (param_ss.fail())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "TextSearch parameter `enable_nlq` value should be bool");
            }
            else
            {
                /// boolalpha (true or false)
                std::stringstream param_ss_retry(param_value);
                param_ss_retry >> std::boolalpha >> enable_natural_language_query;
                if (param_ss_retry.fail() || !param_ss_retry.eof())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "TextSearch parameter `enable_nlq` value should be bool");
            }
        }
        else if (param_key == "operator")
        {
            if (param_value != "OR" && param_value != "AND")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "TextSearch parameter `operator` value should be either OR or AND");

            text_operator = param_value;
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown parameter {} for TextSearch", param_key);
    }

    return std::make_shared<TextSearchInfo>(text_column_name, query_text_value, function_col_name, topk, text_operator, enable_natural_language_query);
}

TextSearchInfoPtr makeTextSearchInfo(const QueryTreeNodes & text_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    if (text_search_func_nodes.size() != 1 || !text_search_func_nodes[0])
        return nullptr;

    const auto & text_search_func_node_typed = text_search_func_nodes[0]->as<FunctionNode &>();

    const auto & arguments_nodes = text_search_func_node_typed.getArguments().getNodes();

    const auto & parameters_nodes = text_search_func_node_typed.getParameters().getNodes();

    Array text_params(parameters_nodes.size());
    for (size_t i = 0; i < parameters_nodes.size(); ++i)
    {
        const auto & parameter_node = parameters_nodes[i];

        /// Function parameters constness validated during analysis stage
        const auto & constant_node = parameter_node->as<ConstantNode &>();
        if (constant_node.getResultType()->getTypeId() != TypeIndex::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters inside TextSearch function must be key-value format string, separated by `=`.");

        text_params[i] = constant_node.getValue().safeGet<String>();
    }

    auto text_search_func = text_search_func_node_typed.getSpecialSearchFunction();
    String function_col_name = text_search_func->getResultColumnName();

    auto tmp_text_search_info = commonMakeTextSearchInfo("TextSearch", function_col_name, arguments_nodes[0],
                                                        arguments_nodes[1], static_cast<int>(limit_length), context, text_params);

    LOG_DEBUG(getLogger("makeTextSearchInfo"), "create text search function: {}", text_search_func_node_typed.getFunctionName());

    return tmp_text_search_info;
}

/// create sparse search info, used by SparseSearch
SparseSearchInfoPtr commonMakeSparseSearchInfo(
    const String & search_name,
    const String & function_col_name,
    QueryTreeNodePtr query_column,
    QueryTreeNodePtr query_sparse,
    int topk,
    const ContextPtr & context,
    const Array & parameters)
{
    String sparse_column_name;

    if (query_column)
    {
        if (auto * sparse_column_typed = query_column->as<ColumnNode>())
        {
            sparse_column_name = sparse_column_typed->getColumnName();

            StorageMetadataPtr metadata_snapshot = nullptr;
            bool is_remote_storage = false;
            auto table_storage = getTableStorageForSearchColumn(sparse_column_typed->getColumnSourceOrNull(), sparse_column_name, context);
            if (table_storage)
            {
                metadata_snapshot = table_storage->getInMemoryMetadataPtr();
                is_remote_storage = table_storage->isRemote();
            }

            /// Sparse search cannot be performed when no sparse index exists
            /// Skip the sparse index check when table is distributed.
            if (!is_remote_storage)
                checkSparseIndex(metadata_snapshot, sparse_column_name);
        }
    }

    std::unordered_map<UInt32, Float32> query_sparse_vector;

    if (query_sparse)
    {
        ColumnPtr query_sparse_column;

        /// In new analyzer, the map function is hidden in a ConstantNode
        if (const auto * constant_node = query_sparse->as<ConstantNode>())
        {
            /// Construct ColumnPtr from Constant Node
            query_sparse_column = constant_node->getResultType()->createColumnConst(1, constant_node->getValue());
        }
        else if (query_sparse->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            /// The query sparse vector only can be created by map or mapFromArrays function
            /// Execute the ASTFunction to get the query sparse vector column
            auto expression = query_sparse->clone();
            Block temp_block = {{DataTypeUInt8().createColumnConst(1, 0), std::make_shared<DataTypeUInt8>(), "_dummy"}};

            try
            {
                auto execution_context = Context::createCopy(context);
                GlobalPlannerContextPtr global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
                auto planner_context = std::make_shared<PlannerContext>(execution_context, global_planner_context, SelectQueryOptions{});

                collectSourceColumns(expression, planner_context, false /*keep_alias_columns*/);
                collectSets(expression, *planner_context);

                auto actions_dag = buildActionsDAGFromExpressionNode(expression, {}, planner_context);
                auto expression_actions = std::make_shared<ExpressionActions>(std::move(actions_dag), ExpressionActionsSettings::fromContext(context, CompileExpressions::yes));
                expression_actions->execute(temp_block, false);
                auto & query_sparse_column_with_type = temp_block.safeGetByPosition(0);

                if(!query_sparse_column_with_type.column || !isColumnConst(*query_sparse_column_with_type.column))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong query sparse vector type for argument {} in {} function", query_sparse->formatASTForErrorMessage(), search_name);

                query_sparse_column = query_sparse_column_with_type.column;
            }
            catch(const std::exception& e)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", e.what());
            }
            catch(...)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "An exception occurred while executing the query sparse vector {} in {} function", query_sparse->formatASTForErrorMessage(), search_name);
            }
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected node type for query sparse: {}", query_sparse->getNodeType());
        }

        /// Get values from const map
        const ColumnMap * map_column = checkAndGetColumnConstData<ColumnMap>(query_sparse_column.get());

        if (!map_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong query sparse vector type {} in {} function, expected const Map type", query_sparse_column->getName(), search_name);

        const ColumnTuple & nested_data = map_column->getNestedData();
        const auto & key_column = nested_data.getColumn(0);
        const auto & val_column = nested_data.getColumn(1);

        WhichDataType key_type(key_column.getDataType());
        WhichDataType val_type(val_column.getDataType());

        if ((key_type.isUInt8() || key_type.isUInt16() || key_type.isUInt32()) && val_type.isFloat())
        {
            for (size_t i = 0; i < key_column.size(); ++i)
            {
                query_sparse_vector[static_cast<UInt32>(key_column.getUInt(i))] = val_column.getFloat32(i);
            }
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong query sparse vector type in SparseSearch function, expected const Map<UInt32, Float32> type.");
    }

    LOG_DEBUG(getLogger("makeSparseSearchInfo"), "sparse search column: {}", sparse_column_name);

    SparseSearchInfo::SearchMode search_mode;
    for (const auto & arg : parameters)
    {
        if (arg.getType() != Field::Types::String)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "All parameters inside {} function must be key-value format string, separated by `=`.",
                search_name);

        String param_str = arg.safeGet<String>();
        auto pos = param_str.find('=');
        if (pos == std::string::npos || pos == 0 || pos == param_str.length())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The parameter {} inside {} function should be key-value format string, separated by `=`.",
                param_str,
                search_name);

        String param_key = param_str.substr(0, pos);
        String param_value = param_str.substr(pos + 1);

        if (param_key == "search_mode")
        {
            search_mode = SparseSearchInfo::strToSparseMode(param_value);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown parameter {} for {} function", param_key, search_name);
    }

    return std::make_shared<SparseSearchInfo>(sparse_column_name, query_sparse_vector, function_col_name, topk, search_mode);
}

SparseSearchInfoPtr makeSparseSearchInfo(const QueryTreeNodes & sparse_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    if (sparse_search_func_nodes.size() != 1 || !sparse_search_func_nodes[0])
        return nullptr;

    const auto & sparse_search_func_node_typed = sparse_search_func_nodes[0]->as<FunctionNode &>();

    const auto & arguments_nodes = sparse_search_func_node_typed.getArguments().getNodes();

    const auto & parameters_nodes = sparse_search_func_node_typed.getParameters().getNodes();

    Array sparse_params(parameters_nodes.size());
    for (size_t i = 0; i < parameters_nodes.size(); ++i)
    {
        const auto & parameter_node = parameters_nodes[i];

        /// Function parameters constness validated during analysis stage
        const auto & constant_node = parameter_node->as<ConstantNode &>();
        if (constant_node.getResultType()->getTypeId() != TypeIndex::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters inside SparseSearch function must be key-value format string, separated by `=`.");

        sparse_params[i] = constant_node.getValue().safeGet<String>();
    }

    auto sparse_search_func = sparse_search_func_node_typed.getSpecialSearchFunction();
    String function_col_name = sparse_search_func->getResultColumnName();

    auto tmp_sparse_search_info = commonMakeSparseSearchInfo("SparseSearch", function_col_name, arguments_nodes[0],
                                                        arguments_nodes[1], static_cast<int>(limit_length), context, sparse_params);

    LOG_DEBUG(getLogger("makeSparseSearchInfo"), "create sparse search function: {}", sparse_search_func_node_typed.getFunctionName());

    return tmp_sparse_search_info;
}

HybridSearchInfoPtr makeHybirdSearchInfo(const QueryTreeNodes & hybrid_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    if (hybrid_search_func_nodes.size() != 1 || !hybrid_search_func_nodes[0])
        return nullptr;

    const auto & hybrid_search_func_node_typed = hybrid_search_func_nodes[0]->as<FunctionNode &>();

    const auto & arguments_nodes = hybrid_search_func_node_typed.getArguments().getNodes();

    std::unordered_map<String, String> hybrid_parameters_map;
    std::vector<String> vector_scan_parameter;
    std::vector<String> text_search_parameters;
    std::vector<String> sparse_search_parameters;
    String vector_scan_index_name;

    const auto & parameters_nodes = hybrid_search_func_node_typed.getParameters().getNodes();
    for (const auto & parameter_node : parameters_nodes)
    {
        /// Function parameters constness validated during analysis stage
        const auto & constant_node = parameter_node->as<ConstantNode &>();
        if (constant_node.getResultType()->getTypeId() != TypeIndex::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters inside HybridSearch function must be key-value format string, separated by `=`.");

        String param_str = constant_node.getValue().safeGet<String>();
        auto pos = param_str.find('=');
        if (pos == std::string::npos || pos == 0 || pos == param_str.length())
            throw Exception(ErrorCodes::ILLEGAL_HYBRID_SEARCH, "The parameter {} inside HybridSearch function should be key-value format string, separated by `=`.", param_str);

        String param_key = param_str.substr(0, pos);
        String param_value = param_str.substr(pos + 1);

        if (param_key == "fusion_type" || param_key == "fusion_weight" || param_key == "fusion_k" || param_key == "num_candidates")
        {
            if (hybrid_parameters_map.count(param_key) > 0)
            {
                throw Exception(ErrorCodes::ILLEGAL_HYBRID_SEARCH, "Multiple {} parameters in the HybridSearch function.", param_key);
            }
            hybrid_parameters_map[param_key] = param_value;
        }
        else if (param_key.find(vector_scan_parameter_prefix) == 0)
        {
            /// Extract index_name parameter for vector scan if exists
            if (param_key.find("index_name") != std::string::npos)
                vector_scan_index_name = param_value;
            else
                vector_scan_parameter.push_back(param_str.substr(std::strlen(vector_scan_parameter_prefix)));
        }
        else if (param_key == "enable_nlq" || param_key == "operator")
        {
            text_search_parameters.push_back(param_str);
        }
        else if (param_key.find(sparse_search_parameter_prefix) == 0)
        {
            sparse_search_parameters.push_back(param_str.substr(std::strlen(sparse_search_parameter_prefix)));
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_HYBRID_SEARCH, "Unknown parameter {} in the HybridSearch function.", param_key);
        }
    }

    /// Use num_candidates for vector scan's top-k to get more candidates results for hybrid search
    const auto & settings_ref = context->getSettingsRef();
    int num_candidates = 0;
    if (hybrid_parameters_map.contains("num_candidates"))
    {
        std::stringstream num_candidates_ss(hybrid_parameters_map["num_candidates"]);
        num_candidates_ss >> num_candidates;
        if (num_candidates_ss.fail() || !num_candidates_ss.eof())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HybridSearch parameter `num_candidates` value should be int");
    }

    /// Use default value (3 * topk) if specified num_candidates <= 0
    if (num_candidates <= 0)
        num_candidates = static_cast<int>(settings_ref.hybrid_search_top_k_multiple_base * limit_length);
    else if (static_cast<UInt64>(num_candidates) < limit_length)
    {
        /// num_candidates should be no less than limit N (top k)
        num_candidates = static_cast<int>(limit_length);
    }

    LOG_DEBUG(getLogger("makeHybirdSearchInfo"), "num_candidates is {}", num_candidates);

    VectorScanInfoPtr tmp_vector_scan_info = nullptr;
    TextSearchInfoPtr tmp_text_search_info = nullptr;
    SparseSearchInfoPtr tmp_sparse_search_info = nullptr;

    /// The search types in the hybrid search function.
    std::vector<HybridSearchFuncType> search_type_in_hybrid_search;

    /// make VSDescription for HybridSearchInfo
    auto makeVectorScanInfo = [&](size_t vector_column_arg_idx, size_t query_vector_arg_idx)
    {
        Array vector_scan_params_array(vector_scan_parameter.size());
        for (size_t i = 0; i < vector_scan_parameter.size(); ++i)
            vector_scan_params_array[i] = vector_scan_parameter[i];

        auto vector_scan_desc = commonMakeVectorScanDescription(
            "distance_func",
            arguments_nodes[vector_column_arg_idx],
            arguments_nodes[query_vector_arg_idx],
            num_candidates,
            context,
            vector_scan_params_array,
            vector_scan_index_name);

        VSDescriptions vector_scan_descriptions;
        vector_scan_descriptions.push_back(vector_scan_desc);
        tmp_vector_scan_info = std::make_shared<VectorScanInfo>(vector_scan_descriptions);
    };

    /// make TextSearchInfo for HybridSearchInfo
    auto makeTextSearchInfo = [&](size_t text_column_arg_idx, size_t query_text_arg_idx)
    {
        Array text_params(text_search_parameters.size());
        for (size_t i = 0; i < text_search_parameters.size(); ++i)
            text_params[i] = text_search_parameters[i];

        tmp_text_search_info = commonMakeTextSearchInfo(
            "HybridSearch",
            "textsearch_func",
            arguments_nodes[text_column_arg_idx],
            arguments_nodes[query_text_arg_idx],
            num_candidates,
            context,
            text_params);
    };

    /// make SparseSearchInfo for HybridSearchInfo
    auto makeSparseSearchInfo = [&](size_t sparse_column_arg_idx, size_t query_sparse_arg_idx)
    {
        Array sparse_params(sparse_search_parameters.size());
        for (size_t i = 0; i < sparse_search_parameters.size(); ++i)
            sparse_params[i] = sparse_search_parameters[i];

        tmp_sparse_search_info = commonMakeSparseSearchInfo(
            "HybridSearch",
            "sparsesearch_func",
            arguments_nodes[sparse_column_arg_idx],
            arguments_nodes[query_sparse_arg_idx],
            num_candidates,
            context,
            sparse_params);
    };

    for (size_t i = 0; i < 2; ++i)
    {
        if (auto * argument_node = arguments_nodes[i]->as<ColumnNode>())
        {
            switch (inferSearchModeInHybridSearch(argument_node->getColumn()))
            {
                case HybridSearchFuncType::VECTOR_SCAN:
                    search_type_in_hybrid_search.push_back(HybridSearchFuncType::VECTOR_SCAN);
                    makeVectorScanInfo(i, i + 2);
                    break;
                case HybridSearchFuncType::TEXT_SEARCH:
                    search_type_in_hybrid_search.push_back(HybridSearchFuncType::TEXT_SEARCH);
                    makeTextSearchInfo(i, i + 2);
                    break;
                case HybridSearchFuncType::SPARSE_SEARCH:
                    search_type_in_hybrid_search.push_back(HybridSearchFuncType::SPARSE_SEARCH);
                    makeSparseSearchInfo(i, i + 2);
                    break;
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported search type in HybridSearch function");
            }
        }
        else /// text search argument
        {
            search_type_in_hybrid_search.push_back(HybridSearchFuncType::TEXT_SEARCH);
            makeTextSearchInfo(i, i + 2);
        }
    }

    String hybrid_fusion_type = hybrid_parameters_map["fusion_type"];
    auto hybrid_search_func = hybrid_search_func_node_typed.getSpecialSearchFunction();
    String function_column_name = hybrid_search_func->getResultColumnName();

    HybridSearchInfoPtr hybrid_search_info = nullptr;
    if (isRelativeScoreFusion(hybrid_fusion_type))
    {
        float hybrid_fusion_weight = static_cast<float>(settings_ref.hybrid_search_fusion_weight);
        if (hybrid_parameters_map.count("fusion_weight") > 0)
        {
            std::stringstream fusion_weight_ss(hybrid_parameters_map["fusion_weight"]);
            fusion_weight_ss >> hybrid_fusion_weight;
            if (fusion_weight_ss.fail())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HybridSearch parameter `fusion_weight` value should be float");
        }

        if (hybrid_fusion_weight < 0 || hybrid_fusion_weight > 1)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Wrong HybridSearch parameter for Relative Score Fusion(RSF), valid value is in interval [0.0f, 1.0f]");
        }

        hybrid_search_info = std::make_shared<HybridSearchInfo>(
            tmp_vector_scan_info,
            tmp_text_search_info,
            tmp_sparse_search_info,
            search_type_in_hybrid_search,
            function_column_name, static_cast<int>(limit_length), hybrid_fusion_type, hybrid_fusion_weight);
    }
    else if (isRankFusion(hybrid_fusion_type))
    {
        int hybrid_fusion_k = static_cast<int>(settings_ref.hybrid_search_fusion_k);
        if (hybrid_parameters_map.count("fusion_k") > 0)
        {
            std::stringstream fusion_k_ss(hybrid_parameters_map["fusion_k"]);
            fusion_k_ss >> hybrid_fusion_k;
            if (fusion_k_ss.fail())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HybridSearch parameter `fusion_k` value should be int");
        }

        if (hybrid_fusion_k < 0)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Wrong HybridSearch parameter for Reciprocal Rank Fusion(RRF), `fusion_k` is less than 0");
        }
        hybrid_search_info = std::make_shared<HybridSearchInfo>(
            tmp_vector_scan_info,
            tmp_text_search_info,
            tmp_sparse_search_info,
            search_type_in_hybrid_search,
            function_column_name, static_cast<int>(limit_length), hybrid_fusion_type, hybrid_fusion_k);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong or missing HybridSearch function parameter `fusion_type`. Valid values: 'RSF' and 'RRF'");
    }

    return hybrid_search_info;
}

std::optional<SpecialSearchAnalysisResult> analyzeVectorScan(const QueryTreeNodes & vector_scan_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length,
    const QueryTreeNodePtr & query_tree)
{
    auto vector_scan_descs = extractVectorScanDescriptions(vector_scan_func_nodes, context, limit_length);

    if (vector_scan_descs.empty())
        return std::nullopt;

    /// Not allow different directions in multiple distance functions
    if (vector_scan_descs.size() > 1)
    {
        int direction = 0;
        for (auto & vector_scan_desc : vector_scan_descs)
        {
            if (direction == 0)
                direction = vector_scan_desc.direction;
            else if (direction != vector_scan_desc.direction)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Not allowed metric types with different result order in multiple distances");
        }
    }
    else /// single vector scan function
    {
        /// Avoid the check for distributed or other cases when we don't get table storage
        auto vector_scan_desc = vector_scan_descs[0];
        auto search_column_source_node = getColumnSourceForSpecialSearchFunc(vector_scan_func_nodes[0]);
        auto table_storage = getTableStorageForSearchColumn(search_column_source_node, vector_scan_desc.search_column_name, context);
        if (table_storage && !table_storage->isRemote())
        {
            /// When metric_type = IP in definition of vector index, order by must be DESC.
            bool is_batch = isBatchDistance(vector_scan_desc.column_name);
            String func_name = is_batch ? "batch_distance" : "distance";

            /// In SortDirection ASCENDING - 0, DESCENDING - 1. In VSDesc 1 - ascending, -1 - descending
            SortDirection expected_direction =
                (vector_scan_desc.direction == 1) ? SortDirection::ASCENDING : SortDirection::DESCENDING;
            checkOrderBySortDirection(func_name, query_tree, expected_direction, is_batch);
        }
    }

    SpecialSearchAnalysisResult vector_scan_analysis_result;
    vector_scan_analysis_result.has_vector_scan = true;
    vector_scan_analysis_result.vector_scan_descriptions = std::move(vector_scan_descs);
    return vector_scan_analysis_result;
}

std::optional<SpecialSearchAnalysisResult> analyzeTextSearch(const QueryTreeNodes & text_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    auto text_search_info = makeTextSearchInfo(text_search_func_nodes, context, limit_length);

    if (!text_search_info)
        return std::nullopt;

    SpecialSearchAnalysisResult text_search_analysis_result;
    text_search_analysis_result.has_text_search = true;
    text_search_analysis_result.text_search_info = std::move(text_search_info);
    return text_search_analysis_result;
}

std::optional<SpecialSearchAnalysisResult> analyzeSparseSearch(const QueryTreeNodes & sparse_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    auto sparse_search_info = makeSparseSearchInfo(sparse_search_func_nodes, context, limit_length);

    if (!sparse_search_info)
        return std::nullopt;

    SpecialSearchAnalysisResult sparse_search_analysis_result;
    sparse_search_analysis_result.has_sparse_search = true;
    sparse_search_analysis_result.sparse_search_info = std::move(sparse_search_info);
    return sparse_search_analysis_result;
}

std::optional<SpecialSearchAnalysisResult> analyzeHybridSearch(const QueryTreeNodes & hybrid_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    auto hybrid_search_info = makeHybirdSearchInfo(hybrid_search_func_nodes, context, limit_length);

    if (!hybrid_search_info)
        return std::nullopt;

    SpecialSearchAnalysisResult hybrid_search_analysis_result;
    hybrid_search_analysis_result.has_hybrid_search = true;
    hybrid_search_analysis_result.hybrid_search_info = std::move(hybrid_search_info);
    return hybrid_search_analysis_result;
}

/** Construct special search analysis result if query tree has distance, textsearch or hybridsearch functions.
  * Actions before special search are added into actions chain, if result is not null optional.
  */
std::optional<SpecialSearchAnalysisResult> analyzeSpecialSearch(const QueryTreeNodePtr & query_tree,
    const ContextPtr & context)
{
    auto special_search_function_nodes = collectHybridSearchFunctionNodes(query_tree);

    if (special_search_function_nodes.size() == 0)
    {
        /// Check context
        if (auto vec_scan_descs = context->getVecScanDescriptions())
        {
            SpecialSearchAnalysisResult vector_scan_analysis_result;
            vector_scan_analysis_result.has_vector_scan = true;
            vector_scan_analysis_result.vector_scan_descriptions = *vec_scan_descs;
            return vector_scan_analysis_result;
        }
        else if (auto text_search_info = context->getTextSearchInfo())
        {
            SpecialSearchAnalysisResult text_search_analysis_result;
            text_search_analysis_result.has_text_search = true;
            text_search_analysis_result.text_search_info = text_search_info;
            return text_search_analysis_result;
        }
        else if (auto hybrid_search_info = context->getHybridSearchInfo())
        {
            SpecialSearchAnalysisResult hybrid_search_analysis_result;
            hybrid_search_analysis_result.has_hybrid_search = true;
            hybrid_search_analysis_result.hybrid_search_info = hybrid_search_info;
            return hybrid_search_analysis_result;
        }
        else if (auto sparse_search_info = context->getSparseSearchInfo())
        {
            SpecialSearchAnalysisResult sparse_search_analysis_result;
            sparse_search_analysis_result.has_sparse_search = true;
            sparse_search_analysis_result.sparse_search_info = sparse_search_info;
            return sparse_search_analysis_result;
        }
        else
            return std::nullopt;
    }

    LOG_DEBUG(getLogger("analyzeSpecialSearch"), "analyzeSpecialSearch");

    /// Get topK from limit N
    UInt64 limit_length = getTopKFromLimit(query_tree, context);

    /// topk in multiple distance functions case should be distances_top_k_multiply_factor * k
    if (special_search_function_nodes.size() > 1)
        limit_length = limit_length * context->getSettingsRef().distances_top_k_multiply_factor;

    LOG_DEBUG(getLogger("analyzeSpecialSearch"), "limit_length={}", limit_length);

    /// Check the function name to find which search: vector scan, text or hybrid search
    const auto & search_func_node = special_search_function_nodes[0]->as<FunctionNode &>();
    String func_name = search_func_node.getFunctionName();

    LOG_DEBUG(getLogger("analyzeSpecialSearch"), "search func node name={}", func_name);

    std::optional<SpecialSearchAnalysisResult> special_search_analysis_result_optional = std::nullopt;

    if (isVectorScanFunc(func_name))
        special_search_analysis_result_optional = analyzeVectorScan(special_search_function_nodes, context, limit_length, query_tree);
    else if (isTextSearch(func_name))
        special_search_analysis_result_optional = analyzeTextSearch(special_search_function_nodes, context, limit_length);
    else if (isHybridSearch(func_name))
        special_search_analysis_result_optional = analyzeHybridSearch(special_search_function_nodes, context, limit_length);
    else if (isSparseSearch(func_name))
        special_search_analysis_result_optional = analyzeSparseSearch(special_search_function_nodes, context, limit_length);

    /// Add source column of vector column or text column to analysis result
    if (special_search_analysis_result_optional)
    {
        /// Find column source of the vector column a in search function
        auto search_column_source_node = getColumnSourceForSpecialSearchFunc(special_search_function_nodes[0]);
        special_search_analysis_result_optional->source_weak_pointer = search_column_source_node;
    }

    return special_search_analysis_result_optional;
}

}
