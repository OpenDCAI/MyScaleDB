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

#include <Core/IResolvedFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Planner/TableExpressionData.h>
#include <AIDB/Utils/CommonUtils.h>

namespace DB
{

class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

class SpecialSearchFunction;
using SpecialSearchFunctionPtr = std::shared_ptr<SpecialSearchFunction>;
using ConstSpecialSearchFunctionPtr = std::shared_ptr<const SpecialSearchFunction>;

static constexpr auto SPECIAL_SEARCH_COLUMN_SUFFIX = "_func";

/** Special search functions interface. (reference from IAggregateFunction)
  * Instances of classes with this interface do not contain the data itself for special search,
  *  but contain only metadata (description) of the special search function.
  * Add column name and column identifier for search function result column.
  */
class SpecialSearchFunction : public std::enable_shared_from_this<SpecialSearchFunction>, public IResolvedFunction
{
public:
    SpecialSearchFunction(const String & name_, const DataTypes & argument_types_, const Array & parameters_)
        : name(name_)
        , argument_types(argument_types_)
        , parameters(parameters_)
    {
        if (isBatchDistance(name))
        {
            auto id_type = std::make_shared<DataTypeUInt32>();
            auto distance_type = std::make_shared<DataTypeFloat32>();
            DataTypes types;
            types.emplace_back(id_type);
            types.emplace_back(distance_type);
            result_type = std::make_shared<DataTypeTuple>(types);
        }
        else
            result_type = std::make_shared<DataTypeFloat32>();

        /// Default column name with _func suffix
        result_column_name = name + SPECIAL_SEARCH_COLUMN_SUFFIX;
    }

    String getName() const { return name; }

    ~SpecialSearchFunction() override = default;

    const DataTypePtr & getResultType() const override { return result_type; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const Array & getParameters() const override { return parameters; }

    /// Get search result column name
    const String & getResultColumnName() const
    {
        return result_column_name;
    }

    /// Set search result column name
    void setResultColumnName(const String & func_col_name) const
    {
        result_column_name = func_col_name;
    }

    const ColumnIdentifier & getColumnIdentifier() const { return function_identifier_name; }

    void setColumnIdentifier(const ColumnIdentifier & column_identifier) const
    {
        function_identifier_name = column_identifier;
    }

protected:
    String name;
    DataTypes argument_types;
    Array parameters;
    DataTypePtr result_type;

    /// search function result column name
    mutable String result_column_name;

    mutable ColumnIdentifier function_identifier_name; /// column identifier for function result column
};

}
