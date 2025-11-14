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

#include <AIDB/Common/SkipIndex/IndexSettings.h>

namespace DB
{

IndexSettings & IndexSettings::operator=(const IndexSettings & other)
{
    if (this != &other)
    {
        this->json_parameter = other.json_parameter;
    }
    return *this;
}

SparseIndexSettings & SparseIndexSettings::operator=(const SparseIndexSettings & other)
{
    if (this != &other)
    {
        IndexSettings::operator=(other);
        this->column = other.column;
    }
    return *this;
}

TantivyIndexSettings & TantivyIndexSettings::operator=(const TantivyIndexSettings & other)
{
    if (this != &other)
    {
        IndexSettings::operator=(other);
        this->indexed_columns = other.indexed_columns;
    }
    return *this;
}

}
