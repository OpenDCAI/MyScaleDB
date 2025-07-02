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


#include <AIDB/Common/SkipIndex/IndexFileMeta.h>

namespace DB
{

IndexFileMeta::IndexFileMeta() : offset_begin(0), offset_end(0)
{
    file_name[0] = '\0';
}

IndexFileMeta::IndexFileMeta(const String & file_name_, UInt64 offset_begin_, UInt64 offset_end_)
{
    strcpy(file_name, file_name_.c_str());
    offset_begin = offset_begin_;
    offset_end = offset_end_;
}

bool IndexFileMeta::operator==(const IndexFileMeta & other) const
{
    return strcmp(file_name, other.file_name) == 0 && offset_begin == other.offset_begin && offset_end == other.offset_end;
}

}
