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
#include <AIDB/Common/SkipIndex/IndexFilesManager.h>
#include <Disks/DiskLocal.h>
#include <IO/HashingReadBuffer.h>
#include <IO/copyData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CUSTOM_SKIP_INDEX_FILES_MANAGER_ERROR;
    extern const int CUSTOM_SKIP_INDEX_FILES_ARCHIVE_ERROR;
    extern const int CUSTOM_SKIP_INDEX_FILES_EXTRACT_ERROR;
}


IndexFilesManager::IndexFilesManager(
    const CustomIndexType index_type_,
    const String & index_meta_file_suffix,
    const String & index_data_file_suffix,
    const String & skp_index_name_,
    const DataPartStoragePtr storage_,
    const MutableDataPartStoragePtr storage_builder_)
    : index_type(index_type_)
    , skp_idx_name(skp_index_name_)
    , storage(storage_)
    , storage_builder(storage_builder_)
    , index_meta_file_name(skp_index_name_ + index_meta_file_suffix)
    , index_data_file_name(skp_index_name_ + index_data_file_suffix)
    , context(Context::getGlobalContextInstance())
    , log(&Poco::Logger::get(getCustomIndexName(index_type_) + "IndexFilesManager"))
    , tmp_disk(std::make_shared<DiskLocal>(TEMP_DISK_NAME, this->context->getPath()))
{
    if (!this->context || !this->storage || !this->tmp_disk)
    {
        throw DB::Exception(DB::ErrorCodes::CUSTOM_SKIP_INDEX_FILES_MANAGER_ERROR, "Failed init IndexFilesManager for {}.", this->index_type);
    }
    this->initFullIndexPathInStore();
}

void IndexFilesManager::initFullIndexPathInStore()
{
    /// store_prefix is like /var/lib/clickhouse/xxx_index_store/
    fs::path store_prefix = "";
    if (this->index_type == CustomIndexType::TantivyIndex)
    {
        store_prefix = this->context->getTantivyIndexStorePath();
    }
    else if (this->index_type == CustomIndexType::SparseIndex)
    {
        store_prefix = this->context->getSparseIndexStorePath();
    }

    // storage_relative_path is like store/20a/20add947-81e3-41d1-a429-0c4f43e711be/tmp_mut_all_1_1_1_2/
    fs::path storage_relative_path = storage->getRelativePath();
    // storage_relative_parent_path is like store/20a/20add947-81e3-41d1-a429-0c4f43e711be/
    fs::path storage_relative_parent_path = storage_relative_path.parent_path().parent_path();
    // tmp_mut_all_1_1_1_2 -> all_1_1_1_2
    String part_name = storage->getPartDirectory();

    // initialize store directory
    std::unique_lock<std::shared_mutex> lock(this->full_index_path_lock);
    this->full_index_path_in_store = store_prefix / storage_relative_parent_path / part_name / this->skp_idx_name / "";
    LOG_INFO(this->log, "init `full_index_path_in_store`: {} for part {}", this->full_index_path_in_store, storage->getRelativePath());
}

String IndexFilesManager::getFullIndexPathInStore()
{
    std::shared_lock<std::shared_mutex> lock(this->full_index_path_lock);
    return this->full_index_path_in_store;
}

String IndexFilesManager::updateFullIndexPathInStore(const String & new_part_path_in_store)
{
    // /var/lib/..../xx_index_store/store/20a/20add947-81e3-41d1-a429-0c4f43e711be/all_1_1_1_2/
    auto current_part_path_in_store = fs::path(this->full_index_path_in_store).parent_path().parent_path();
    auto new_full_index_path_in_store = fs::path(new_part_path_in_store) / this->skp_idx_name / "";

    if (this->full_index_path_in_store != new_full_index_path_in_store)
    {
        LOG_INFO(
            this->log, "update `full_index_path_in_store` from `{}` to `{}`", this->full_index_path_in_store, new_full_index_path_in_store);

        // rename Skip index path in `XXIndexFilesManager`.
        std::unique_lock<std::shared_mutex> lock(this->full_index_path_lock);
        this->full_index_path_in_store = new_full_index_path_in_store;

        // rename data part path in store.
        if (fs::exists(current_part_path_in_store))
        {
            fs::rename(current_part_path_in_store, new_part_path_in_store);
        }
        else
        {
            if (fs::exists(new_part_path_in_store))
                LOG_INFO(this->log, "part path in store `{}` has already been renamed.", new_part_path_in_store);
            else
                LOG_WARNING(this->log, "can't find and rename part path in store. `{}`.", current_part_path_in_store);
        }
    }
    return this->full_index_path_in_store;
}

void IndexFilesManager::removeFullIndexPathInStoreForward()
{
    std::shared_lock<std::shared_mutex> lock(this->full_index_path_lock);
    StoreDirectoryHelper::removeIndexPathWithCleanup(this->full_index_path_in_store);
}

ChecksumPairs IndexFilesManager::archive()
{
    std::shared_lock<std::shared_mutex> lock(this->full_index_path_lock);
    String index_files_directory = this->full_index_path_in_store;


    if (!this->tmp_disk->isDirectory(index_files_directory))
    {
        LOG_WARNING(log, "[archive] index_files_directory({}) is not a directory, may be an empty part.", index_files_directory);
        ChecksumPairs checksums;
        return checksums;
    }

    if (!this->storage_builder)
    {
        LOG_ERROR(log, "[archive] storage_builder can't be null when archiving index files to part.");
        throw DB::Exception(
            DB::ErrorCodes::CUSTOM_SKIP_INDEX_FILES_ARCHIVE_ERROR, "storage_builder can't be null when archiving index files to part.");
    }

    std::unique_ptr<WriteBufferFromFileBase> meta_data_write_stream
        = this->storage_builder->writeFile(this->index_meta_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    std::unique_ptr<WriteBufferFromFileBase> index_data_write_stream
        = this->storage_builder->writeFile(this->index_data_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});

    IndexFileMetas metas;
    UInt64 written_bytes = 0;
    /// index_data_write_stream -> data_hashing_uncompressed_stream
    auto data_hashing_uncompressed_stream = std::make_unique<HashingWriteBuffer>(*index_data_write_stream);
    /// meta_data_write_stream -> meta_hashing_uncompressed_stream
    auto meta_hashing_uncompressed_stream = std::make_unique<HashingWriteBuffer>(*meta_data_write_stream);

    for (const auto & entry : fs::directory_iterator(index_files_directory))
    {
        if (fs::is_regular_file(entry))
        {
            std::unique_ptr<ReadBufferFromFileBase> temp_file_read_stream
                = this->tmp_disk->readFile(entry.path(), {}, std::nullopt, std::nullopt);
            UInt64 file_size = static_cast<UInt64>(temp_file_read_stream->getFileSize());
            copyData(*temp_file_read_stream, *data_hashing_uncompressed_stream);
            metas.emplace_back(entry.path().filename(), written_bytes, written_bytes + file_size);
            written_bytes += file_size;
            LOG_TRACE(
                this->log,
                "[archive] Archiving SkipIndex file [file_name:{}, file_size:{}, written_bytes:{}]",
                entry.path().filename(),
                file_size,
                written_bytes);
        }
        else
        {
            LOG_ERROR(
                log,
                "[archive] Can't archive SkipIndex file [file_path:{}, written_bytes:{}] from index_store:{} to data_part:{}",
                entry.path(),
                written_bytes,
                index_files_directory,
                this->storage_builder->getRelativePath());
            throw DB::Exception(
                DB::ErrorCodes::CUSTOM_SKIP_INDEX_FILES_ARCHIVE_ERROR,
                "Can't archive SkipIndex file [file_path:{}, written_bytes:{}] from index_store:{} to data_part:{}",
                entry.path(),
                written_bytes,
                index_files_directory,
                this->storage_builder->getRelativePath());
        }
    }

    /// Stores SkipIndex data information
    data_hashing_uncompressed_stream->finalize();

    /// Stores SkipIndex meta information
    size_t metas_size = metas.size();
    meta_hashing_uncompressed_stream->write(reinterpret_cast<const char *>(&metas_size), sizeof(size_t));
    meta_hashing_uncompressed_stream->write(reinterpret_cast<const char *>(metas.data()), metas_size * sizeof(IndexFileMeta));
    meta_hashing_uncompressed_stream->finalize();

    LOG_INFO(
        this->log,
        "[archive] Archiving of skip index files from the index_store:`{}` to the data_part:`{}` is complete, total bytes: {}, "
        "total "
        "files: {}",
        index_files_directory,
        storage_builder->getRelativePath(),
        written_bytes,
        metas_size);

    ChecksumPairs checksums;
    // To prevent inconsistency issues with SkipIndex index file checksums across multiple replicas, an empty checksum is generated here.
    checksums.emplace_back(index_data_file_name, DB::MergeTreeDataPartChecksums::Checksum());
    // DB::MergeTreeDataPartChecksums::Checksum(data_hashing_uncompressed_stream->count(), data_hashing_uncompressed_stream->getHash()));
    checksums.emplace_back(index_meta_file_name, DB::MergeTreeDataPartChecksums::Checksum());
    // DB::MergeTreeDataPartChecksums::Checksum(meta_hashing_uncompressed_stream->count(), meta_hashing_uncompressed_stream->getHash()));

    return checksums;
}

void IndexFilesManager::extract()
{
    DB::OpenTelemetry::SpanHolder span("index_files_manager::extract");
    std::shared_lock<std::shared_mutex> lock(this->full_index_path_lock);
    String index_files_directory = this->full_index_path_in_store;

    if (this->tmp_disk->isDirectory(index_files_directory))
    {
        LOG_INFO(
            log,
            "[extract] directory `{}` not empty. Assuming these skip index files have already been extracted",
            index_files_directory);
        return;
    }

    if (!this->storage)
    {
        LOG_ERROR(log, "[extract] storage can't be null when extracting skip index meta/data file.");
        throw DB::Exception(
            DB::ErrorCodes::CUSTOM_SKIP_INDEX_FILES_EXTRACT_ERROR, "storage can't be null when extracting skip index meta/data file.");
    }


    if (!this->storage->exists(this->index_meta_file_name) || !this->storage->exists(this->index_data_file_name))
    {
        throw DB::Exception(
            DB::ErrorCodes::CUSTOM_SKIP_INDEX_FILES_EXTRACT_ERROR,
            "Can't perform extraction operation. The data part `{}` does not contain any index files, bad {} index file.",
            storage->getRelativePath(),
            getCustomIndexName(this->index_type));
    }

    this->tmp_disk->createDirectories(index_files_directory);


    std::unique_ptr<ReadBufferFromFileBase> meta_data_read_stream
        = storage->readFile(this->index_meta_file_name, {}, std::nullopt, std::nullopt);
    std::unique_ptr<ReadBufferFromFileBase> index_data_read_stream
        = storage->readFile(this->index_data_file_name, {}, std::nullopt, std::nullopt);

    // read SkipIndex meta file in data part.
    size_t metas_size = 0;
    meta_data_read_stream->readStrict(reinterpret_cast<char *>(&metas_size), sizeof(size_t));
    IndexFileMetas metas(metas_size);
    meta_data_read_stream->readStrict(reinterpret_cast<char *>(metas.data()), metas_size * sizeof(IndexFileMeta));

    LOG_INFO(
        this->log,
        "[extract] Extracting SkipIndex files from data_part: {} to index_store:{}, total files: {}.",
        storage->getRelativePath(),
        index_files_directory,
        metas_size);

    // Extract SkipIndex data file in data part.
    for (size_t i = 0; i < metas_size; i++)
    {
        try
        {
            UInt64 file_size = metas[i].offset_end - metas[i].offset_begin;
            index_data_read_stream->seek(metas[i].offset_begin, SEEK_SET);
            std::unique_ptr<WriteBufferFromFileBase> temp_data_write_stream
                = this->tmp_disk->writeFile(index_files_directory + metas[i].file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
            char * buffer = new char[file_size];
            index_data_read_stream->readStrict(buffer, file_size);
            temp_data_write_stream->write(buffer, file_size);
            temp_data_write_stream->finalize();
            delete[] buffer;
            LOG_TRACE(
                log,
                "[extract] skip index file [file_idx:{}, file_name:{}] from data_part:{} to index_store:{}",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_directory);
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "[extract] Can't extract skip index file [file_idx:{}, file_name:{}] from data_part:{} to index_store:{}",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_directory);

            throw DB::Exception(
                DB::ErrorCodes::CUSTOM_SKIP_INDEX_FILES_EXTRACT_ERROR,
                "Can't extract skip index file [file_idx:{}, file_name:{}] from data_part:{} to index_store:{}",
                i,
                metas[i].file_name,
                storage->getRelativePath(),
                index_files_directory);
        }
    }
}


}
