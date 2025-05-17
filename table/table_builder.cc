// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;              // data block的选项
  Options index_block_options;  // index block的选项
  WritableFile* file;           // sstable 文件
  uint64_t offset;  // 要写入data block在sstable文件中的偏移，初始0
  Status status;    // 当前状态-初始ok
  BlockBuilder data_block;   // 当前操作的data block
  BlockBuilder index_block;  // sstable的index block
  std::string last_key;      // 当前data block最后的k/v对的key
  int64_t num_entries;       // 当前data block的个数，初始0
  bool closed;               // Either Finish() or Abandon() has been called.
                             // //调用了Finish() or Abandon()，初始false
  FilterBlockBuilder* filter_block;  // 根据filter数据快速定位key是否在block中

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;    // 是否写入 index_block 数据;
                               // 达到一定阈值后,才会写入;
  BlockHandle pending_handle;  // Handle to add to index block //添加到index
                               // block的data block的信息

  std::string compressed_output;  // 压缩后的data
                                  // block，临时存储，写入后即被清空
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  // 如果标记r->pending_index_entry为true, 表明遇到了 下一个data
  // block的第一个kv;
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 寻找一个 大于等于当前block的最大key && 小于下一个 block最小key的key,
    // 来当上一个block的索引, 进一步缩短空间;
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    // 直到遇到下一个databock的第一个key时，我们才为上一个datablock生成index
    // entry， 这样的好处是：可以为index使用较短的key；比如上一个data
    // block最后一个k/v的key是"the quick brown fox"， 其后继data
    // block的第一个key是"the who"，我们就可以用一个较短的字符串"the
    // r"作为上一个data block的 index block entry的key;
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  // 每一个 key 都进行填入过滤器中;
  if (r->filter_block != nullptr) {
    // 添加到 过滤器中;
    r->filter_block->AddKey(key);
  }

  // 设置r->last_key = key，将(key, value)添加到r->data_block中，并更新entry数;
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;  // kv对增加
  // block 进行 Add();
  r->data_block.Add(key, value);

  // 如果data block的个数超过限制，就立刻Flush到文件中
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    // 会开启新的的 过滤器;
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;       // 所有数据;
  assert(!r->closed);  // 首先保证未关闭，且状态ok;
  if (!ok()) return;
  if (r->data_block.empty()) return;
  // 保证pending_index_entry为false，即data block的Add已经完成;
  assert(!r->pending_index_entry);
  // 写入 data block, 并设置其index entry信息 BlockHandle对象;
  WriteBlock(&r->data_block, &r->pending_handle);

  // 写入成功，则Flush文件，并设置r->pending_index_entry为true,
  // 以根据下一个data block的first key调整index entry的key—即r->last_key
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }

  // 开启新的 过滤器 block;
  if (r->filter_block != nullptr) {
    // 将 data block 在 sstable 中的便宜加入到 filter block 中;
    r->filter_block->StartBlock(r->offset);
    // 并指明开始新的 data block;
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 获得data block的序列化字符串
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      // snappy压缩格式
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just store
        // uncompressed form
        // 如果不支持Snappy，或者压缩率低于12.5%，依然当作不压缩存储
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }

    case kZstdCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Zstd_Compress(r->options.zstd_compression_level, raw.data(),
                              raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Zstd not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 将data内容写入到文件, 并重置block成初始化状态，清空 compressed out put
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  // 为index设置data block的handle信息
  handle->set_size(block_contents.size());
  // 写入data block内容
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    // 写入1byte的type和4bytes的crc32
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 写入成功更新offset-下一个data block的写入偏移
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 首先调用Flush，写入最后的一块data
  // block，然后设置关闭标志closed=true。表明该sstable已经关闭，不能再添加k/v对
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    // 写入filter block到文件中
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // 如果filterblock不为NULL，则加入从"filter.Name"到filter data位置的映射。
  // 通过meta index block，可以根据filter名字快速定位到filter的数据区
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  // 写入index block，如果成功Flush过data block，那么需要为最后一块data
  // block设置index block， 并加入到index block中。
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
