// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {

// |      blocks_       | blocks_ | blocks_ |
// |use;allptr;remaining|
class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;             // 不允许传参构造;
  Arena& operator=(const Arena&) = delete;  // 不允许直接赋值构造;

  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  // 对外暴露 内存对齐方式;
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated by the
  // arena.

  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  // 1. 内部分配空间逻辑;
  char* AllocateFallback(size_t bytes);
  // 1.1 底层分配逻辑;
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state;
  char* alloc_ptr_;               // 指向当前分配的内存块地址, 递增;
  size_t alloc_bytes_remaining_;  // 当前内存块的剩余空间;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;  // 内存块数组地址;

  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  std::atomic<size_t> memory_usage_;  // 原子类型, 内存使用量;
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
//  assert(bytes > 0);
  // static_assert( bytes > 0?1:0);
  // 所需空间 < 剩余空间, 直接分配即可;
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;        // 返回当前内存块指针;
    alloc_ptr_ += bytes;              // 为下次分配;
    alloc_bytes_remaining_ -= bytes;  // 可用空间减少;
    return result;
  }
  // 需要扩容;
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
