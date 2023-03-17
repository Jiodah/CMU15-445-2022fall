//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = this->IndexOf(key);
  auto ans = dir_[index]->Find(key, value);
  return ans;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = this->IndexOf(key);
  auto ans = dir_[index]->Remove(key);
  return ans;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  bucket->IncrementDepth();
  int depth = bucket->GetDepth();
  num_buckets_++;
  std::shared_ptr<Bucket> p(new Bucket(bucket_size_, depth));
  size_t preidx = std::hash<K>()((*(bucket->GetItems().begin())).first) & ((1 << (depth - 1)) - 1);
  for (auto it = bucket->GetItems().begin(); it != bucket->GetItems().end();) {
    size_t idx = std::hash<K>()((*it).first) & ((1 << depth) - 1);
    if (idx != preidx) {
      p->Insert((*it).first, (*it).second);  // 1xxx
      bucket->GetItems().erase(it++);
    } else {
      it++;
    }
  }
  for (size_t i = 0; i < dir_.size(); i++) {
    // 1xxx
    if ((i & ((1 << (depth - 1)) - 1)) == preidx && (i & ((1 << depth) - 1)) != preidx) {
      dir_[i] = p;
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    size_t index = this->IndexOf(key);
    bool flag = dir_[index]->Insert(key, value);
    if (flag) {
      break;
    }
    if (GetLocalDepthInternal(index) != GetGlobalDepthInternal()) {
      RedistributeBucket(dir_[index]);
    } else {
      global_depth_++;
      size_t dir_size = dir_.size();
      for (size_t i = 0; i < dir_size; i++) {
        dir_.emplace_back(dir_[i]);
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if ((*it).first == key) {
      value = (*it).second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end();) {
    if ((*it).first == key) {
      list_.erase(it++);
      return true;
    }
    it++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      (*it).second = value;
      return true;
    }
  }
  if (this->IsFull()) {
    return false;
  }
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
