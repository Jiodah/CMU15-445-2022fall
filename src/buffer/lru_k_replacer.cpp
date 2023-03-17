//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Judge(frame_id_t s, frame_id_t t) -> bool {
  if (hash_[s].time_.size() < k_ && hash_[t].time_.size() == k_) {
    return true;
  }
  if (hash_[s].time_.size() == k_ && hash_[t].time_.size() < k_) {
    return false;
  }
  return hash_[s].time_.front() < hash_[t].time_.front();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  *frame_id = -1;
  for (const auto &kv : hash_) {
    if (kv.second.evictable_) {
      if (*frame_id == -1 || Judge(kv.first, *frame_id)) {
        *frame_id = kv.first;
      }
    }
  }
  if (*frame_id != -1) {
    hash_.erase(*frame_id);
    curr_size_--;

    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (hash_.count(frame_id) == 0 && replacer_size_ == hash_.size()) {
    return;
  }
  if (hash_[frame_id].time_.size() == k_) {
    hash_[frame_id].time_.pop();
  }
  hash_[frame_id].time_.push(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (hash_.count(frame_id) != 0) {
    bool flag = hash_[frame_id].evictable_;
    hash_[frame_id].evictable_ = set_evictable;
    if (!flag && set_evictable) {
      curr_size_++;
    } else if (flag && !set_evictable) {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (hash_.count(frame_id) == 0) {
    return;
  }
  if (!hash_[frame_id].evictable_) {
    throw "Remove a non-evictable frame!";
  }
  hash_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
