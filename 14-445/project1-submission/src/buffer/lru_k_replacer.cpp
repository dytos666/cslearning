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
#include <algorithm>
#include <cstddef>
#include <exception>
#include <mutex>
#include <utility>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  node_store_.reserve(replacer_size_);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  auto it = std::find_if(not_k_.begin(), not_k_.end(), [](const LRUKNode *node) { return node->is_evictable_; });
  if (it != not_k_.end()) {
    *frame_id = (*it)->fid_;
    node_store_.erase(*frame_id);
    not_k_.erase(it);
    --curr_size_;
    return true;
  }

  it = std::find_if(has_k_.begin(), has_k_.end(), [](const LRUKNode *node) { return node->is_evictable_; });
  if (it != has_k_.end()) {
    *frame_id = (*it)->fid_;
    node_store_.erase(*frame_id);
    has_k_.erase(it);
    --curr_size_;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  if (replacer_size_ < static_cast<size_t>(frame_id)) {
    throw std::exception();
  }
  ++current_timestamp_;
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    node_store_.insert(std::make_pair(frame_id, LRUKNode()));
    auto &new_node = node_store_[frame_id];
    new_node.is_evictable_ = true;
    new_node.fid_ = frame_id;
    new_node.latest_time_stamp_ = current_timestamp_;
    ++curr_size_;
  }

  it = node_store_.find(frame_id);

  auto &node = it->second;
  node.history_.push_front(current_timestamp_);

  size_t new_time_stamp;

  if (node.history_.size() > k_) {
    node.history_.pop_back();
    new_time_stamp = node.history_.back();
    has_k_.erase(&node);
    node.latest_time_stamp_ = new_time_stamp;
    has_k_.insert(&node);
    return;
  }

  if (node.history_.size() == k_) {
    new_time_stamp = node.history_.back();
    node.latest_time_stamp_ = new_time_stamp;
    not_k_.erase(&node);
    has_k_.insert(&node);

    return;
  }
  not_k_.erase(&node);

  not_k_.insert(&node);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (replacer_size_ < static_cast<size_t>(frame_id)) {
    throw std::exception();
  }
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  if (it->second.is_evictable_ && !set_evictable) {
    --curr_size_;
  } else if (!it->second.is_evictable_ && set_evictable) {
    ++curr_size_;
  }
  it->second.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  if (!it->second.is_evictable_) {
    throw std::exception();
  }
  auto n = it->second.history_.size();
  if (n == k_) {
    has_k_.erase(&it->second);
  } else {
    not_k_.erase(&it->second);
  }
  --curr_size_;

  node_store_.erase(it);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
