//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  Page *mypage;
  frame_id_t new_frame;

  if (!free_list_.empty()) {
    new_frame = free_list_.front();
    free_list_.pop_front();
    mypage = pages_ + new_frame;
  } else {
    if (!replacer_->Evict(&new_frame)) {
      return nullptr;
    }
    mypage = pages_ + new_frame;
  }
  *page_id = AllocatePage();
  if (mypage->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, mypage->GetData(), mypage->GetPageId(), std::move(promise)});
    future.get();
    mypage->is_dirty_ = false;
  }
  page_table_.erase(mypage->GetPageId());
  page_table_.insert(std::make_pair(*page_id, new_frame));
  mypage->pin_count_ = 1;
  mypage->page_id_ = *page_id;
  mypage->ResetMemory();
  replacer_->RecordAccess(new_frame);
  replacer_->SetEvictable(new_frame, false);
  return mypage;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  Page *mypage;
  frame_id_t new_frame;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    new_frame = page_table_[page_id];
    mypage = pages_ + new_frame;
    mypage->pin_count_++;
    replacer_->RecordAccess(new_frame);
    replacer_->SetEvictable(new_frame, false);
    return mypage;
  }
  {
    if (!free_list_.empty()) {
      new_frame = free_list_.front();
      free_list_.pop_front();
      mypage = pages_ + new_frame;
    } else {
      if (!replacer_->Evict(&new_frame)) {
        return nullptr;
      }
      mypage = pages_ + new_frame;
    }
    if (mypage->IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, mypage->GetData(), mypage->GetPageId(), std::move(promise)});
      future.get();
      mypage->is_dirty_ = false;
    }
    page_table_.erase(mypage->GetPageId());
    page_table_.insert(std::make_pair(page_id, new_frame));
    mypage->pin_count_ = 1;
    mypage->page_id_ = page_id;

    mypage->ResetMemory();
    replacer_->RecordAccess(new_frame);
    replacer_->SetEvictable(new_frame, false);
  }
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, mypage->GetData(), mypage->GetPageId(), std::move(promise)});
  future.get();
  return mypage;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::lock_guard<std::mutex> guard(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || (pages_ + it->second)->pin_count_ == 0) {
    return false;
  }
  auto new_frame = it->second;
  Page *mypage = pages_ + new_frame;
  if (--mypage->pin_count_ == 0) {
    replacer_->SetEvictable(new_frame, true);
  }
  mypage->is_dirty_ = is_dirty || mypage->is_dirty_;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  Page *mypage = pages_ + it->second;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, mypage->GetData(), mypage->GetPageId(), std::move(promise)});
  future.get();

  mypage->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &elem : page_table_) {
    FlushPage(elem.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  auto frame_id = it->second;
  Page *mypage = pages_ + frame_id;
  if (mypage->GetPinCount() > 0) {
    return false;
  }
  if (mypage->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, mypage->GetData(), mypage->GetPageId(), std::move(promise)});
    future.get();
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  mypage->ResetMemory();
  mypage->pin_count_ = 0;
  mypage->page_id_ = INVALID_PAGE_ID;
  mypage->is_dirty_ = false;
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
