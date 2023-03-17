//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(Page *curr_page, int index, page_id_t page_id, BufferPoolManager *bufferPoolManager);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return static_cast<bool>(page_id_ == itr.page_id_ && index_ == itr.index_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !static_cast<bool>(page_id_ == itr.page_id_ && index_ == itr.index_);
  }

 private:
  page_id_t page_id_ = INVALID_PAGE_ID;
  Page *curr_page_ = nullptr;
  int index_ = 0;
  BufferPoolManager *buffer_pool_manager_ = nullptr;
};

}  // namespace bustub
