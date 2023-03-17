//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &keyComparator) -> ValueType {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) <= 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return array_[r - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const MappingType &value, const KeyComparator &keyComparator) -> void {
  for (int i = GetSize() - 1; i > 0; i--) {
    if (keyComparator(array_[i].first, value.first) > 0) {
      array_[i + 1] = array_[i];
    } else {
      array_[i + 1] = value;
      IncreaseSize(1);
      return;
    }
  }
  SetKeyAt(1, value.first);
  SetValueAt(1, value.second);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyType &key, Page *page_bother, Page *page_parent_page,
                                           const KeyComparator &keyComparator,
                                           BufferPoolManager *buffer_pool_manager_) {
  auto *tmp = static_cast<MappingType *>(malloc(sizeof(MappingType) * (GetMaxSize() + 1)));
  // 将 key，page_bother->GetPageId() 插入，利用 tmp 来防止溢出
  bool flag = true;
  tmp[0] = array_[0];
  for (int i = 1; i < GetMaxSize(); i++) {
    if (keyComparator(array_[i].first, key) < 0) {
      tmp[i] = array_[i];
    } else if (flag && keyComparator(array_[i].first, key) > 0) {
      flag = false;
      tmp[i] = std::make_pair(key, page_bother->GetPageId());
      tmp[i + 1] = array_[i];
    } else {
      tmp[i + 1] = array_[i];
    }
  }
  if (flag) {
    tmp[GetMaxSize()] = std::make_pair(key, page_bother->GetPageId());
  }
  auto page_bother_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page_bother->GetData());
  page_bother_node->SetParentPageId(GetPageId());
  IncreaseSize(1);
  // split，前半截不动，后半截移动到新的页上
  int mid = (GetMaxSize() + 1) / 2;
  auto page_parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page_parent_page->GetData());
  for (int i = 0; i < mid; i++) {
    array_[i] = tmp[i];
  }
  int i = 0;
  while (mid <= (GetMaxSize())) {
    Page *child = buffer_pool_manager_->FetchPage(tmp[mid].second);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());
    child_node->SetParentPageId(page_parent_node->GetPageId());
    page_parent_node->array_[i++] = tmp[mid++];
    page_parent_node->IncreaseSize(1);
    IncreaseSize(-1);
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }
  free(tmp);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &keyComparator) -> bool {
  int index = KeyIndex(key, keyComparator);
  if (index >= GetSize() || keyComparator(KeyAt(index), key) != 0) {
    return false;
  }
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) -> int {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBotherPage(page_id_t child_page_id, Page *&bother_page, KeyType &key,
                                                   bool &ispre, BufferPoolManager *buffer_pool_manager_) -> void {
  int i = 0;
  for (i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == child_page_id) {
      break;
    }
  }
  if (i >= 1) {
    bother_page = buffer_pool_manager_->FetchPage(ValueAt(i - 1));
    key = KeyAt(i);
    ispre = true;
    return;
  }
  bother_page = buffer_pool_manager_->FetchPage(ValueAt(i + 1));
  key = KeyAt(i + 1);
  ispre = false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const KeyType &key, Page *right_page,
                                           BufferPoolManager *buffer_pool_manager_) -> void {
  auto right = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(right_page->GetData());
  int size = GetSize();
  array_[GetSize()] = std::make_pair(key, right->ValueAt(0));
  IncreaseSize(1);
  for (int i = GetSize(), j = 1; j < right->GetSize(); i++, j++) {
    array_[i] = std::make_pair(right->KeyAt(j), right->ValueAt(j));
    IncreaseSize(1);
  }
  buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  buffer_pool_manager_->DeletePage(right->GetPageId());
  for (int i = size; i < GetSize(); i++) {
    page_id_t child_page_id = ValueAt(i);
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  SetValueAt(0, value);
  SetKeyAt(1, key);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst() -> void {
  for (int i = 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
