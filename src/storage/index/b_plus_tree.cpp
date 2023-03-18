#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  auto page = FindLeafPageRW(key, transaction, READ);
  if (page == nullptr) {
    return false;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf_page->KeyIndex(key, comparator_);
  bool find = false;
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->push_back(leaf_page->ValueAt(index));
    find = true;
  }
  if (transaction == nullptr) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  } else {
    UnlockAndUnpin(transaction, READ);
  }
  return find;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageRW(const KeyType &key, Transaction *transaction, Operation op) -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  latch_.lock();
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  while (true) {
    if (curr_page == nullptr) {
      latch_.unlock();
      return nullptr;
    }
    if (op == Operation::READ) {
      curr_page->RLatch();
    } else {
      curr_page->WLatch();
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(curr_page);
    }
    if (root_page_id_ == curr_page->GetPageId()) {
      break;
    }
    if (op == Operation::READ) {
      if (transaction != nullptr) {
        UnlockAndUnpin(transaction, op);
      } else {
        curr_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
      }
    } else {
      UnlockAndUnpin(transaction, op);
    }
    curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  }
  assert(root_page_id_ == curr_page->GetPageId());
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->Lookup(key, comparator_));
    if (op == Operation::READ) {
      next_page->RLatch();
      if (transaction != nullptr) {
        UnlockAndUnpin(transaction, op);
      } else {
        curr_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
      }
    } else {
      next_page->WLatch();
      if (IsSafe(next_page, op)) {
        UnlockAndUnpin(transaction, op);
      }
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(next_page);
    }
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  latch_.unlock();
  return curr_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(Page *page, Operation op) -> bool {
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // 插入时，如果有一个页是非满的，那么上面的锁都能释放了，因为不会分裂到这之上了
  if (op == Operation::INSERT) {
    return node->GetSize() < (node->IsLeafPage() ? leaf_max_size_ - 1 : internal_max_size_);
  }
  // 删除，如果有一个页比 minSize 大，那么删除后不会影响到父页，可以认为安全
  return node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UnlockAndUnpin(Transaction *transaction, Operation op) -> void {
  if (transaction == nullptr) {
    return;
  }
  for (auto page : *transaction->GetPageSet()) {
    if (op == Operation::READ) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();
  for (auto page : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page);
  }
  transaction->GetDeletedPageSet()->clear();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *page_leaf = FindLeafPageRW(key, transaction, INSERT);
  while (page_leaf == nullptr) {
    latch_.lock();
    if (IsEmpty()) {
      page_id_t page_id;
      Page *page = buffer_pool_manager_->NewPage(&page_id);
      auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
      root_page_id_ = page_id;
      UpdateRootPageId(true);
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
    latch_.unlock();
    page_leaf = FindLeafPageRW(key, transaction, INSERT);
  }
  auto leaf_node = reinterpret_cast<LeafPage *>(page_leaf->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  bool retcode = leaf_node->Insert(std::make_pair(key, value), index, comparator_);
  if (!retcode) {
    UnlockAndUnpin(transaction, INSERT);
    return false;
  }
  // 叶子页插入一项后，大小为 max_size，那么需要分裂
  if (leaf_node->GetSize() == leaf_max_size_) {
    page_id_t page_bother_id;
    Page *page_bother = buffer_pool_manager_->NewPage(&page_bother_id);
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(page_bother->GetData());
    leaf_bother_node->Init(page_bother_id, INVALID_PAGE_ID, leaf_max_size_);
    // 分裂，page_bother 为后半截
    leaf_node->Split(page_bother);
    // 父页需要插入一项，key = leaf_bother_node->KeyAt(0)，value = page_bother->GetPageId()
    InsertInParentRW(page_leaf, leaf_bother_node->KeyAt(0), page_bother, transaction);
  }
  UnlockAndUnpin(transaction, INSERT);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParentRW(Page *page_leaf, const KeyType &key, Page *page_bother, Transaction *transaction)
    -> void {
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());
  // rootpage，新建一个新的 rootpage
  if (tree_page->GetPageId() == root_page_id_) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    new_page->WLatch();
    auto new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetValueAt(0, page_leaf->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, page_bother->GetPageId());
    new_root->IncreaseSize(2);
    auto page_leaf_node = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());
    page_leaf_node->SetParentPageId(new_page_id);
    auto page_bother_node = reinterpret_cast<BPlusTreePage *>(page_bother->GetData());
    page_bother_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    UpdateRootPageId(false);
    transaction->GetPageSet()->pop_back();
    page_leaf->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page_bother->GetPageId(), true);
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return;
  }
  Page *parent_page = (*transaction->GetPageSet())[transaction->GetPageSet()->size() - 2];
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  page_id_t parent_id = parent_page->GetPageId();
  auto page_bother_node = reinterpret_cast<InternalPage *>(page_bother->GetData());
  // 父页有空间，那么插入即可
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    parent_node->Insert(std::make_pair(key, page_bother->GetPageId()), comparator_);
    page_bother_node->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(page_bother->GetPageId(), true);
    transaction->GetPageSet()->pop_back();
    page_leaf->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), true);
    return;
  }
  // 父页没有空间，调用父页分裂。先创建一个 tmp，使得新节点可以插入。然后分裂
  page_id_t page_parent_bother_id;
  Page *page_parent_bother = buffer_pool_manager_->NewPage(&page_parent_bother_id);
  auto parent_bother_node = reinterpret_cast<InternalPage *>(page_parent_bother->GetData());
  parent_bother_node->Init(page_parent_bother_id, INVALID_PAGE_ID, internal_max_size_);
  parent_node->Split(key, page_bother, page_parent_bother, comparator_, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(page_bother->GetPageId(), true);
  transaction->GetPageSet()->pop_back();
  page_leaf->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_leaf->GetPageId(), true);
  InsertInParentRW(parent_page, parent_bother_node->KeyAt(0), page_parent_bother, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto leaf_page = FindLeafPageRW(key, transaction, DELETE);
  if (leaf_page == nullptr) {
    return;
  }
  DeleteEntryRW(leaf_page, key, transaction);
  UnlockAndUnpin(transaction, DELETE);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntryRW(Page *&page, const KeyType &key, Transaction *transaction) -> void {
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (b_node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    if (!leaf_node->Delete(key, comparator_)) {
      return;
    }
  } else {
    auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
    if (!inter_node->Delete(key, comparator_)) {
      return;
    }
  }
  if (root_page_id_ == b_node->GetPageId()) {
    AdjustRootPageRW(page, transaction);
    return;
  }
  if (b_node->GetSize() < b_node->GetMinSize()) {
    Page *bother_page;
    KeyType parent_key{};
    bool ispre;
    auto parent_page = (*transaction->GetPageSet())[transaction->GetPageSet()->size() - 2];
    auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent_node->GetBotherPage(page->GetPageId(), bother_page, parent_key, ispre, buffer_pool_manager_);
    auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());
    if (bother_node->GetSize() + b_node->GetSize() <=
        (b_node->IsLeafPage() ? leaf_max_size_ - 1 : internal_max_size_)) {
      if (!ispre) {
        std::swap(page, bother_page);
      }
      CoalesceRW(page, bother_page, parent_key, transaction);
      DeleteEntryRW(parent_page, parent_key, transaction);
    } else {
      RedistributeRW(page, bother_page, parent_page, parent_key, ispre, transaction);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRootPageRW(Page *page, Transaction *transaction) {
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (b_node->IsLeafPage() && b_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
  }
  if (!b_node->IsLeafPage() && b_node->GetSize() == 1) {
    auto inter_node = reinterpret_cast<InternalPage *>(b_node);
    root_page_id_ = inter_node->ValueAt(0);
  }
  UpdateRootPageId(false);
  transaction->AddIntoDeletedPageSet(page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeRW(Page *page, Page *bother_page, Page *parent_page, const KeyType &parent_key,
                                    bool ispre, Transaction *transaction) {
  auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());
  // 如果 bother_node 是内部页
  if (!bother_node->IsLeafPage()) {
    auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
    auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
    // 拿出 bother 最后的元素，放到 page 的最前面， child 也需要随之改变
    Page *child_page;
    KeyType key;
    if (ispre) {
      page_id_t last_value = inter_bother_node->ValueAt(inter_bother_node->GetSize() - 1);
      KeyType last_key = inter_bother_node->KeyAt(inter_bother_node->GetSize() - 1);
      inter_bother_node->Delete(last_key, comparator_);
      bother_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(inter_bother_node->GetPageId(), true);
      inter_b_node->InsertFirst(parent_key, last_value);
      child_page = buffer_pool_manager_->FetchPage(last_value);
      key = last_key;
    } else {
      page_id_t first_value = inter_bother_node->ValueAt(0);
      KeyType first_key = inter_bother_node->KeyAt(1);
      inter_bother_node->DeleteFirst();
      bother_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(inter_bother_node->GetPageId(), true);
      inter_b_node->Insert(std::make_pair(parent_key, first_value), comparator_);
      child_page = buffer_pool_manager_->FetchPage(first_value);
      key = first_key;
    }
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    if (child_node->IsLeafPage()) {
      auto leaf_child_node = reinterpret_cast<LeafPage *>(child_page->GetData());
      leaf_child_node->SetParentPageId(inter_b_node->GetPageId());
    } else {
      auto inter_child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
      inter_child_node->SetParentPageId(inter_b_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
    auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    int index = inter_parent_node->KeyIndex(parent_key, comparator_);
    inter_parent_node->SetKeyAt(index, key);
  } else {  // bother 是叶子页
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
    auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
    // 拿出 bother 最后的元素，放到 page 的最前面， 没有 child
    KeyType key;
    if (ispre) {
      ValueType last_value = leaf_bother_node->ValueAt(leaf_bother_node->GetSize() - 1);
      KeyType last_key = leaf_bother_node->KeyAt(leaf_bother_node->GetSize() - 1);
      leaf_bother_node->Delete(last_key, comparator_);
      bother_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_bother_node->GetPageId(), true);
      leaf_b_node->InsertFirst(last_key, last_value);
      key = last_key;
    } else {
      ValueType first_value = leaf_bother_node->ValueAt(0);
      KeyType first_key = leaf_bother_node->KeyAt(0);
      leaf_bother_node->Delete(first_key, comparator_);
      bother_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_bother_node->GetPageId(), true);
      leaf_b_node->InsertLast(first_key, first_value);
      key = leaf_bother_node->KeyAt(0);
    }
    auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    int index = inter_parent_node->KeyIndex(parent_key, comparator_);
    inter_parent_node->SetKeyAt(index, key);
  }
  transaction->GetPageSet()->pop_back();
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceRW(Page *page, Page *bother_page, const KeyType &parent_key, Transaction *transaction) {
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (b_node->IsLeafPage()) {
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
    auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_bother_node->Merge(page, buffer_pool_manager_);
    leaf_bother_node->SetNextPageId(leaf_b_node->GetNextPageId());
  } else {
    auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
    inter_bother_node->Merge(parent_key, page, buffer_pool_manager_);
  }
  bother_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
  transaction->GetPageSet()->pop_back();
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->DeletePage(page->GetPageId());
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  curr_page->RLatch();
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->ValueAt(0));
    next_page->RLatch();
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  return INDEXITERATOR_TYPE(curr_page, 0, curr_page->GetPageId(), buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  auto leaf_page = FindLeafPageRW(key, nullptr, READ);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index;
  for (index = 0; index < leaf_node->GetSize(); index++) {
    if (comparator_(leaf_node->KeyAt(index), key) == 0) {
      break;
    }
  }
  if (index == leaf_node->GetSize()) {
    leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return End();
  }
  return INDEXITERATOR_TYPE(leaf_page, index, leaf_page->GetPageId(), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  curr_page->RLatch();
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->ValueAt(curr_page_inter->GetSize() - 1));
    next_page->RLatch();
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  auto curr_node = reinterpret_cast<LeafPage *>(curr_page->GetData());
  page_id_t page_id = curr_page->GetPageId();
  curr_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
  return INDEXITERATOR_TYPE(curr_page, curr_node->GetSize(), page_id, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
