//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <functional>
#include <set>
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
auto DeleteTxnLockSetForTable(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    return;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // ABORTED 或者 COMMITED,直接退出
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    exit(-1);
  }
  /*可重复读,在 SHRINKING 状态不允许使用锁,GROWING 状态可以加任何锁
  读提交,在 SHRINKING 状态只能加 S/IS 锁,GROWING 状态可以加任何锁
  读未提交,不允许 S/IS/SIX 锁,SHRINKING 状态不允许加锁*/
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else {
    if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  for (auto iter : lock_request_queue->request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      if (iter->lock_mode_ == lock_mode) {
        return true;
      }
      // 还有其他的事务在升级
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // S 锁只能升级为 X 或者 SIX
      if (iter->lock_mode_ == LockMode::SHARED &&
          (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // IX 锁只能升级为 X 或者 SIX
      if (iter->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
          (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // SIX 锁只能升级为 X
      if (iter->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // X 锁无法升级
      if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 在 request queue 中删除原有锁
      lock_request_queue->request_queue_.remove(iter);
      // 在事务 txn 的持有的锁中删除原有锁
      DeleteTxnLockSetForTable(txn, iter->lock_mode_, oid);
      delete iter;
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);
  while (!lock_request_queue->GrantLockForTable(txn, lock_mode)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // std::cout << txn->GetTransactionId() << "  aborted" << std::endl;
      lock_request_queue->request_queue_.remove(lock_request);
      if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      delete lock_request;
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // // std::cout << txn->GetTransactionId() << "  " << int(txn->GetState()) << "  " << int(txn->GetIsolationLevel())
  //           << " unlock table " << oid << std::endl;
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  bool is_search = false;
  for (auto iter : lock_request_queue->request_queue_) {
    if (txn->GetTransactionId() == iter->txn_id_ && iter->granted_) {
      is_search = true;
      if ((txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (iter->lock_mode_ == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::SHARED)) ||
          (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           (iter->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::SHRINKING);
      }
      DeleteTxnLockSetForTable(txn, iter->lock_mode_, oid);
      lock_request_queue->request_queue_.remove(iter);
      delete iter;
      break;
    }
  }
  if (is_search) {
    lock_request_queue->cv_.notify_all();
    lock.unlock();
  } else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

auto DeleteTxnLockSetForRow(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid, const RID &rid)
    -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
    if ((*txn->GetSharedRowLockSet())[oid].empty()) {
      (*txn->GetSharedRowLockSet()).erase(oid);
    }
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
    if ((*txn->GetExclusiveRowLockSet())[oid].empty()) {
      (*txn->GetExclusiveRowLockSet()).erase(oid);
    }
    return;
  }
}
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // // std::cout << txn->GetTransactionId() << "  " << int(txn->GetState()) << "  " << int(txn->GetIsolationLevel())
  //           << "  lock row  " << oid << " " << rid << "  " << int(lock_mode) << std::endl;
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    exit(-1);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else {
    if (lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue_table = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  // 对行加 S 锁,需要保证事务对表有锁,任何锁都行
  // 对行加 X 锁,需要保证事务对表有 IX/X/SIX 锁
  if (lock_mode == LockMode::SHARED) {
    lock_request_queue_table->latch_.lock();
    bool table_present = false;
    for (auto &iter : lock_request_queue_table->request_queue_) {
      if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_ &&
          (iter->lock_mode_ == LockMode::SHARED || iter->lock_mode_ == LockMode::INTENTION_SHARED ||
           iter->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
           iter->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || iter->lock_mode_ == LockMode::EXCLUSIVE)) {
        table_present = true;
        break;
      }
    }
    lock_request_queue_table->latch_.unlock();
    if (!table_present) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    lock_request_queue_table->latch_.lock();
    bool table_present = false;
    for (auto &iter : lock_request_queue_table->request_queue_) {
      if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_ &&
          (iter->lock_mode_ == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
           iter->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        table_present = true;
        break;
      }
    }
    lock_request_queue_table->latch_.unlock();
    if (!table_present) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  for (auto iter : lock_request_queue->request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      if (iter->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // 不是从 S 升级到 X
      if (iter->lock_mode_ == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      lock_request_queue->request_queue_.remove(iter);
      DeleteTxnLockSetForRow(txn, iter->lock_mode_, iter->oid_, iter->rid_);
      delete iter;
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);
  while (!lock_request_queue->GrantLockForRow(txn, lock_mode)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // std::cout << txn->GetTransactionId() << "  aborted" << std::endl;
      lock_request_queue->request_queue_.remove(lock_request);
      if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      delete lock_request;
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // // std::cout << txn->GetTransactionId() << "  " << int(txn->GetState()) << "  " << int(txn->GetIsolationLevel()) <<
  // " unlock row " << oid << " " << rid << std::endl;
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  bool is_search = false;
  for (auto iter : lock_request_queue->request_queue_) {
    if (txn->GetTransactionId() == iter->txn_id_ && iter->granted_) {
      is_search = true;
      if ((txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) ||
          (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           (iter->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::SHRINKING);
      }
      lock_request_queue->request_queue_.remove(iter);
      DeleteTxnLockSetForRow(txn, iter->lock_mode_, oid, rid);
      delete iter;
      break;
    }
  }
  lock.unlock();
  lock_request_queue->cv_.notify_all();
  if (!is_search) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

auto Compatible(const std::set<LockManager::LockMode> &granted_set, const LockManager::LockMode &lock_mode) -> bool {
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    return granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    return granted_set.find(LockManager::LockMode::SHARED) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::SHARED) {
    return granted_set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return granted_set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    return granted_set.empty();
  }

  return false;
}
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> lock(waits_for_latch_);

  bool is_present = false;
  for (auto a : waits_for_[t1]) {
    if (a == t2) {
      break;
      is_present = true;
    }
  }
  if (!is_present) {
    waits_for_[t1].push_back(t2);
    std::sort(waits_for_[t1].begin(), waits_for_[t1].end());
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> lock(waits_for_latch_);
  for (auto iter = waits_for_[t1].begin(); iter != waits_for_[t1].begin(); iter++) {
    if (*iter == t2) {
      iter = waits_for_[t1].erase(iter);
    }
  }
  // std::remove(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
}
auto LockManager::DFS(std::vector<txn_id_t> cycle_vector, bool &is_cycle, txn_id_t *txn_id) -> void {
  if (waits_for_.find(cycle_vector[cycle_vector.size() - 1]) == waits_for_.end()) {
    return;
  }
  for (auto txn : waits_for_[cycle_vector[cycle_vector.size() - 1]]) {
    if (is_cycle) {
      return;
    }
    auto iter = std::find(cycle_vector.begin(), cycle_vector.end(), txn);
    if (iter != cycle_vector.end()) {
      is_cycle = true;
      *txn_id = *iter;
      while (iter != cycle_vector.end()) {
        if (*txn_id < *iter) {
          *txn_id = *iter;
        }
        iter++;
      }
      auto transaction = TransactionManager::GetTransaction(*txn_id);
      transaction->SetState(TransactionState::ABORTED);
    }
    if (!is_cycle) {
      cycle_vector.push_back(txn);
      DFS(cycle_vector, is_cycle, txn_id);
      cycle_vector.pop_back();
    }
  }
}
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> txn_vector;
  txn_vector.reserve(waits_for_.size());
  for (const auto &wait : waits_for_) {
    txn_vector.push_back(wait.first);
  }
  std::sort(txn_vector.begin(), txn_vector.end(), std::greater<>());
  for (auto txn : txn_vector) {
    std::vector<txn_id_t> cycle_vector;
    bool is_cycle = false;
    cycle_vector.push_back(txn);
    DFS(cycle_vector, is_cycle, txn_id);
    if (is_cycle) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &waits : waits_for_) {
    for (auto value : waits.second) {
      edges.emplace_back(waits.first, value);
    }
  }

  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      waits_for_latch_.lock();
      table_lock_map_latch_.lock();
      for (const auto &table_pairs : table_lock_map_) {
        table_pairs.second->latch_.lock();
        for (auto i_request : table_pairs.second->request_queue_) {
          for (auto j_request : table_pairs.second->request_queue_) {
            if (j_request->granted_ && !i_request->granted_ &&
                !Compatible({j_request->lock_mode_}, i_request->lock_mode_)) {
              AddEdge(i_request->txn_id_, j_request->txn_id_);
            }
          }
        }
        table_pairs.second->latch_.unlock();
      }
      table_lock_map_latch_.unlock();

      row_lock_map_latch_.lock();
      for (const auto &row_pairs : row_lock_map_) {
        row_pairs.second->latch_.lock();
        for (auto i_request : row_pairs.second->request_queue_) {
          for (auto j_request : row_pairs.second->request_queue_) {
            if (j_request->granted_ && !i_request->granted_ &&
                !Compatible({j_request->lock_mode_}, i_request->lock_mode_)) {
              AddEdge(i_request->txn_id_, j_request->txn_id_);
            }
          }
        }
        row_pairs.second->latch_.unlock();
      }
      row_lock_map_latch_.unlock();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        // auto transaction = TransactionManager::GetTransaction(txn_id);
        // transaction->LockTxn();
        // auto shared_row_lock_set = transaction->GetSharedRowLockSet();
        // while (!shared_row_lock_set->empty()) {
        //   auto oid = shared_row_lock_set->begin()->first;
        //   auto rid = *shared_row_lock_set->begin()->second.begin();
        //   UnlockRow(transaction, oid, rid);
        // }

        // auto exclusive_row_lock_set = transaction->GetExclusiveRowLockSet();
        // while (!exclusive_row_lock_set->empty()) {
        //   auto oid = exclusive_row_lock_set->begin()->first;
        //   auto rid = *exclusive_row_lock_set->begin()->second.begin();
        //   UnlockRow(transaction, oid, rid);
        // }

        // auto shared_table_lock_set = transaction->GetSharedTableLockSet();
        // while (!shared_table_lock_set->empty()) {
        //   auto oid = *shared_table_lock_set->begin();
        //   UnlockTable(transaction, oid);
        // }

        // auto shared_intention_exclusive_table_lock_set = transaction->GetSharedIntentionExclusiveTableLockSet();
        // while (!shared_intention_exclusive_table_lock_set->empty()) {
        //   auto oid = *shared_intention_exclusive_table_lock_set->begin();
        //   UnlockTable(transaction, oid);
        // }

        // auto intention_shared_table_lock_set = transaction->GetIntentionSharedTableLockSet();
        // while (!intention_shared_table_lock_set->empty()) {
        //   auto oid = *intention_shared_table_lock_set->begin();
        //   UnlockTable(transaction, oid);
        // }

        // auto exclusive_table_lock_set = transaction->GetExclusiveTableLockSet();
        // while (!exclusive_table_lock_set->empty()) {
        //   auto oid = *exclusive_table_lock_set->begin();
        //   UnlockTable(transaction, oid);
        // }

        // auto intention_exclusive_table_lock_set = transaction->GetIntentionExclusiveTableLockSet();
        // while (!intention_exclusive_table_lock_set->empty()) {
        //   auto oid = *intention_exclusive_table_lock_set->begin();
        //   UnlockTable(transaction, oid);
        // }
        // transaction->SetState(TransactionState::ABORTED);
        // transaction->UnlockTxn();

        for (const auto &wait : waits_for_) {
          RemoveEdge(wait.first, txn_id);
        }
        waits_for_.erase(txn_id);
        table_lock_map_latch_.lock();
        for (const auto &table_pairs : table_lock_map_) {
          table_pairs.second->cv_.notify_all();
        }
        table_lock_map_latch_.unlock();

        row_lock_map_latch_.lock();
        for (const auto &row_pairs : row_lock_map_) {
          row_pairs.second->cv_.notify_all();
        }
        row_lock_map_latch_.unlock();
      }
      // table_lock_map_latch_.unlock();
      // row_lock_map_latch_.unlock();
      waits_for_latch_.unlock();
    }
  }
}

auto AddTxnLockSetForTable(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
    return;
  }
}

auto AddTxnLockSetForRow(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid, RID &rid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
    return;
  }
}
auto LockManager::LockRequestQueue::GrantLockForTable(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  // // std::cout << txn->GetTransactionId() << " try to lock table" << std::endl;
  std::set<LockMode> granted_set;
  std::set<LockMode> wait_set;
  LockRequest *lock_request = nullptr;
  for (auto &iter : request_queue_) {
    if (iter->granted_) {
      granted_set.insert(iter->lock_mode_);
    }
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_request = iter;
    }
  }
  if (Compatible(granted_set, lock_mode)) {
    if (upgrading_ != INVALID_TXN_ID) {
      if (upgrading_ == txn->GetTransactionId()) {
        upgrading_ = INVALID_TXN_ID;
        if (lock_request != nullptr) {
          lock_request->granted_ = true;
          AddTxnLockSetForTable(txn, lock_mode, lock_request->oid_);
          // std::cout << txn->GetTransactionId() << " grant lock table" << std::endl;
          return true;
        }
      }
      return false;
    }
    for (auto &iter : request_queue_) {
      if (iter->txn_id_ != txn->GetTransactionId()) {
        if (!iter->granted_) {
          wait_set.insert(iter->lock_mode_);
        }
      } else {
        break;
      }
    }
    if (Compatible(wait_set, lock_mode)) {
      if (lock_request != nullptr) {
        lock_request->granted_ = true;
        AddTxnLockSetForTable(txn, lock_mode, lock_request->oid_);
        // // std::cout << txn->GetTransactionId() << " grant lock table" << std::endl;
        return true;
      }
    }
  }
  return false;
}
auto LockManager::LockRequestQueue::GrantLockForRow(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  // std::cout << txn->GetTransactionId() << " try to lock row" << std::endl;
  std::set<LockMode> granted_set;
  std::set<LockMode> wait_set;
  LockRequest *lock_request = nullptr;
  for (auto iter : request_queue_) {
    if (iter->granted_) {
      granted_set.insert(iter->lock_mode_);
    }
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_request = iter;
    }
  }
  if (Compatible(granted_set, lock_mode)) {
    if (upgrading_ != INVALID_TXN_ID) {
      if (upgrading_ == txn->GetTransactionId()) {
        upgrading_ = INVALID_TXN_ID;
        if (lock_request != nullptr) {
          lock_request->granted_ = true;
          AddTxnLockSetForRow(txn, lock_mode, lock_request->oid_, lock_request->rid_);
          // std::cout << txn->GetTransactionId() << " grant lock row" << std::endl;
          return true;
        }
      }
      return false;
    }

    for (auto &iter : request_queue_) {
      if (iter->txn_id_ != txn->GetTransactionId()) {
        if (!iter->granted_) {
          wait_set.insert(iter->lock_mode_);
        }
      } else {
        break;
      }
    }
    if (Compatible(wait_set, lock_mode)) {
      if (lock_request != nullptr) {
        lock_request->granted_ = true;
        AddTxnLockSetForRow(txn, lock_mode, lock_request->oid_, lock_request->rid_);
        // std::cout << txn->GetTransactionId() << " grant lock row" << std::endl;
        return true;
      }
    }
  }
  return false;
}
}  // namespace bustub
