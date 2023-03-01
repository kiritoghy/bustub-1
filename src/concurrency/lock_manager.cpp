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

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // check if the transaction is aborted or committed
  LOG_INFO("txn %d try acquire %d table lock", txn->GetTransactionId(), lock_mode);
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_INFO("Txn%d TABLE_LOCK_NOT_PRESENT", txn->GetTransactionId());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  // check if the transaction is growing or shrinking
  txn->LockTxn();
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }
  txn->UnlockTxn();

  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        txn->UnlockTxn();
        return false;
      }
    }
  }

  std::unique_lock<std::mutex> lock(table_lock_map_latch_);
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    // no lock on the table
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  lock.unlock();

  // check lock upgrade
  std::unique_lock<std::mutex> lock2(lock_request_queue->latch_);
  auto iter = lock_request_queue->request_queue_.begin();
  for (; iter != lock_request_queue->request_queue_.end(); iter++) {
    if (iter->get()->txn_id_ == txn->GetTransactionId()) {
      // try to upgrade the lock
      LOG_INFO("txn%d try tp upgrade lock", txn->GetTransactionId());
      LOG_INFO("size:%zu", lock_request_queue->request_queue_.size());
      BUSTUB_ASSERT(iter->get()->granted_, "lock request should be granted");

      if (iter->get()->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        // another transaction is upgrading the lock
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        txn->UnlockTxn();
        return false;
      }
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      if (iter->get()->lock_mode_ == LockMode::INTENTION_SHARED) {
        if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE ||
            lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          lock_request_queue->request_queue_.erase(iter);
          txn->LockTxn();
          txn->GetIntentionSharedTableLockSet()->erase(oid);
          txn->UnlockTxn();
          break;
        }
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        return false;
      }
      if (iter->get()->lock_mode_ == LockMode::SHARED) {
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
          lock_request_queue->request_queue_.erase(iter);
          txn->LockTxn();
          txn->GetSharedTableLockSet()->erase(oid);
          txn->UnlockTxn();
          break;
        }
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        return false;
      }
      if (iter->get()->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        if (lock_mode == LockMode::EXCLUSIVE) {
          lock_request_queue->request_queue_.erase(iter);
          txn->LockTxn();
          txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
          txn->UnlockTxn();
          break;
        }
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        return false;
      }
      txn->LockTxn();
      txn->SetState(TransactionState::ABORTED);
      txn->UnlockTxn();
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      return false;
    }
  }
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    auto it = lock_request_queue->request_queue_.begin();
    for (; it != lock_request_queue->request_queue_.end(); ++it) {
      if (!it->get()->granted_) {
        break;
      }
    }
    lock_request_queue->request_queue_.emplace(it,
                                               std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
  } else {
    lock_request_queue->request_queue_.emplace_back(
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
  }

  lock2.unlock();

  // wait for the lock
  std::unique_lock<std::mutex> lock3(lock_request_queue->latch_);
  while (true) {
    try {
      bool res = GrantLock(txn, lock_mode, oid);
      if (res) {
        break;
      }
      lock_request_queue->cv_.wait(lock3);
    } catch (TransactionAbortException e) {
      LOG_INFO("Txn %d aborted while grantlock", txn->GetTransactionId());
      return false;
    }
  }
  LOG_INFO("txn:%d lock table %d", txn->GetTransactionId(), oid);
  return true;
}
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // if (txn->GetState() == TransactionState::ABORTED) {
  //   LOG_INFO("transaction %d is aborted", txn->GetTransactionId());
  //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  //   return false;
  // }

  // check if all row locks are released
  if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
       !txn->GetSharedRowLockSet()->find(oid)->second.empty()) ||
      (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() &&
       !txn->GetExclusiveRowLockSet()->find(oid)->second.empty())) {
    txn->SetState(TransactionState::ABORTED);
    LOG_INFO("transaction %d is aborted", txn->GetTransactionId());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  std::unique_lock<std::mutex> lock(table_lock_map_latch_);
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    // no lock on the table
    txn->SetState(TransactionState::ABORTED);
    LOG_INFO("transaction %d is aborted", txn->GetTransactionId());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  auto lock_request_queue = table_lock_map_[oid];
  lock.unlock();

  std::unique_lock<std::mutex> lock2(lock_request_queue->latch_);
  auto iter = lock_request_queue->request_queue_.begin();
  for (; iter != lock_request_queue->request_queue_.end(); ++iter) {
    if (iter->get()->oid_ == oid) {
      break;
    }
  }
  if (iter == lock_request_queue->request_queue_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto lock_mode = iter->get()->lock_mode_;
  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    if ((iter->get()->lock_mode_ == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) ||
        iter->get()->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  // LOG_INFO("before queuesize:%zu", lock_request_queue->request_queue_.size());
  lock_request_queue->request_queue_.erase(iter);
  // LOG_INFO("queuesize:%zu", lock_request_queue->request_queue_.size());
  lock2.unlock();

  txn->LockTxn();
  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
  txn->UnlockTxn();

  lock_request_queue->cv_.notify_all();
  LOG_INFO("txn:%d unlock table %d", txn->GetTransactionId(), oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  // Row locking should not support Intention locks.
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::INTENTION_SHARED) {
    LOG_INFO("txn: %d ATTEMPTED_INTENTION_LOCK_ON_ROW", txn->GetTransactionId());
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      LOG_INFO("txn %d TABLE_LOCK_NOT_PRESENT", txn->GetTransactionId());
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }

  if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedLocked(oid) &&
        !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      LOG_INFO("txn %d TABLE_LOCK_NOT_PRESENT", txn->GetTransactionId());
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }

  // check if the transaction is growing or shrinking
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
  }

  std::unique_lock<std::mutex> lock(row_lock_map_latch_);
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    // no lock on the table
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock.unlock();

  // check lock upgrade
  std::unique_lock<std::mutex> lock2(lock_request_queue->latch_);
  auto iter = lock_request_queue->request_queue_.begin();
  for (; iter != lock_request_queue->request_queue_.end(); iter++) {
    if (iter->get()->txn_id_ == txn->GetTransactionId()) {
      // try to upgrade the lock
      BUSTUB_ASSERT(iter->get()->granted_, "lock request should be granted");
      if (iter->get()->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        // another transaction is upgrading the lock
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      if (iter->get()->lock_mode_ == LockMode::SHARED) {
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
          lock_request_queue->request_queue_.erase(iter);
          txn->GetSharedRowLockSet()->erase(oid);
          break;
        }
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        return false;
      }
      // Exclusive lock
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      return false;
    }
  }
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    // 升级锁，将锁请求放到第一个未获取的位置
    auto it = lock_request_queue->request_queue_.begin();
    for (; it != lock_request_queue->request_queue_.end(); ++it) {
      if (!it->get()->granted_) {
        break;
      }
    }
    lock_request_queue->request_queue_.emplace(
        it, std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
  } else {
    // normal lock require
    lock_request_queue->request_queue_.emplace_back(
        std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
  }

  lock2.unlock();

  std::unique_lock<std::mutex> lock3(lock_request_queue->latch_);
  while (true) {
    try {
      bool res = GrantLock(txn, lock_mode, oid, rid);
      if (res) {
        break;
      }
      lock_request_queue->cv_.wait(lock3);
    } catch (TransactionAbortException e) {
      return false;
    }
  }
  LOG_INFO("txn%d lock row", txn->GetTransactionId());
  return true;
}

auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  std::unique_lock<std::mutex> lock(row_lock_map_latch_);
  for (auto &lock_request : row_lock_map_[rid]->request_queue_) {
    if (lock_request->granted_) {
      if (lock_request->txn_id_ == txn->GetTransactionId()) {
        continue;
      }
      if (!CheckCompability(lock_request->lock_mode_, lock_mode)) {
        lock.unlock();
        return false;
      }
    }
  }

  // upgrade lock
  if (row_lock_map_[rid]->upgrading_ != INVALID_TXN_ID) {
    if (row_lock_map_[rid]->upgrading_ == txn->GetTransactionId()) {
      for (auto &lock_request : row_lock_map_[rid]->request_queue_) {
        if (lock_request->txn_id_ == txn->GetTransactionId()) {
          lock_request->lock_mode_ = lock_mode;
          lock_request->granted_ = true;
          row_lock_map_[rid]->upgrading_ = INVALID_TXN_ID;
          InsertIntoTransactionRowLockSet(txn, lock_mode, oid, rid);
          row_lock_map_[rid]->cv_.notify_all();
          lock.unlock();
          return true;
        }
      }
    } else {
      lock.unlock();
      return false;
    }
  }

  // normal lock
  std::vector<std::shared_ptr<LockRequest>> queue;
  for (auto &lock_request : row_lock_map_[rid]->request_queue_) {
    if (!lock_request->granted_) {
      if (lock_request->txn_id_ != txn->GetTransactionId()) {
        // if (!CheckCompability(lock_request->lock_mode_, lock_mode)) {
        //   lock.unlock();
        //   return false;
        // }
        lock.unlock();
        return false;
        // queue.emplace_back(lock_request);
      } else {
        // queue.emplace_back(lock_request);
        // for (auto &lock_request : queue) {
        //   // lock_request->lock_mode_ = lock_mode;
        //   lock_request->granted_ = true;
        //   InsertIntoTransactionRowLockSet(txn, lock_request->lock_mode_, oid, rid);
        //   row_lock_map_[rid]->cv_.notify_all();
        // }
        lock_request->granted_ = true;
        InsertIntoTransactionRowLockSet(txn, lock_request->lock_mode_, oid, rid);
        row_lock_map_[rid]->cv_.notify_all();
        lock.unlock();
        return true;
      }
    } else {
      if (lock_request->txn_id_ == txn->GetTransactionId()) {
        InsertIntoTransactionRowLockSet(txn, lock_request->lock_mode_, oid, rid);
      }
    }
  }

  // BUSTUB_ASSERT(false, "grantLock should not reach here");
  row_lock_map_[rid]->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // if (txn->GetState() == TransactionState::ABORTED) {
  //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  //   return false;
  // }

  std::unique_lock<std::mutex> lock(row_lock_map_latch_);
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    // no lock on the table
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }

  auto lock_request_queue = row_lock_map_[rid];
  lock.unlock();

  std::unique_lock<std::mutex> lock2(lock_request_queue->latch_);
  auto iter = lock_request_queue->request_queue_.begin();
  for (; iter != lock_request_queue->request_queue_.end(); ++iter) {
    if (iter->get()->oid_ == oid && iter->get()->rid_ == rid) {
      break;
    }
  }
  if (iter == lock_request_queue->request_queue_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  auto lock_mode = iter->get()->lock_mode_;

  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    if ((iter->get()->lock_mode_ == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) ||
        iter->get()->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  lock_request_queue->request_queue_.erase(iter);
  lock2.unlock();

  if (lock_mode == LockMode::EXCLUSIVE) {
    auto it = txn->GetExclusiveRowLockSet()->find(oid);
    if (it != txn->GetExclusiveRowLockSet()->end()) {
      it->second.erase(rid);
    }
  } else {
    auto it = txn->GetSharedRowLockSet()->find(oid);
    if (it != txn->GetSharedRowLockSet()->end()) {
      it->second.erase(rid);
    }
  }

  lock_request_queue->cv_.notify_all();
  LOG_INFO("txn:%d unlock row", txn->GetTransactionId());
  return true;
}

auto LockManager::CheckCompability(LockMode lock_mode1, LockMode lock_mode2) -> bool {
  if (lock_mode1 == LockMode::EXCLUSIVE) {
    return false;
  }
  if (lock_mode1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (lock_mode2 != LockMode::INTENTION_SHARED) {
      return false;
    }
  }
  if (lock_mode1 == LockMode::INTENTION_EXCLUSIVE) {
    if (lock_mode2 == LockMode::EXCLUSIVE || lock_mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE ||
        lock_mode2 == LockMode::SHARED) {
      return false;
    }
  }
  if (lock_mode1 == LockMode::INTENTION_SHARED) {
    if (lock_mode2 == LockMode::EXCLUSIVE) {
      return false;
    }
  }
  if (lock_mode1 == LockMode::SHARED) {
    if (lock_mode2 == LockMode::EXCLUSIVE || lock_mode2 == LockMode::SHARED_INTENTION_EXCLUSIVE ||
        lock_mode2 == LockMode::INTENTION_EXCLUSIVE) {
      return false;
    }
  }
  return true;
}

auto LockManager::GrantLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  std::unique_lock<std::mutex> lock(table_lock_map_latch_);
  // compatibility check
  for (auto &lock_request : table_lock_map_[oid]->request_queue_) {
    if (lock_request->granted_) {
      if (lock_request->txn_id_ == txn->GetTransactionId()) {
        continue;
      }
      if (!CheckCompability(lock_request->lock_mode_, lock_mode)) {
        lock.unlock();
        return false;
      }
    }
  }
  // pass compatibility check

  // priority check
  // upgrade lock
  if (table_lock_map_[oid]->upgrading_ != INVALID_TXN_ID) {
    if (table_lock_map_[oid]->upgrading_ == txn->GetTransactionId()) {
      for (auto &lock_request : table_lock_map_[oid]->request_queue_) {
        if (lock_request->txn_id_ == txn->GetTransactionId()) {
          lock_request->lock_mode_ = lock_mode;
          lock_request->granted_ = true;
          table_lock_map_[oid]->upgrading_ = INVALID_TXN_ID;
          InsertIntoTransactionTableLockSet(txn, lock_mode, oid);
          table_lock_map_[oid]->cv_.notify_all();
          lock.unlock();
          return true;
        }
      }
    } else {
      lock.unlock();
      return false;
    }
  }

  // normal lock
  std::vector<std::shared_ptr<LockRequest>> queue;
  for (auto &lock_request : table_lock_map_[oid]->request_queue_) {
    if (!lock_request->granted_) {
      if (lock_request->txn_id_ != txn->GetTransactionId()) {
        // if (!CheckCompability(lock_request->lock_mode_, lock_mode)) {
        //   lock.unlock();
        //   return false;
        // }
        lock.unlock();
        return false;
        // queue.emplace_back(lock_request);
      } else {
        // queue.emplace_back(lock_request);
        // for (auto &lock_request : queue) {
        //   // lock_request->lock_mode_ = lock_mode;
        //   lock_request->granted_ = true;
        //   if (lock_request->txn_id_ == txn->GetTransactionId()) {
        //     InsertIntoTransactionTableLockSet(txn, lock_request->lock_mode_, oid);
        //   }
        //   table_lock_map_[oid]->cv_.notify_all();
        // }
        // lock_request->lock_mode_ = lock_mode;
        lock_request->granted_ = true;
        InsertIntoTransactionTableLockSet(txn, lock_request->lock_mode_, oid);
        lock.unlock();
        table_lock_map_[oid]->cv_.notify_all();
        return true;
      }
    }
    // } else {
    //   if (lock_request->txn_id_ == txn->GetTransactionId()) {
    //     InsertIntoTransactionTableLockSet(txn, lock_request->lock_mode_, oid);
    //   }
    // }
  }
  // BUSTUB_ASSERT(false, "txn:%d grantLock should not reach here", txn->GetTransactionId());
  table_lock_map_[oid]->cv_.notify_all();
  return true;
}

auto LockManager::InsertIntoTransactionTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid)
    -> void {
  txn->LockTxn();
  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  }
  txn->UnlockTxn();
}

auto LockManager::InsertIntoTransactionRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                                  const RID &rid) -> void {
  txn->LockTxn();
  if (lock_mode == LockMode::EXCLUSIVE) {
    auto it = txn->GetExclusiveRowLockSet()->find(oid);
    if (it == txn->GetExclusiveRowLockSet()->end()) {
      std::unordered_set<RID> rids;
      rids.insert(rid);
      txn->GetExclusiveRowLockSet()->insert(std::make_pair(oid, rids));
    } else {
      it->second.insert(rid);
    }
  } else if (lock_mode == LockMode::SHARED) {
    auto it = txn->GetSharedRowLockSet()->find(oid);
    if (it == txn->GetSharedRowLockSet()->end()) {
      std::unordered_set<RID> rids;
      rids.insert(rid);
      txn->GetSharedRowLockSet()->insert(std::make_pair(oid, rids));
    } else {
      it->second.insert(rid);
    }
  }
  txn->UnlockTxn();
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
