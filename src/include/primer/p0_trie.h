// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // p0_trie.h
// //
// // Identification: src/include/primer/p0_trie.h
// //
// // Copyright (c) 2015-2022, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #pragma once

// #include <memory>
// #include <stack>
// #include <stdexcept>
// #include <string>
// #include <unordered_map>
// #include <utility>
// #include <vector>

// #include "common/exception.h"
// #include "common/rwlatch.h"

// namespace bustub {

// /**
//  * TrieNode is a generic container for any node in Trie.
//  */
// class TrieNode {
//  public:
//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Construct a new Trie Node object with the given key char.
//    * is_end_ flag should be initialized to false in this constructor.
//    *
//    * @param key_char Key character of this trie node
//    */
//   explicit TrieNode(char key_char) {
//     key_char_ = key_char;
//     is_end_ = false;
//   }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Move constructor for trie node object. The unique pointers stored
//    * in children_ should be moved from other_trie_node to new trie node.
//    *
//    * @param other_trie_node Old trie node.
//    */
//   TrieNode(TrieNode &&other_trie_node) noexcept {
//     is_end_ = other_trie_node.is_end_;
//     key_char_ = other_trie_node.key_char_;
//     children_ = std::move(other_trie_node.children_);
//   }

//   /**
//    * @brief Destroy the TrieNode object.
//    */
//   virtual ~TrieNode() = default;

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Whether this trie node has a child node with specified key char.
//    *
//    * @param key_char Key char of child node.
//    * @return True if this trie node has a child with given key, false otherwise.
//    */
//   bool HasChild(char key_char) const {
//     if (children_.empty()) {
//       return false;
//     }
//     return children_.find(key_char) != children_.end();
//   }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Whether this trie node has any children at all. This is useful
//    * when implementing 'Remove' functionality.
//    *
//    * @return True if this trie node has any child node, false if it has no child node.
//    */
//   bool HasChildren() const { return !children_.empty(); }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Whether this trie node is the ending character of a key string.
//    *
//    * @return True if is_end_ flag is true, false if is_end_ is false.
//    */
//   bool IsEndNode() const { return is_end_; }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Return key char of this trie node.
//    *
//    * @return key_char_ of this trie node.
//    */
//   char GetKeyChar() const { return key_char_; }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Insert a child node for this trie node into children_ map, given the key char and
//    * unique_ptr of the child node. If specified key_char already exists in children_,
//    * return nullptr. If parameter `child`'s key char is different than parameter
//    * `key_char`, return nullptr.
//    *
//    * Note that parameter `child` is rvalue and should be moved when it is
//    * inserted into children_map.
//    *
//    * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
//    * underlying data without taking ownership of the unique_ptr. Further, we can set the return
//    * value to nullptr when error occurs.
//    *
//    * @param key Key of child node
//    * @param child Unique pointer created for the child node. This should be added to children_ map.
//    * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
//    */
//   std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
//     if (key_char != child->GetKeyChar()) {
//       return nullptr;
//     }
//     if (HasChild(key_char)) {
//       return nullptr;
//     }

//     children_[key_char] = std::move(child);
//     return &children_[key_char];
//   }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Get the child node given its key char. If child node for given key char does
//    * not exist, return nullptr.
//    *
//    * @param key Key of child node
//    * @return Pointer to unique_ptr of the child node, nullptr if child
//    *         node does not exist.
//    */
//   std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
//     if (HasChild(key_char)) {
//       return &children_[key_char];
//     }
//     return nullptr;
//   }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Remove child node from children_ map.
//    * If key_char does not exist in children_, return immediately.
//    *
//    * @param key_char Key char of child node to be removed
//    */
//   void RemoveChildNode(char key_char) {
//     if (HasChild(key_char)) {
//       children_.erase(key_char);
//     }
//   }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Set the is_end_ flag to true or false.
//    *
//    * @param is_end Whether this trie node is ending char of a key string
//    */
//   void SetEndNode(bool is_end) { is_end_ = is_end; }

//  protected:
//   /** Key character of this trie node */
//   char key_char_;
//   /** whether this node marks the end of a key */
//   bool is_end_{false};
//   /** A map of all child nodes of this trie node, which can be accessed by each
//    * child node's key char. */
//   std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
// };

// /**
//  * TrieNodeWithValue is a node that mark the ending of a key, and it can
//  * hold a value of any type T.
//  */
// template <typename T>
// class TrieNodeWithValue : public TrieNode {
//  private:
//   /* Value held by this trie node. */
//   T value_;

//  public:
//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
//    * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
//    *
//    * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
//    * Since it contains unique pointers, the first parameter is a rvalue reference.
//    *
//    * You should:
//    * 1) invoke TrieNode's move constructor to move data from TrieNode to
//    * TrieNodeWithValue.
//    * 2) set value_ member variable of this node to parameter `value`.
//    * 3) set is_end_ to true
//    *
//    * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
//    * @param value
//    */
//   TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)), value_(value) { is_end_ = true; }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
//    *
//    * You should:
//    * 1) Invoke the constructor for TrieNode with given key_char.
//    * 2) Set value_ for this node.
//    * 3) set is_end_ to true.
//    *
//    * @param key_char Key char of this node
//    * @param value Value of this node
//    */
//   TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value) {}

//   /**
//    * @brief Destroy the Trie Node With Value object
//    */
//   ~TrieNodeWithValue() override = default;

//   /**
//    * @brief Get the stored value_.
//    *
//    * @return Value of type T stored in this node
//    */
//   T GetValue() const { return value_; }
// };

// /**
//  * Trie is a concurrent key-value store. Each key is string and its corresponding
//  * value can be any type.
//  */
// class Trie {
//  private:
//   /* Root node of the trie */
//   std::unique_ptr<TrieNode> root_;
//   /* Read-write lock for the trie */
//   ReaderWriterLatch latch_;

//  public:
//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Construct a new Trie object. Initialize the root node with '\0'
//    * character.
//    */
//   Trie() { root_ = std::make_unique<TrieNode>('\0'); }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Insert key-value pair into the trie.
//    *
//    * If key is empty string, return false immediately.
//    *
//    * If key alreadys exists, return false. Duplicated keys are not allowed and
//    * you should never overwrite value of an existing key.
//    *
//    * When you reach the ending character of a key:
//    * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
//    * and add it to parent node's children_ map.
//    * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
//    * invoking the appropriate constructor.
//    * 3. If it is already a TrieNodeWithValue,
//    * then insertion fails and return false. Do not overwrite existing data with new data.
//    *
//    * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
//    * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
//    * is_end_ == true, it points to TrieNodeWithValue.
//    *
//    * @param key Key used to traverse the trie and find correct node
//    * @param value Value to be inserted
//    * @return True if insertion succeeds, false if key already exists
//    */
//   template <typename T>
//   bool Insert(const std::string &key, T value) {
//     latch_.WLock();
//     if (key.empty()) {
//       latch_.WUnlock();
//       return false;
//     }
//     std::unique_ptr<TrieNode> *tmp = &root_;
//     std::unique_ptr<TrieNode> *p;
//     for (auto &key_char : key) {
//       if (!tmp->get()->HasChild(key_char)) {
//         tmp->get()->InsertChildNode(key_char, std::make_unique<TrieNode>(key_char));
//       }
//       p = tmp;
//       tmp = tmp->get()->GetChildNode(key_char);
//     }

//     if (tmp->get()->IsEndNode()) {
//       latch_.WUnlock();
//       return false;
//     }

//     // auto tmp_node = std::move(*tmp);
//     // p->get()->RemoveChildNode(tmp_node->GetKeyChar());
//     // p->get()->InsertChildNode(tmp_node->GetKeyChar(),
//     //                           std::make_unique<TrieNodeWithValue<T>>(std::move(*tmp_node), value));
//     auto temp_node = std::move(*tmp);
//     p->get()->RemoveChildNode(key.back());
//     auto new_node = std::make_unique<TrieNodeWithValue<T>>(std::move(*temp_node), value);
//     p->get()->InsertChildNode(key.back(), std::move(new_node));

//     latch_.WUnlock();
//     return true;
//   }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Remove key value pair from the trie.
//    * This function should also remove nodes that are no longer part of another
//    * key. If key is empty or not found, return false.
//    *
//    * You should:
//    * 1) Find the terminal node for the given key.
//    * 2) If this terminal node does not have any children, remove it from its
//    * parent's children_ map.
//    * 3) Recursively remove nodes that have no children and is not terminal node
//    * of another key.
//    *
//    * @param key Key used to traverse the trie and find correct node
//    * @return True if key exists and is removed, false otherwise
//    */
//   bool Remove(const std::string &key) {
//     latch_.WLock();
//     if (key.empty()) {
//       latch_.WUnlock();
//       return false;
//     }
//     auto node_stack = std::stack<std::unique_ptr<TrieNode> *>();

//     auto tmp = &root_;
//     for (auto key_char : key) {
//       if (!tmp->get()->HasChild(key_char)) {
//         latch_.WUnlock();
//         return false;
//       }

//       node_stack.push(tmp);
//       tmp = tmp->get()->GetChildNode(key_char);
//     }

//     // Didn't find key
//     if (!tmp->get()->IsEndNode()) {
//       latch_.WUnlock();
//       return false;
//     }

//     // key is part of other keys
//     if (tmp->get()->HasChildren()) {
//       tmp->get()->SetEndNode(false);
//       latch_.WUnlock();
//       return true;
//     }

//     // remove node
//     // while (!node_stack.empty()) {
//     //   node_stack.top()->get()->RemoveChildNode(tmp->get()->GetKeyChar());
//     //   if (node_stack.top()->get()->HasChildren() || node_stack.top()->get()->IsEndNode()) {
//     //     latch_.WUnlock();
//     //     return true;
//     //   }
//     //   tmp = node_stack.top();
//     //   node_stack.pop();
//     // }
//     node_stack.top()->get()->RemoveChildNode(key.back());
//     while (node_stack.size() > 1) {
//       auto top_node = node_stack.top();
//       char key_char = top_node->get()->GetKeyChar();
//       if (top_node->get()->HasChildren() || top_node->get()->IsEndNode()) {
//         break;
//       }
//       node_stack.pop();
//       node_stack.top()->get()->RemoveChildNode(key_char);
//     }

//     latch_.WUnlock();
//     return true;
//   }

//   /**
//    * TODO(P0): Add implementation
//    *
//    * @brief Get the corresponding value of type T given its key.
//    * If key is empty, set success to false.
//    * If key does not exist in trie, set success to false.
//    * If given type T is not the same as the value type stored in TrieNodeWithValue
//    * (ie. GetValue<int> is called but terminal node holds std::string),
//    * set success to false.
//    *
//    * To check whether the two types are the same, dynamic_cast
//    * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
//    * is not nullptr, then type T is the correct type.
//    *
//    * @param key Key used to traverse the trie and find correct node
//    * @param success Whether GetValue is successful or not
//    * @return Value of type T if type matches
//    */
//   template <typename T>
//   T GetValue(const std::string &key, bool *success) {
//     latch_.RLock();
//     if (key.empty()) {
//       latch_.RUnlock();
//       *success = false;
//       return T();
//     }
//     auto tmp = &root_;
//     for (auto &key_char : key) {
//       if (!tmp->get()->HasChild(key_char)) {
//         *success = false;
//         latch_.RUnlock();
//         return T();
//       }
//       tmp = tmp->get()->GetChildNode(key_char);
//     }

//     if (tmp->get()->IsEndNode()) {
//       *success = true;
//       latch_.RUnlock();
//       return dynamic_cast<TrieNodeWithValue<T> *>(tmp->get())->GetValue();
//     }
//     *success = false;
//     latch_.RUnlock();
//     return T();
//   }
// };
// }  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) { is_end_ = false; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    key_char_ = other_trie_node.key_char_;
    is_end_ = other_trie_node.is_end_;
    children_ = std::move(other_trie_node.children_);
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return children_.find(key_char) != children_.end(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    // 如果已经有这个key_char 或者 传入的key_char与child的key_char不等
    if (children_.find(key_char) != children_.end() || key_char != child->key_char_) {
      return nullptr;
    }
    children_[key_char] = std::move(child);
    return &children_[key_char];
  }

  /** q
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    if (children_.find(key_char) != children_.end()) {
      return &children_[key_char];
    }
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (children_.find(key_char) != children_.end()) {
      children_.erase(key_char);
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::forward<TrieNode>(trieNode)), value_(value) {
    is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value) { is_end_ = true; }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

class RLock {
  ReaderWriterLatch *latch_;

 public:
  explicit RLock(ReaderWriterLatch *latch) : latch_(latch) { latch_->RLock(); }

  ~RLock() { latch_->RUnlock(); }
};

class WLock {
  ReaderWriterLatch *latch_;

 public:
  explicit WLock(ReaderWriterLatch *latch) : latch_(latch) { latch_->WLock(); }
  ~WLock() { latch_->WUnlock(); }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() { root_ = std::make_unique<TrieNode>('\0'); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    WLock w(&latch_);
    // 如果key是空的，返回false
    if (key.empty()) {
      return false;
    }
    auto p = &root_;
    std::unique_ptr<TrieNode> *parent;
    for (auto key_char : key) {
      // 如果没有这个key_char,就插入
      if (!p->get()->HasChild(key_char)) {
        p->get()->InsertChildNode(key_char, std::make_unique<TrieNode>(key_char));
      }
      parent = p;
      p = p->get()->GetChildNode(key_char);
    }
    // 如果是重复的key_char，返回false
    if (p->get()->IsEndNode()) {
      return false;
    }
    // 删除普通的节点，之后插入带value的节点
    auto temp_node = std::move(*p);
    parent->get()->RemoveChildNode(key.back());
    auto new_node = std::make_unique<TrieNodeWithValue<T>>(std::move(*temp_node), value);
    parent->get()->InsertChildNode(key.back(), std::move(new_node));
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    WLock w(&latch_);
    // 用栈记录遍历的节点
    auto node_stack = std::stack<std::unique_ptr<TrieNode> *>();
    // 如果key是空的，返回false
    if (key.empty()) {
      return false;
    }
    auto p = &root_;
    for (auto key_char : key) {
      // 如果没有这个key_char, 返回false
      if (!p->get()->HasChild(key_char)) {
        return false;
      }
      node_stack.push(p);
      p = p->get()->GetChildNode(key_char);
    }
    // 如果尾节点的is_end_标志为false，返回false
    if (!p->get()->IsEndNode()) {
      return false;
    }
    // 如果尾节点的is_end_标志为true，但下面仍然有子节点，直接修改下标志为false
    if (p->get()->HasChildren()) {
      p->get()->SetEndNode(false);
      return true;
    }
    // 找到了真正要删除的节点，从栈弹出父节点开始递归删除符合条件的节点
    node_stack.top()->get()->RemoveChildNode(key.back());
    while (node_stack.size() > 1) {
      auto top_node = node_stack.top();
      char key_char = top_node->get()->GetKeyChar();
      if (top_node->get()->HasChildren() || top_node->get()->IsEndNode()) {
        break;
      }
      node_stack.pop();
      node_stack.top()->get()->RemoveChildNode(key_char);
    }
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    RLock r(&latch_);
    // 如果key是空的，置为false，返回T()
    if (key.empty()) {
      *success = false;
      return T();
    }
    auto p = &root_;
    for (auto key_char : key) {
      // 如果没有这个key_char,置为false，返回T()
      if (!p->get()->HasChild(key_char)) {
        *success = false;
        return T();
      }
      p = p->get()->GetChildNode(key_char);
    }
    // 如果是最后一个节点, 并且类型为 TrieNodeWithValue
    if (p->get()->IsEndNode()) {
      auto temp_node = dynamic_cast<TrieNodeWithValue<T> *>((*p).get());
      if (temp_node != nullptr) {
        *success = true;
        return temp_node->GetValue();
      }
    }
    *success = false;
    return T();
  }
};
}  // namespace bustub
