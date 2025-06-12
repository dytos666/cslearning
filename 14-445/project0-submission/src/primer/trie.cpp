//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// trie.cpp
//
// Identification: src/primer/trie.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/trie.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

/**
 * @brief Get the value associated with the given key.
 * 1. If the key is not in the trie, return nullptr.
 * 2. If the key is in the trie but the type is mismatched, return nullptr.
 * 3. Otherwise, return the value.
 */
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto node = root_;
  for (auto c : key) {
    if (node == nullptr || node->children_.find(c) == node->children_.end()) {
      return nullptr;
    }
    node = node->children_.at(c);
  }
  const auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());

  if (value_node != nullptr) {
    return value_node->value_.get();
  }
  return nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

/**
 * @brief Put a new key-value pair into the trie. If the key already exists, overwrite the value.
 * @return the new trie.
 */
template <class T>
auto PutHelper(const std::shared_ptr<const TrieNode> &root, std::string_view key, T value)
    -> std::shared_ptr<const TrieNode> {
  auto mutable_root = root->Clone();

  if (key.empty()) {
    auto value_ptr = std::make_shared<T>(std::move(value));
    return std::make_shared<const TrieNodeWithValue<T>>(mutable_root->children_, std::move(value_ptr));
  }

  char c = key.front();
  auto child_iter = mutable_root->children_.find(c);
  if (child_iter != mutable_root->children_.end()) {
    auto child = PutHelper<T>(child_iter->second, key.substr(1), std::move(value));
    mutable_root->children_[c] = child;
    return mutable_root;
  }

  std::shared_ptr<const TrieNode> new_node =
      std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  for (auto it = key.rbegin(); it != key.rend(); ++it) {
    std::map<char, std::shared_ptr<const TrieNode>> new_children;
    new_children[*it] = new_node;
    new_node = std::make_shared<const TrieNode>(std::move(new_children));
  }

  mutable_root->children_[c] = new_node->children_.at(c);
  return mutable_root;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  if (key.empty()) {
    auto value_ptr = std::make_shared<T>(std::move(value));
    if (root_->children_.empty()) {
      auto ans = std::make_unique<TrieNodeWithValue<T>>(std::move(value_ptr));
      return Trie(std::move(ans));
    }
    auto ans = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(value_ptr));
    return Trie(std::move(ans));
  }

  std::shared_ptr<TrieNode> ans = nullptr;
  if (root_ == nullptr) {
    ans = std::make_shared<TrieNode>();
  } else {
    ans = root_->Clone();
  }
  auto new_root = PutHelper<T>(ans, key, std::move(value));

  return Trie(std::move(new_root));
}

/**
 * @brief Remove the key from the trie.
 * @return If the key does not exist, return the original trie. Otherwise, returns the new trie.
 */
auto RemoveHelper(std::shared_ptr<TrieNode> &root, std::string_view key) -> void {
  char c = key.front();
  auto it = root->children_.find(c);
  if (it != root->children_.end()) {
    if (key.size() == 1) {
      if (it->second->children_.empty()) {
        root->children_.erase(c);
        if (!root->is_value_node_ && root->children_.empty()) {
          root = nullptr;
        }
      } else {
        it->second = std::make_shared<TrieNode>(it->second->children_);
      }
    } else {
      std::shared_ptr<TrieNode> new_root = root->children_[c]->Clone();
      RemoveHelper(new_root, key.substr(1));
      root->children_[c] = new_root;
      if (new_root == nullptr) {
        root->children_.erase(c);
      }
      if (!root->is_value_node_ && root->children_.empty()) {
        root = nullptr;
      }
    }
  }
}
auto Trie::Remove(std::string_view key) const -> Trie {
  if (key.empty()) {
    return {};
  }

  std::shared_ptr<TrieNode> new_root = nullptr;
  if (root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = root_->Clone();
  }
  RemoveHelper(new_root, key);
  return Trie(std::move(new_root));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
