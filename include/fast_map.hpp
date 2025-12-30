#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <glog/logging.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

// A simple 64-bit FNV-1a + finalizer: fast for short strings
static inline uint64_t fnv1a64(std::string_view s, uint64_t seed) {
  uint64_t h = 1469598103934665603ULL ^ seed;
  for (char c : s)
    h = (h ^ static_cast<unsigned char>(c)) * 1099511628211ULL;
  // Mix to better spread low-entropy cases
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ULL;
  h ^= h >> 33;
  return h;
}

template <typename T> struct alignas(64) Slot {
  // Store immutable key for verification; value is mutable.
  std::string key;
  uint32_t len = 0;
  uint64_t fp = 0; // full 64-bit fingerprint
  std::atomic<T> value{};

  bool occupied() const noexcept { return len != 0; }
};

// Non-atomic version for thread-local use
template <typename T> struct alignas(64) LocalSlot {
  // Store immutable key for verification; value is mutable.
  std::string key;
  uint32_t len = 0;
  uint64_t fp = 0; // full 64-bit fingerprint
  T value{};

  bool occupied() const noexcept { return len != 0; }
};

template <typename T> class FastMap {
public:
  // Build from a small set of unique keys. Throws on duplicates. Allows empty
  // sets.
  explicit FastMap(const std::vector<std::string> &keys) {
    if (keys.empty()) {
      // Create empty map - all operations will LOG(FATAL) if attempted
      seed_ = 0;
      size_ = 0;
      mask_ = size_t(-1);
      return;
    }
    const size_t K = keys.size();
    // Verify uniqueness
    {
      std::vector<std::string> copy = keys;
      std::sort(copy.begin(), copy.end());
      if (std::unique(copy.begin(), copy.end()) != copy.end())
        throw std::invalid_argument("duplicate key");
    }
    // Try to find a seed that makes hash % K unique (minimal perfect)
    seed_ = 0;
    bool perfect = false;
    for (uint64_t trial = 0; trial < 100000; ++trial) {
      if (try_build(keys, K, /*cap=*/K, /*seed=*/trial)) {
        seed_ = trial;
        perfect = true;
        break;
      }
    }
    if (!perfect) {
      // Fall back to small power-of-two capacity >= 2*K
      size_t cap = 1;
      while (cap < 2 * K)
        cap <<= 1; // 2,4,8,...
      for (uint64_t trial = 0; trial < 100000; ++trial) {
        if (try_build(keys, K, cap, trial)) {
          seed_ = trial;
          break;
        }
      }
    }
  }

  ~FastMap() = default;

  // delete copy semantics
  FastMap(const FastMap &) = delete;
  FastMap &operator=(const FastMap &) = delete;

  // delete move semantics
  FastMap(FastMap &&) = delete;
  FastMap &operator=(FastMap &&) = delete;

  // Returns pointer to the atomic value for key 'k', or nullptr if absent.
  std::atomic<T> *find_ptr(std::string_view k) noexcept {
    size_t idx;
    const Slot<T> *s = probe(k, idx);
    return (s && equals(*s, k)) ? &slots_[idx].value : nullptr;
  }
  const std::atomic<T> *find_ptr(std::string_view k) const noexcept {
    size_t idx;
    const Slot<T> *s = probe(k, idx);
    return (s && equals(*s, k)) ? &slots_[idx].value : nullptr;
  }

  // Convenience: get value if present, else LOG(FATAL) with all keys (Acquire
  // read).
  T get(std::string_view k) const noexcept {
    if (auto p = find_ptr(k))
      return p->load(std::memory_order_acquire);

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k << "' not found. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "FastMap key '" << k << "' not found";
    return T{}; // Never reached, but satisfies compiler
  }

  // Convenience: assign (Release write). LOG(FATAL) if key is missing.
  void set(std::string_view k, T v) noexcept {
    if (auto p = find_ptr(k)) {
      p->store(v, std::memory_order_release);
      return;
    }

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k
              << "' not found for set operation. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "FastMap key '" << k << "' not found for set operation";
  }

  // Convenience: add delta (Relaxed). LOG(FATAL) if key missing.
  void add(std::string_view k, T delta) noexcept {
    if (auto p = find_ptr(k)) {
      p->fetch_add(delta, std::memory_order_relaxed);
      return;
    }

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k
              << "' not found for add operation. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "FastMap key '" << k << "' not found for add operation";
  }

  // Optional: resolve once, then use index-based access in hot loops.
  // Returns npos if not found.
  size_t index_of(std::string_view k) const noexcept {
    size_t idx;
    const Slot<T> *s = probe(k, idx);
    return (s && equals(*s, k)) ? idx : npos;
  }
  static constexpr size_t npos = std::numeric_limits<size_t>::max();

  // Fast index-based operations (after index_of).
  std::atomic<T> &by_index(size_t idx) noexcept { return slots_[idx].value; }
  const std::atomic<T> &by_index(size_t idx) const noexcept {
    return slots_[idx].value;
  }
  size_t size() const noexcept { return size_; }

  // Get reference to the stored key string. LOG(FATAL) if key is missing.
  const std::string &get_key(std::string_view k) const noexcept {
    size_t idx;
    const Slot<T> *s = probe(k, idx);
    if (s && equals(*s, k))
      return slots_[idx].key;

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k
              << "' not found for get_key operation. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "FastMap key '" << k << "' not found for get_key operation";
    static const std::string empty; // Never reached, but satisfies compiler
    return empty;
  }

  /* // Iterator support for keys and indices
  class const_iterator {
  public:
      using iterator_category = std::forward_iterator_tag;
      using value_type = std::pair<std::string_view, size_t>;
      using difference_type = std::ptrdiff_t;
      using pointer = const value_type*;
      using reference = const value_type&;

      const_iterator(const std::vector<Slot<T>>* slots, size_t index)
          : slots_(slots), index_(index) {
          advance_to_next_occupied();
          update_current_pair();
      }

      reference operator*() const {
          return current_pair_;
      }

      pointer operator->() const {
          return &current_pair_;
      }

      const_iterator& operator++() {
          ++index_;
          advance_to_next_occupied();
          update_current_pair();
          return *this;
      }

      const_iterator operator++(int) {
          const_iterator tmp = *this;
          ++(*this);
          return tmp;
      }

      bool operator==(const const_iterator& other) const {
          return index_ == other.index_;
      }

      bool operator!=(const const_iterator& other) const {
          return !(*this == other);
      }

  private:
      const std::vector<Slot<T>>* slots_;
      size_t index_;
      mutable value_type current_pair_;

      void advance_to_next_occupied() {
          while (index_ < slots_->size() && !(*slots_)[index_].occupied()) {
              ++index_;
          }
      }

      void update_current_pair() {
          if (index_ < slots_->size()) {
              current_pair_ = {(*slots_)[index_].key, index_};
          }
      }
  };

  // Iterator methods
  const_iterator begin() const noexcept {
      return const_iterator(&slots_, 0);
  }

  const_iterator end() const noexcept {
      return const_iterator(&slots_, slots_.size());
  }

  const_iterator cbegin() const noexcept {
      return begin();
  }

  const_iterator cend() const noexcept {
      return end();
  } */

  // Utility methods for iteration
  template <typename Func> void for_each_key(Func &&func) const {
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        func(slots_[i].key, i);
      }
    }
  }

  template <typename Func> void for_each_occupied_index(Func &&func) const {
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        func(i);
      }
    }
  }

  // Get all keys (useful for debugging or enumeration)
  std::vector<std::string> get_all_keys() const {
    std::vector<std::string> result;
    result.reserve(size_);
    for (const auto &slot : slots_) {
      if (slot.occupied()) {
        result.push_back(slot.key);
      }
    }
    return result;
  }

  // Get all occupied indices
  std::vector<size_t> get_all_indices() const {
    std::vector<size_t> result;
    result.reserve(size_);
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        result.push_back(i);
      }
    }
    return result;
  }

private:
  std::vector<Slot<T>> slots_;
  uint64_t seed_ = 0;
  size_t size_ = 0;
  size_t mask_ = 0; // cap-1 for power-of-two; ignored for perfect (cap==K but
                    // may not be power-of-two)

  // Build with capacity 'cap' and seed. Returns true if successful (no
  // collisions).
  bool try_build(const std::vector<std::string> &keys, size_t K, size_t cap,
                 uint64_t seed) {
    std::vector<Slot<T>> tmp(cap);
    // For perfect mode cap==K we still index with h % cap; for power-of-two cap
    // we can use &mask.
    const bool pow2 = (cap & (cap - 1)) == 0;
    for (const auto &k : keys) {
      const uint64_t h = fnv1a64(k, seed);
      const size_t idx = pow2 ? (h & (cap - 1)) : (h % cap);
      if (tmp[idx].occupied())
        return false; // collision -> fail this seed
      tmp[idx].key = k;
      tmp[idx].len = static_cast<uint32_t>(k.size());
      tmp[idx].fp = h;
    }
    slots_.swap(tmp);
    mask_ = pow2 ? (cap - 1) : size_t(-1);
    size_ = K;
    return true;
  }

  // Compute index and return slot pointer (may be empty).
  inline const Slot<T> *probe(std::string_view k, size_t &idx) const noexcept {
    const uint64_t h = fnv1a64(k, seed_);
    if (mask_ != size_t(-1)) { // power-of-two capacity
      idx = h & mask_;
    } else { // perfect: cap==size_
      idx = static_cast<size_t>(h % size_);
    }
    return &slots_[idx];
  }

  static inline bool equals(const Slot<T> &s, std::string_view k) noexcept {
    // Fast checks before memcmp
    if (s.len != k.size())
      return false;
    // Optional: check a quick fingerprint to avoid memcmp on false hits.
    // Not strictly needed because MPH avoids collisions, but if we fell back
    // to a larger cap it still helps reject misses quickly.
    // (Keep it: extremely cheap and branch-friendly.)
    // Note: 's.fp' was computed with the same seed.
    // If cap==K and build succeeded, this will always match for the correct
    // key. Still, memcmp is required to be airtight. (Remove the fp check if
    // you prefer purely structural verification.)
    if (!s.occupied())
      return false;
    return std::memcmp(k.data(), s.key.data(), s.len) == 0;
  }
};

// Non-atomic version for thread-local use
template <typename T> class LocalMap {
public:
  // Build from a small set of unique keys. Throws on duplicates. Allows empty
  // sets.
  explicit LocalMap(const std::vector<std::string> &keys) {
    if (keys.empty()) {
      // Create empty map - all operations will LOG(FATAL) if attempted
      seed_ = 0;
      size_ = 0;
      mask_ = size_t(-1);
      return;
    }
    const size_t K = keys.size();
    // Verify uniqueness
    {
      std::vector<std::string> copy = keys;
      std::sort(copy.begin(), copy.end());
      if (std::unique(copy.begin(), copy.end()) != copy.end())
        throw std::invalid_argument("duplicate key");
    }
    // Try to find a seed that makes hash % K unique (minimal perfect)
    seed_ = 0;
    bool perfect = false;
    for (uint64_t trial = 0; trial < 100000; ++trial) {
      if (try_build(keys, K, /*cap=*/K, /*seed=*/trial)) {
        seed_ = trial;
        perfect = true;
        break;
      }
    }
    if (!perfect) {
      // Fall back to small power-of-two capacity >= 2*K
      size_t cap = 1;
      while (cap < 2 * K)
        cap <<= 1; // 2,4,8,...
      for (uint64_t trial = 0; trial < 100000; ++trial) {
        if (try_build(keys, K, cap, trial)) {
          seed_ = trial;
          break;
        }
      }
    }
  }

  ~LocalMap() = default;

  // delete copy semantics
  LocalMap(const LocalMap &) = delete;
  LocalMap &operator=(const LocalMap &) = delete;

  // delete move semantics
  LocalMap(LocalMap &&) = delete;
  LocalMap &operator=(LocalMap &&) = delete;

  // Returns pointer to the value for key 'k', or nullptr if absent.
  T *find_ptr(std::string_view k) noexcept {
    size_t idx;
    const LocalSlot<T> *s = probe(k, idx);
    return (s && equals(*s, k)) ? &slots_[idx].value : nullptr;
  }
  const T *find_ptr(std::string_view k) const noexcept {
    size_t idx;
    const LocalSlot<T> *s = probe(k, idx);
    return (s && equals(*s, k)) ? &slots_[idx].value : nullptr;
  }

  // Convenience: get reference to value if present, else LOG(FATAL) with all
  // keys.
  T &get(std::string_view k) noexcept {
    if (auto p = find_ptr(k))
      return *p;

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k << "' not found. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "LocalMap key '" << k << "' not found";
    std::terminate(); // Never reached, but satisfies compiler
  }

  // Convenience: get const reference to value if present, else LOG(FATAL) with
  // all keys.
  const T &get(std::string_view k) const noexcept {
    if (auto p = find_ptr(k))
      return *p;

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k << "' not found. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "LocalMap key '" << k << "' not found";
    std::terminate(); // Never reached, but satisfies compiler
  }

  // Convenience: assign. LOG(FATAL) if key is missing.
  void set(std::string_view k, T v) noexcept {
    if (auto p = find_ptr(k)) {
      *p = v;
      return;
    }

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k
              << "' not found for set operation. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "LocalMap key '" << k << "' not found for set operation";
  }

  // Convenience: add delta. LOG(FATAL) if key missing.
  void add(std::string_view k, T delta) noexcept {
    if (auto p = find_ptr(k)) {
      *p += delta;
      return;
    }

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k
              << "' not found for add operation. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "LocalMap key '" << k << "' not found for add operation";
  }

  // Optional: resolve once, then use index-based access in hot loops.
  // Returns npos if not found.
  size_t index_of(std::string_view k) const noexcept {
    size_t idx;
    const LocalSlot<T> *s = probe(k, idx);
    return (s && equals(*s, k)) ? idx : npos;
  }
  static constexpr size_t npos = std::numeric_limits<size_t>::max();

  // Fast index-based operations (after index_of).
  T &by_index(size_t idx) noexcept { return slots_[idx].value; }
  const T &by_index(size_t idx) const noexcept { return slots_[idx].value; }
  size_t size() const noexcept { return size_; }

  // Get reference to the stored key string. LOG(FATAL) if key is missing.
  const std::string &get_key(std::string_view k) const noexcept {
    size_t idx;
    const LocalSlot<T> *s = probe(k, idx);
    if (s && equals(*s, k))
      return slots_[idx].key;

    // Key not found - log all present keys and then fatal error
    LOG(INFO) << "Key '" << k
              << "' not found for get_key operation. Available keys:";
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        LOG(INFO) << "  - '" << slots_[i].key << "' (index: " << i << ")";
      }
    }
    LOG(FATAL) << "LocalMap key '" << k << "' not found for get_key operation";
    static const std::string empty; // Never reached, but satisfies compiler
    return empty;
  }

  // Simple iterator: allows iterating over occupied slots.
  // Meets minimum requirements: operator==, operator!=, operator++,
  // dereference.
  struct iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = LocalSlot<T>;
    using pointer = LocalSlot<T> *;
    using reference = LocalSlot<T> &;

    std::vector<LocalSlot<T>> *slots_ = nullptr;
    size_t idx_ = 0;

    iterator(std::vector<LocalSlot<T>> *slots, size_t idx)
        : slots_(slots), idx_(idx) {
      skip_empty();
    }

    void skip_empty() {
      if (!slots_ || slots_->empty())
        return;
      // If initially at end (only for end() constructor), don't wrap
      // immediately? Wait, end() is usually idx = size. If we want circular,
      // maybe begin() == end() is only true if invalid? Standard iterators:
      // begin() can equal end() if empty. But for wrapping iterator, we
      // shouldn't really use end() for termination of loop.

      // Ensure idx_ is within bounds or wrapped
      if (idx_ >= slots_->size())
        idx_ = 0;

      size_t start = idx_;
      // Scan for occupied
      while (idx_ < slots_->size() && !(*slots_)[idx_].occupied()) {
        idx_++;
        if (idx_ >= slots_->size())
          idx_ = 0; // Wrap
        if (idx_ == start)
          break; // We scanned everything and found nothing
      }
    }

    bool operator==(const iterator &other) const {
      return idx_ == other.idx_ && slots_ == other.slots_;
    }

    bool operator!=(const iterator &other) const { return !(*this == other); }

    iterator &operator++() { // prefix
      if (!slots_ || slots_->empty())
        return *this;

      // Move to next
      idx_++;
      // skip_empty handles wrapping
      skip_empty();

      return *this;
    }

    // Postfix ++
    iterator operator++(int) {
      iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    reference operator*() const { return (*slots_)[idx_]; }
    pointer operator->() const { return &(*slots_)[idx_]; }
  };

  iterator begin() { return iterator(&slots_, 0); }
  iterator end() { return iterator(&slots_, slots_.size()); }

  // Const iterator support could be added similarly if needed, but for now
  // keeping it simple as requested. Non-const iterator on const object
  // won't work, so we might need const_iterator if the map is const.
  // But given "very simple", I'll stick to non-const for now unless user asked.
  // Wait, usually one wants to iterate over a map they just built (which might
  // be const). The user asked for "LocalMap::iterator". I will just add
  // standard begin/end.

  // Utility methods for iteration
  template <typename Func> void for_each_key(Func &&func) const {
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        func(slots_[i].key, i);
      }
    }
  }

  template <typename Func> void for_each_occupied_index(Func &&func) const {
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        func(i);
      }
    }
  }

  // Get all keys (useful for debugging or enumeration)
  std::vector<std::string> get_all_keys() const {
    std::vector<std::string> result;
    result.reserve(size_);
    for (const auto &slot : slots_) {
      if (slot.occupied()) {
        result.push_back(slot.key);
      }
    }
    return result;
  }

  // Get all occupied indices
  std::vector<size_t> get_all_indices() const {
    std::vector<size_t> result;
    result.reserve(size_);
    for (size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].occupied()) {
        result.push_back(i);
      }
    }
    return result;
  }

private:
  std::vector<LocalSlot<T>> slots_;
  uint64_t seed_ = 0;
  size_t size_ = 0;
  size_t mask_ = 0; // cap-1 for power-of-two; ignored for perfect (cap==K but
                    // may not be power-of-two)

  // Build with capacity 'cap' and seed. Returns true if successful (no
  // collisions).
  bool try_build(const std::vector<std::string> &keys, size_t K, size_t cap,
                 uint64_t seed) {
    std::vector<LocalSlot<T>> tmp(cap);
    // For perfect mode cap==K we still index with h % cap; for power-of-two cap
    // we can use &mask.
    const bool pow2 = (cap & (cap - 1)) == 0;
    for (const auto &k : keys) {
      const uint64_t h = fnv1a64(k, seed);
      const size_t idx = pow2 ? (h & (cap - 1)) : (h % cap);
      if (tmp[idx].occupied())
        return false; // collision -> fail this seed
      tmp[idx].key = k;
      tmp[idx].len = static_cast<uint32_t>(k.size());
      tmp[idx].fp = h;
    }
    slots_.swap(tmp);
    mask_ = pow2 ? (cap - 1) : size_t(-1);
    size_ = K;
    return true;
  }

  // Compute index and return slot pointer (may be empty).
  inline const LocalSlot<T> *probe(std::string_view k,
                                   size_t &idx) const noexcept {
    const uint64_t h = fnv1a64(k, seed_);
    if (mask_ != size_t(-1)) { // power-of-two capacity
      idx = h & mask_;
    } else { // perfect: cap==size_
      idx = static_cast<size_t>(h % size_);
    }
    return &slots_[idx];
  }

  static inline bool equals(const LocalSlot<T> &s,
                            std::string_view k) noexcept {
    // Fast checks before memcmp
    if (s.len != k.size())
      return false;
    // Optional: check a quick fingerprint to avoid memcmp on false hits.
    // Not strictly needed because MPH avoids collisions, but if we fell back
    // to a larger cap it still helps reject misses quickly.
    // (Keep it: extremely cheap and branch-friendly.)
    // Note: 's.fp' was computed with the same seed.
    // If cap==K and build succeeded, this will always match for the correct
    // key. Still, memcmp is required to be airtight. (Remove the fp check if
    // you prefer purely structural verification.)
    if (!s.occupied())
      return false;
    return std::memcmp(k.data(), s.key.data(), s.len) == 0;
  }
};

// Convenience type aliases for common use cases
using FastMapInt64 = FastMap<int64_t>;
using FastMapInt32 = FastMap<int32_t>;
using FastMapUInt64 = FastMap<uint64_t>;
using FastMapUInt32 = FastMap<uint32_t>;
using FastMapDouble = FastMap<double>;
using FastMapFloat = FastMap<float>;

// Local (non-atomic) versions
using LocalMapInt64 = LocalMap<int64_t>;
using LocalMapInt32 = LocalMap<int32_t>;
using LocalMapUInt64 = LocalMap<uint64_t>;
using LocalMapUInt32 = LocalMap<uint32_t>;
using LocalMapDouble = LocalMap<double>;
using LocalMapFloat = LocalMap<float>;