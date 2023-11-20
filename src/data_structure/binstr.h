#pragma once
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cmath>
#include <concepts>
#include <iterator>
#include <string>
#include <string_view>
#include <optional>

namespace foxbatdb {
  struct BinaryStringIterator;

  class BinaryString {
    friend class BinaryStringIterator;
  private:
    std::string mData_;

  public:
    using Iterator = BinaryStringIterator;

  public:
    BinaryString() = default;
    BinaryString(std::string_view data);
    BinaryString(const BinaryString&) = default;
    BinaryString(BinaryString&&) = default;
    ~BinaryString() = default;

    BinaryString& operator=(const BinaryString&) = default;
    BinaryString& operator=(BinaryString&&) = default;

    bool operator==(const BinaryString& rhs) const;

    std::size_t Hash() const;
    std::size_t Length() const;
    bool IsEmpty() const;
    std::string ToTextString() const;
    const char* ToCString() const;
    char* ToCString();
    std::byte At(std::size_t idx) const;
    BinaryString SubStr(std::size_t start, std::size_t end) const;
    void Resize(std::size_t size);

    template <std::integral T>
    std::optional<T> ToInteger() const {
      T ret;
      auto [_, ec] = std::from_chars<T>(mData_.data(), mData_.data() + mData_.size(), ret);
      if (ec != std::errc()) {
        return std::nullopt;
      } else {
        return ret;
      }
    }

    template <std::floating_point T>
    std::optional<T> ToFloat() const {
      T ret;
      auto [_, ec] = std::from_chars<T>(
          mData_.data(), mData_.data() + mData_.size(), ret);
      if (ec != std::errc()) {
        return std::nullopt;
      } else {
        return ret;
      }
    }
    
    void Append(std::byte b);
    void Append(const BinaryString& rhs);

    void Rewrite(std::size_t pos, std::byte b);
    void Rewrite(std::size_t pos, const BinaryString& data);

    Iterator begin();
    Iterator end();
  };

  struct BinaryStringIterator {
    using iterator_concept [[maybe_unused]] = std::contiguous_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using element_type = char;
    using pointer = element_type*;
    using reference = element_type&;

    BinaryStringIterator() = default;
    BinaryStringIterator(BinaryString& s);

    reference operator*() const;
    pointer operator->() const;

    BinaryStringIterator& operator++();
    BinaryStringIterator operator++(int);
    BinaryStringIterator& operator+=(int i);

    BinaryStringIterator operator+(const difference_type other) const;
    friend BinaryStringIterator operator+(const difference_type value, const BinaryStringIterator& other);

    BinaryStringIterator& operator--();
    BinaryStringIterator operator--(int);
    BinaryStringIterator& operator-=(int i);
    difference_type operator-(const BinaryStringIterator& other) const;
    BinaryStringIterator operator-(const difference_type other) const;
    friend BinaryStringIterator operator-(const difference_type value, const BinaryStringIterator& other);

    reference operator[](difference_type idx) const;

    auto operator<=>(const BinaryStringIterator&) const = default;

   private:
    std::string::iterator pos;
  };
  }

namespace std {
template <>
class hash<foxbatdb::BinaryString> {
   public:
    std::size_t operator()(const foxbatdb::BinaryString& p) const {
      return p.Hash();
    }
};
}  // namespace std