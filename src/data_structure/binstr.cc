#include "binstr.h"

namespace foxbatdb {
  BinaryString::BinaryString(std::string_view data) {
    this->mData_.append(data.begin(), data.end());
  }

  bool BinaryString::operator==(const BinaryString& rhs) const {
    return this->mData_ == rhs.mData_;
  }

  std::size_t BinaryString::Hash() const {
    return std::hash<std::string>{}(this->ToTextString());
  }

  std::size_t BinaryString::Length() const {
    return this->mData_.size();
  }

  bool BinaryString::IsEmpty() const {
    return 0 == this->Length();
  }

  std::string BinaryString::ToTextString() const {
    return this->mData_;
  }

  const char* BinaryString::ToCString() const {
    return this->mData_.data();
  }

  std::byte BinaryString::At(std::size_t idx) const {
    return std::byte(this->mData_.at(idx));
  }

  BinaryString BinaryString::SubStr(std::size_t start, std::size_t end) const {
     if (start >= end)
       return {};
     if (end > this->Length())
       end = this->Length();
     return BinaryString {
         std::string_view{this->mData_.begin() + start, this->mData_.begin() + end}
     };
  }

  void BinaryString::Append(std::byte b) {
     this->mData_.push_back(char(b));
  }

  void BinaryString::Append(const BinaryString& rhs) {
    this->mData_.append(rhs.mData_.cbegin(), rhs.mData_.cend());
  }

  void BinaryString::Rewrite(std::size_t pos, std::byte b) {
    if (pos >= this->mData_.size()) return;
    this->mData_[pos] = char(b);
  }

  void BinaryString::Rewrite(std::size_t pos, const BinaryString& data) {
    if ((pos >= this->mData_.size()) || data.IsEmpty())
      return;
    std::size_t dataIdx = 0;
    for (; pos < this->mData_.size(); ++pos) {
      this->mData_[pos] = char(data.At(dataIdx++));
      if (data.Length() == dataIdx) {
        return;
      }
    }
    this->Append(data.SubStr(dataIdx, data.Length()));
  }

  BinaryString::Iterator BinaryString::begin() {
    return BinaryString::Iterator {*this};
  }

  BinaryString::Iterator BinaryString::end() {
    return this->begin() + this->Length();
  }

  BinaryStringIterator::BinaryStringIterator(BinaryString& s)
      : pos(s.mData_.begin()) {}

  BinaryStringIterator::reference BinaryStringIterator::operator*() const {
    return *pos;
  }

  BinaryStringIterator::pointer BinaryStringIterator::operator->() const {
    return &(*pos);
  }

  BinaryStringIterator& BinaryStringIterator::operator++() {
    pos++;
    return *this;
  }

  BinaryStringIterator BinaryStringIterator::operator++(int) {
    BinaryStringIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  BinaryStringIterator& BinaryStringIterator::operator+=(int i) {
    pos += i;
    return *this;
  }

  BinaryStringIterator BinaryStringIterator::operator+(
      const difference_type other) const {
    BinaryStringIterator ret;
    ret.pos = pos + other;
    return ret;
  }
  
  BinaryStringIterator operator+(
      const BinaryStringIterator::difference_type value,
      const BinaryStringIterator& other) {
    return other + value;
  }

  BinaryStringIterator& BinaryStringIterator::operator--() {
    pos--;
    return *this;
  }

  BinaryStringIterator BinaryStringIterator::operator--(int) {
    BinaryStringIterator tmp = *this;
    --(*this);
    return tmp;
  }

  BinaryStringIterator& BinaryStringIterator::operator-=(int i) {
    pos -= i;
    return *this;
  }

  BinaryStringIterator::difference_type BinaryStringIterator::operator-(
      const BinaryStringIterator& other) const {
    return pos - other.pos;
  }

  BinaryStringIterator BinaryStringIterator::operator-(
      const difference_type other) const {
    BinaryStringIterator ret;
    ret.pos = pos - other;
    return ret;
  }

  BinaryStringIterator operator-(
      const BinaryStringIterator::difference_type value,
      const BinaryStringIterator& other) {
    return other - value;
  }

  BinaryStringIterator::reference BinaryStringIterator::operator[](
      difference_type idx) const {
    return pos[idx];
  }
 }