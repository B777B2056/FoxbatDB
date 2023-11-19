#pragma once

namespace foxbatdb {
  namespace utils {
    template <class Key, class Value, int N>
    class CTMap {
     public:
      struct KV {
        Key key;
        Value value;
      };

      constexpr const Value* operator[](Key key) const { return Get(key); }
      constexpr bool Contains(Key key) const { return Get(key) == nullptr; }

     private:
      constexpr const Value* Get(Key key, int i = 0) const {
        return i == N                ? KeyNotFound()
               : pairs[i].key == key ? &(pairs[i].value)
                                     : Get(key, i + 1);
      }

      static const Value* KeyNotFound()  // not constexpr
      {
        return nullptr;
      }

     public:
      KV pairs[N];
    };
  }
}