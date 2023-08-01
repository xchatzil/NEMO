/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Experimental/CRC32Hash.hpp>
#include <Util/Experimental/Hash.hpp>
#include <Util/Experimental/MurMurHash3.hpp>
#include <assert.h>
#ifndef NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_HASHMAP_HPP_
#define NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_HASHMAP_HPP_

namespace NES::Experimental {

/**
 * @brief Implementation of a chained HashMap, which uses TupleBuffers as a storage backend.
 * The implementation origins from Kersten et.al. https://github.com/TimoKersten/db-engine-paradigms and Leis et.al
 * https://db.in.tum.de/~leis/papers/morsels.pdf.
 *
 * The HashMap is split in two memory areas,
 *
 * Entry Space:
 * The entry space is fixed size and contains pointers into the storage space.
 *
 * Storage Space:
 * The storage space contains individual key, value pairs.
 *
 * Furthermore, the HashMap supports different hash implementations.
 *
 */
class Hashmap {
  public:
#if defined(__SSE4_2__)
    const Hash<CRC32Hash> hasher = Hash<CRC32Hash>();
#else
    const Hash<MurMurHash3> hasher = Hash<MurMurHash3>();
#endif
    using hash_t = uint64_t;
    static const size_t headerSize = 16;// next* + hash = 16byte
    const size_t entrySize;
    const size_t keyOffset;
    const size_t valueOffset;
    const size_t entriesPerBuffer;
    class Entry
    /// Header for hashtable entries
    {
      public:
        Entry* next;
        hash_t hash;
        // payload data follows this header
        Entry(Entry* next, hash_t hash);
    };

    /**
     * @brief Constructor for a new HashMap with a specific keySize, valueSize, and a number of entries.
     * @param bufferProvider that is used to allocate new memory buffers at runtime.
     * @param keySize
     * @param valueSize
     * @param nrOfKeys number of keys the HashMap should contain.
     */
    explicit Hashmap(std::shared_ptr<Runtime::AbstractBufferProvider> bufferProvider,
                     size_t keySize,
                     size_t valueSize,
                     uint64_t nrOfKeys);

    /**
     * @brief Sets the size for the HashMap. This function clears the current HashMap and initializes it with the new size.
     * Thus we don't perform resizing and the current content is lost.
     * @param nrOfKeys number of keys the HashMap should contain.
     * @return
     */
    uint64_t setSize(uint64_t nrOfKeys);

    /**
     * @brief Calculates the
     * @param entry
     * @return
     */
    uint8_t* getKeyPtr(Entry* entry) { return ((uint8_t*) entry) + keyOffset; };

    template<class T>
    T* getKeyPtr(Entry* entry) {
        return (T*) getKeyPtr(entry);
    };

    std::unique_ptr<std::vector<Runtime::TupleBuffer>> extractEntries();
    std::unique_ptr<std::vector<Runtime::TupleBuffer>>& getEntries();

    /**
     * @brief Get the value for a specific entry
     * @param entry
     * @return
     */
    uint8_t* getValuePtr(Entry* entry) { return ((uint8_t*) entry) + valueOffset; };

    template<class T>
    T* getValuePtr(Entry* entry) {
        return (T*) getValuePtr(entry);
    };

    /**
     * @brief Returns the first entry of the chain for the given hash
     */
    inline Entry* find_chain(hash_t hash);

    /**
     * @brief Returns the first entry of the chain for the given hash
     * Uses pointer tagging as a filter to quickly determine whether hash iscontained
     */
    inline Entry* find_chain_tagged(hash_t hash);
    /// Insert entry into chain for the given hash
    inline void insert(Entry* entry, hash_t hash);
    inline Hashmap::Entry* insertEntry(hash_t hash);
    /// Insert entry into chain for the given hash
    /// Updates tag
    inline void insert_tagged(Entry* entry, hash_t hash);

    template<typename K, bool useTags>
    inline Entry* findOneEntry(K& key, hash_t h);

    template<class KeyType>
    inline auto calculateHash(KeyType& key) const;

    template<class KeyType>
    inline uint8_t* getEntry(KeyType& key);

    template<class KeyType, class ValueType>
    inline uint8_t* getEntryWithDefault(KeyType& key, ValueType&& defaultValue);

    template<typename K, bool useTags>
    Hashmap::Entry* findOrCreate(K key, hash_t hash);

    template<typename K, typename V, bool useTags>
    inline Hashmap::Entry* findOrCreateWithDefault(K key, hash_t hash, V defaultValue);

    /// Removes all elements from the hashtable
    inline void clear();

    inline static Entry* end();

    Hashmap(const Hashmap&) = delete;

    ~Hashmap();

    Runtime::TupleBuffer& getBufferForEntry(uint64_t entry) {
        auto bufferIndex = entry / entriesPerBuffer;
        return (*storageBuffers)[bufferIndex];
    }

    Entry* entryIndexToAddress(uint64_t entry) {
        auto bufferIndex = entry / entriesPerBuffer;
        auto& buffer = (*storageBuffers)[bufferIndex];
        auto entryOffsetInBuffer = entry - (bufferIndex * entriesPerBuffer);
        return reinterpret_cast<Entry*>(buffer.getBuffer() + (entryOffsetInBuffer * entrySize));
    }

    Entry* allocateNewEntry();

    void printHistogram() {
        for (uint64_t e = 0; e < capacity; e++) {
            if (entries[e] != nullptr) {
                uint64_t counter = 0;
                auto current = entries[e];
                while (current != nullptr) {
                    current = current->next;
                    counter++;
                }
                NES_DEBUG("pos: " << e << " length: " << counter);
            };
        }
    }

    Entry** entries;

    uint64_t numberOfEntries() { return currentSize; }

  private:
    using ptr_t = uint64_t;
    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    inline Hashmap::Entry* ptr(Hashmap::Entry* p);
    inline ptr_t tag(hash_t p);
    inline Hashmap::Entry* update(Hashmap::Entry* old, Hashmap::Entry* p, hash_t hash);
    Runtime::TupleBuffer entryBuffer;
    std::unique_ptr<std::vector<Runtime::TupleBuffer>> storageBuffers;
    hash_t mask;
    const ptr_t maskPointer = (~(ptr_t) 0) >> (16);
    const ptr_t maskTag = (~(ptr_t) 0) << (sizeof(ptr_t) * 8 - 16);
    size_t capacity = 0;
    uint64_t currentSize = 0;
    size_t keySize;
};

inline Hashmap::Entry* Hashmap::end() { return nullptr; }

inline Hashmap::ptr_t Hashmap::tag(Hashmap::hash_t hash) {
    auto tagPos = hash >> (sizeof(hash_t) * 8 - 4);
    return ((size_t) 1) << (tagPos + (sizeof(ptr_t) * 8 - 16));
}

inline Hashmap::Entry* Hashmap::ptr(Hashmap::Entry* p) { return (Entry*) ((ptr_t) p & maskPointer); }

inline Hashmap::Entry* Hashmap::update(Hashmap::Entry* old, Hashmap::Entry* p, Hashmap::hash_t hash) {
    return reinterpret_cast<Entry*>((size_t) p | ((size_t) old & maskTag) | tag(hash));
}

inline Hashmap::Entry* Hashmap::find_chain(hash_t hash) {
    auto pos = hash & mask;
    return entries[pos];
}

void inline Hashmap::insert(Entry* entry, hash_t hash) {
    const size_t pos = hash & mask;
    assert(pos <= mask);
    assert(pos < capacity);
    auto oldValue = entries[pos];
    entry->next = oldValue;
    entries[pos] = entry;
    this->currentSize++;
}

inline Hashmap::Entry* Hashmap::find_chain_tagged(hash_t hash) {
    //static_assert(sizeof(hash_t) == 8, "Hashtype not supported");
    auto pos = hash & mask;
    auto candidate = entries[pos];
    auto filterMatch = (size_t) candidate & tag(hash);
    if (filterMatch)
        return ptr(candidate);
    else
        return end();
}

void inline Hashmap::insert_tagged(Entry* entry, hash_t hash) {
    const size_t pos = hash & mask;
    assert(pos <= mask);
    assert(pos < capacity);
    auto oldValue = entries[pos];
    entry->next = oldValue;
    entries[pos] = update(entries[pos], entry, hash);
    this->currentSize++;
}

void inline Hashmap::clear() {
    currentSize = 0;
    for (size_t i = 0; i < capacity; i++) {
        entries[i] = nullptr;
    }
    storageBuffers = std::make_unique<std::vector<Runtime::TupleBuffer>>();
}

template<typename K, bool useTags = false>
inline Hashmap::Entry* Hashmap::findOneEntry(K& key, hash_t h) {
    Entry* entry;
    if (useTags) {
        entry = reinterpret_cast<Entry*>(find_chain_tagged(h));
    } else {
        entry = reinterpret_cast<Entry*>(find_chain(h));
    }
    if (entry == end()) {
        return nullptr;
    }
    for (; entry != end(); entry = reinterpret_cast<Entry*>(entry->next)) {
        //auto& otherKey = (*getKeyPtr<K>(entry));
        //std::cout << std::memcmp(getKeyPtr(entry), &key, keySize) << "\t <" << std::get<0>(key) << "," << std::get<1>(key) << "," << std::get<2>(key) << "> vs."
        //    << "<" << std::get<0>(otherKey) << "," << std::get<1>(otherKey) << "," << std::get<2>(otherKey) << ">" << std::endl;
        if (std::memcmp(getKeyPtr(entry), &key, keySize) == 0) {
            return entry;
        }
        //if (entry->hash == h && (*getKeyPtr<K>(entry)) == key) {
        //    return entry;
        //}
    }
    return nullptr;
}

template<typename K, typename V, bool useTags>
inline Hashmap::Entry* Hashmap::findOrCreateWithDefault(K key, hash_t hash, V defaultValue) {
    auto entry = findOneEntry<K, useTags>(key, hash);
    if (!entry) {
        auto newEntry = allocateNewEntry();
        newEntry->hash = hash;
        *getKeyPtr<K>(newEntry) = key;
        *getValuePtr<V>(newEntry) = defaultValue;
        insert(newEntry, hash);
        return newEntry;
    }
    return entry;
}

Hashmap::Entry* Hashmap::insertEntry(hash_t hash) {
    auto newEntry = allocateNewEntry();
    newEntry->hash = hash;
    insert(newEntry, hash);
    return newEntry;
}

template<typename K, bool useTags>
inline Hashmap::Entry* Hashmap::findOrCreate(K key, hash_t hash) {
    auto entry = findOneEntry<K, useTags>(key, hash);
    if (!entry) {
        auto newEntry = allocateNewEntry();
        newEntry->hash = hash;
        *getKeyPtr<K>(newEntry) = key;
        insert(newEntry, hash);
        return newEntry;
    }
    return entry;
}

template<class KeyType>
inline auto Hashmap::calculateHash(KeyType& key) const {
    return hasher(key, NES::Experimental::Hash<NES::Experimental::CRC32Hash>::SEED);
}

template<class KeyType>
inline uint8_t* Hashmap::getEntry(KeyType& key) {
    const auto h = hasher(key, NES::Experimental::Hash<NES::Experimental::CRC32Hash>::SEED);
    auto entry = findOrCreate<KeyType, false>(key, h);
    return getKeyPtr(entry);
}

template<class KeyType, class ValueType>
inline uint8_t* Hashmap::getEntryWithDefault(KeyType& key, ValueType&& defaultValue) {
    const auto h = hasher(key, NES::Experimental::Hash<NES::Experimental::CRC32Hash>::SEED);
    auto entry = findOrCreateWithDefault<KeyType, false>(key, h, defaultValue);
    return getKeyPtr(entry);
}

class HashMapFactory {
  public:
    HashMapFactory(std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager,
                   size_t keySize,
                   size_t valueSize,
                   size_t nrEntries);
    Hashmap create();
    std::unique_ptr<Hashmap> createPtr();

    size_t getKeySize();
    size_t getValueSize();

  private:
    std::shared_ptr<Runtime::AbstractBufferProvider> bufferManager;
    size_t keySize;
    size_t valueSize;
    size_t nrEntries;
};

}// namespace NES::Experimental

#endif// NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_HASHMAP_HPP_
