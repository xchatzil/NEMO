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

#ifndef NES_CORE_INCLUDE_STATE_STATEVARIABLE_HPP_
#define NES_CORE_INCLUDE_STATE_STATEVARIABLE_HPP_

#include <Util/Logger/Logger.hpp>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <State/StateId.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>

namespace NES {
namespace Runtime {
namespace detail {

template<typename T>
struct StateVariableEmplaceHelper {
    template<typename... Arguments>
    static T create(Arguments&&... args) {
        return T(std::forward<Arguments>(args)...);// abstracted away with O1+
    }
};
template<typename T>
struct StateVariableEmplaceHelper<T*> {
    template<typename... Arguments>
    static T* create(Arguments&&... args) {
        // TODO change new to something else!
        return new T(std::forward<Arguments>(args)...);
    }
};

template<typename Key, typename T>
struct StateVariableDestroyerHelper {
    static void destroy(cuckoohash_map<Key, T>& backend) {
        auto locked_table = backend.lock_table();
        locked_table.clear();
        locked_table.unlock();
    }
};
template<typename Key, typename T>
struct StateVariableDestroyerHelper<Key, T*> {
    static void destroy(cuckoohash_map<Key, T*>& backend) {
        auto locked_table = backend.lock_table();
        for (auto& it : locked_table) {
            if (it.second != nullptr) {
                delete it.second;
            }
        }
        locked_table.clear();
        locked_table.unlock();
    }
};

class Destroyable {
  public:
    virtual ~Destroyable() {}
};
}// namespace detail

template<typename Key, typename Value, std::enable_if_t<std::is_integral<Value>::value || std::is_pointer<Value>::value, int> = 0>
class StateVariable : public detail::Destroyable {
  private:
    typedef cuckoohash_map<Key, Value> StateBackend;
    typedef StateBackend& StateBackendRef;
    typedef typename cuckoohash_map<Key, Value>::mapped_type StateBackendMappedType;
    typedef typename cuckoohash_map<Key, Value>::locked_table LockedStateBackend;
    typedef LockedStateBackend& LockedStateBackendRef;
    typedef typename LockedStateBackend::const_iterator KeyValueRangeHandleConstIterator;
    typedef typename LockedStateBackend::iterator KeyValueRangeHandleIterator;

  private:
    StateId stateId;
    StateBackend backend;
    std::function<Value(const Key&)> defaultCallback;

  public:
    class KeyValueHandle {
        friend class StateVariable;

      private:
        StateBackendRef backend;
        Key key;
        std::function<Value(const Key&)> defaultCallback;

      private:
        explicit KeyValueHandle(StateBackend& backend, Key key, std::function<Value(const Key&)> defaultCallback)
            : backend(backend), key(key), defaultCallback(defaultCallback) {}

      public:
        /**
     * check for existence of a key-value pair
     * @return true if the key-value pair exists
     */
        explicit operator bool() { return contains(); }

        /**
    * check for existence of a key-value pair
    * @return true if the key-value pair exists
    */
        [[nodiscard]] bool contains() const { return backend.contains(key); }

        /**
     * Retrieves the actual value for a key
     * @return the actual value for a key
     */
        Value value() { return backend.find(key); }

        /**
     * Retrieves the actual value for a key.
     * If the value is not set yet, we set a default value.
     * @return the actual value for a key
     */
        template<typename... Arguments>
        Value valueOrDefault(Arguments&&... args) {
            if (!backend.contains(key)) {
                emplace(std::forward<Arguments>(args)...);
            }
            return value();
        }

        /**
        * Retrieves the actual value for a key.
        * If the value is not set yet, we set a default value.
        * @return the actual value for a key
        */
        Value valueOrDefaultWithCallback() {
            NES_VERIFY(defaultCallback, "invalid default callback");
            if (!backend.contains(key)) {
                emplace(defaultCallback(key));
            }
            return value();
        }

        /**
     * Perform a Read-modify-write of a key-value pair
     * @tparam F an invocable type void(StateBackendMappedType&)
     * @param func an invocable of type void(StateBackendMappedType&)
     * @return true on success
     */
        template<typename F, std::enable_if_t<std::is_invocable<F, void, StateBackendMappedType&>::value, int> = 0>
        bool rmw(F func) {
            return backend.update_fn(key, func);
        }

        /**
     * Set a new value for the key-value pair
     * @param newValue
     * @return self
     */
        KeyValueHandle& operator=(Value&& newValue) {
            emplace(std::move(newValue));
            return *this;
        }

        /**
     * Set a new value for the key-value pair
     * @param newValue
     * @return self
     */
        KeyValueHandle& operator=(Value newValue) {
            emplace(std::move(newValue));
            return *this;
        }

        /**
     * Set a new value for the key-value pair
     * @param newValue
     * @return self
     */
        void put(Value&& newValue) { emplace(std::move(newValue)); }

        /**
     * Set a new value for the key-value pair using emplace semantics
     * @tparam Arguments
     * @param args
     */
        template<typename... Arguments>
        void emplace(Arguments&&... args) {
            backend.insert_or_assign(key, detail::StateVariableEmplaceHelper<Value>::create(std::forward<Arguments>(args)...));
        }

        /**
     * Delete a key-value pair
     */
        void clear() { backend.erase(key); }
    };

    /**
   * Range of key-value pairs. Note that this object locks the range of values, i.e., holds a lock.
   */
    class KeyValueRangeHandle {
        friend class StateVariable;

      private:
        explicit KeyValueRangeHandle(StateBackendRef backend) : backend(backend.lock_table()) {}

      public:
        /**
     *
     * @return a const iterator to the selected range
     */
        KeyValueRangeHandleConstIterator cbegin() { return backend.cbegin(); }

        /**
     *
     * @return a const iterator to the selected range
     */
        KeyValueRangeHandleConstIterator cend() { return backend.cend(); }

        /**
     *
     * @return a iterator to the selected range
     */
        KeyValueRangeHandleIterator begin() { return backend.begin(); }

        /**
     *
     * @return a iterator to the selected range
     */
        KeyValueRangeHandleIterator end() { return backend.end(); }

      private:
        LockedStateBackend backend;
    };

  public:
    /**
     * @brief Creates a new state variable
     * @param name of the state variable
     * @param defaultCallback a function that gets called when retrieving a value not present in the state
     */
    explicit StateVariable(StateId stateId, std::function<Value(const Key&)> defaultCallback)
        : stateId(std::move(stateId)), backend(), defaultCallback(defaultCallback) {
        NES_ASSERT(this->defaultCallback, "invalid default callback");
    }

    /**
     * @brief Creates a new state variable
     * @param stateId of the state variable
     */
    explicit StateVariable(StateId stateId) : stateId(std::move(stateId)), backend(), defaultCallback(nullptr) {}

    /**
     * @brief Copy Constructor of a state variable
     * @param other the param to copy
     */
    StateVariable(const StateVariable<Key, Value>& other)
        : stateId(other.stateId), backend(other.backend), defaultCallback(other.defaultCallback) {
        // nop
    }

    /**
     * @brief Move Constructor of a state variable
     * @param other the param to move
     */
    StateVariable(StateVariable<Key, Value>&& other) { *this = std::move(other); }

    /**
     * @brief Destructor of a state variable. It frees all allocated resources.
     */
    virtual ~StateVariable() override { detail::StateVariableDestroyerHelper<Key, Value>::destroy(backend); }

    /**
     * @brief Copy assignment operator
     * @param other the param to copy
     * @return the same state variable
     */
    StateVariable& operator=(const StateVariable<Key, Value>& other) {
        stateId = other.stateId;
        backend = other.backend;
        defaultCallback = other.defaultCallback;
    }

    // TODO reimplement this use copy-and-swap: https://stackoverflow.com/questions/3279543/what-is-the-copy-and-swap-idiom
    /**
     * @brief Move assignment operator
     * @param other the param to move
     * @return the same state variable
     */
    StateVariable& operator=(StateVariable<Key, Value>&& other) {
        stateId = std::move(other.stateId);
        backend = std::move(other.backend);
        return *this;
    }

  public:
    /**
     * Point lookup of a key-value pair
     * @param key
     * @return an accessor to a key-value pair
     */
    KeyValueHandle get(Key key) { return KeyValueHandle(backend, key, defaultCallback); }

    /**
   * Point lookup of a key-value pair
   * @param key
   * @return an accessor to a key-value pair
   */
    KeyValueHandle operator[](Key&& key) { return KeyValueHandle(backend, key, defaultCallback); }

    /**
   * Point lookup of a key-value pair
   * @param key
   * @return an accessor to a key-value pair
   */
    KeyValueHandle operator[](const Key& key) { return KeyValueHandle(backend, key, defaultCallback); }

    KeyValueRangeHandle range(Key, Key) { assert(false && "not implemented yet"); }

    /**
   * Range of all key-value pairs
   * @param key
   * @return an accessor to a key-value pair
   */
    KeyValueRangeHandle rangeAll() { return KeyValueRangeHandle(backend); }
};
}// namespace Runtime
}// namespace NES
#endif// NES_CORE_INCLUDE_STATE_STATEVARIABLE_HPP_
