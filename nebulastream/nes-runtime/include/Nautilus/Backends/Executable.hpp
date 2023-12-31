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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
#include <string>
namespace NES::Nautilus::Backends {

/**
 * @brief Represents an executable object, which enables the invocation of dynamically defined methods.
 */
class Executable {
  public:
    template<typename Function>
    Function getInvocableMember(const std::string& member) {
        return reinterpret_cast<Function>(getInvocableFunctionPtr(member));
    }
    virtual ~Executable() = default;

  protected:
    /**
     * @brief Returns a untyped function pointer to a specific symbol.
     * @param member on the dynamic object, currently provided as a MangledName.
     * @return function ptr
     */
    [[nodiscard]] virtual void* getInvocableFunctionPtr(const std::string& member) = 0;
};

}// namespace NES::Nautilus::Backends
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
