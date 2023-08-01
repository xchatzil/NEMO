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

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <sstream>

namespace NES::Windowing {

ThresholdWindow::ThresholdWindow(ExpressionNodePtr predicate) : ContentBasedWindowType(), predicate(std::move(predicate)) {}

ThresholdWindow::ThresholdWindow(ExpressionNodePtr predicate, uint64_t minCount)
    : ContentBasedWindowType(), predicate(std::move(predicate)), minimumCount(std::move(minCount)) {}

WindowTypePtr ThresholdWindow::of(ExpressionNodePtr predicate) {
    return std::reinterpret_pointer_cast<WindowType>(std::make_shared<ThresholdWindow>(ThresholdWindow(std::move(predicate))));
}

WindowTypePtr ThresholdWindow::of(ExpressionNodePtr predicate, uint64_t minimumCount) {
    return std::reinterpret_pointer_cast<WindowType>(
        std::make_shared<ThresholdWindow>(ThresholdWindow(std::move(predicate), std::move(minimumCount))));
}

bool ThresholdWindow::equal(WindowTypePtr otherWindowType) {
    if (otherWindowType->isThresholdWindow()) {
        return std::dynamic_pointer_cast<ThresholdWindow>(otherWindowType)->getPredicate()->equal(this->getPredicate());
    } else {
        return false;
    }
}

bool ThresholdWindow::isThresholdWindow() { return true; }

const ExpressionNodePtr& ThresholdWindow::getPredicate() const { return predicate; }

uint64_t ThresholdWindow::getMinimumCount() { return minimumCount; }

std::string ThresholdWindow::toString() {
    std::stringstream ss;
    ss << "Threshold Window: predicate ";
    ss << predicate->toString();
    ss << "and minimumCount";
    ss << minimumCount;
    ss << std::endl;
    return ss.str();
}

}// namespace NES::Windowing