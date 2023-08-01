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

#include "Util/Logger/Logger.hpp"
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>

namespace NES::Nautilus::IR {
BasicBlock::BasicBlock(std::string identifier,
                       int32_t scopeLevel,
                       std::vector<Operations::OperationPtr> operations,
                       std::vector<std::shared_ptr<Operations::BasicBlockArgument>> arguments)
    : identifier(std::move(identifier)), scopeLevel(scopeLevel), numLoopBackEdges(0), operations(std::move(operations)),
      arguments(std::move(arguments)) {}

std::string BasicBlock::getIdentifier() { return identifier; }
void BasicBlock::setIdentifier(std::string identifier) { this->identifier = identifier; }
uint32_t BasicBlock::getScopeLevel() { return scopeLevel; }
void BasicBlock::setScopeLevel(uint32_t scopeLevel) { this->scopeLevel = scopeLevel; }
uint32_t BasicBlock::getNumLoopBackEdges() { return numLoopBackEdges; }
void BasicBlock::incrementNumLoopBackEdge() { ++this->numLoopBackEdges; }
bool BasicBlock::isLoopHeaderBlock() { return numLoopBackEdges > 0; }
std::vector<Operations::OperationPtr> BasicBlock::getOperations() { return operations; }
Operations::OperationPtr BasicBlock::getTerminatorOp() { return operations.back(); }
std::vector<std::shared_ptr<Operations::BasicBlockArgument>> BasicBlock::getArguments() { return arguments; }
uint64_t BasicBlock::getIndexOfArgument(std::shared_ptr<Operations::Operation> arg) {
    for (uint64_t i = 0; i < arguments.size(); i++) {
        if (arguments[i] == arg)
            return i;
    }
    return -1;
}

void BasicBlock::popOperation() { operations.pop_back(); }

// NESIR Assembly
std::shared_ptr<BasicBlock> BasicBlock::addOperation(Operations::OperationPtr operation) {
    operations.push_back(operation);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addLoopHeadBlock(BasicBlockPtr loopHeadBlock) {
    std::static_pointer_cast<Operations::LoopOperation>(this->operations.back())->getLoopHeadBlock().setBlock(loopHeadBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addNextBlock(std::shared_ptr<BasicBlock> nextBlock) {
    std::static_pointer_cast<Operations::BranchOperation>(this->operations.back())->getNextBlockInvocation().setBlock(nextBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addTrueBlock(std::shared_ptr<BasicBlock> thenBlock) {
    std::static_pointer_cast<Operations::IfOperation>(this->operations.back())->getTrueBlockInvocation().setBlock(thenBlock);
    return shared_from_this();
}
std::shared_ptr<BasicBlock> BasicBlock::addFalseBlock(std::shared_ptr<BasicBlock> elseBlock) {
    std::static_pointer_cast<Operations::IfOperation>(this->operations.back())->getFalseBlockInvocation().setBlock(elseBlock);
    return shared_from_this();
}

void BasicBlock::addPredecessor(std::shared_ptr<BasicBlock> predecessor) { this->predecessors.emplace_back(predecessor); }
std::vector<std::weak_ptr<BasicBlock>>& BasicBlock::getPredecessors() { return predecessors; }
void BasicBlock::addNextBlock(std::shared_ptr<BasicBlock> nextBlock, std::vector<Operations::OperationPtr> ops) {
    auto branchOp = std::make_shared<Operations::BranchOperation>();
    auto& nextBlockIn = branchOp->getNextBlockInvocation();
    nextBlockIn.setBlock(nextBlock);
    for (auto op : ops) {
        nextBlockIn.addArgument(op);
    }
    addOperation(branchOp);
    // add this block as a predecessor to the next block
    //Todo #3167 : can we use this to replace the addPredecessor pass? (also: addTrueBlock, and addFalseBlock)
    // nextBlock->addPredecessor(shared_from_this());
}
void BasicBlock::removeOperation(Operations::OperationPtr operation) {
    operations.erase(std::find(operations.begin(), operations.end(), operation));
}
void BasicBlock::addOperationBefore(Operations::OperationPtr before, Operations::OperationPtr operation) {
    auto position = std::find(operations.begin(), operations.end(), before);
    operations.insert(position, operation);
}

[[nodiscard]] std::pair<std::shared_ptr<BasicBlock>, std::shared_ptr<BasicBlock>> BasicBlock::getNextBlocks() {
    // Depending on the type of the last operation we return the reference to the next block.
    NES_ASSERT(!operations.empty(), "A block can't be empty at this point");
    auto& termination = operations.back();
    if (termination->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(termination);
        return std::make_pair(branchOp->getNextBlockInvocation().getBlock(), nullptr);
    } else if (termination->getOperationType() == IR::Operations::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(termination);
        return std::make_pair(ifOp->getTrueBlockInvocation().getBlock(), ifOp->getFalseBlockInvocation().getBlock());
    } else if (termination->getOperationType() == IR::Operations::Operation::LoopOp) {
        // Todo change in #3169
        auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(termination);
        auto loopHeaderIfOp =
            std::static_pointer_cast<IR::Operations::IfOperation>(loopOp->getLoopHeadBlock().getBlock()->getTerminatorOp());
        return std::make_pair(loopHeaderIfOp->getTrueBlockInvocation().getBlock(),
                              loopHeaderIfOp->getFalseBlockInvocation().getBlock());
    } else if (termination->getOperationType() == IR::Operations::Operation::ReturnOp) {
        return {};
    } else {
        NES_ERROR("BasicBlock::getNextBlocks: Tried to get next block for unsupported operation type: "
                  << termination->getOperationType());
        NES_NOT_IMPLEMENTED();
    }
}

}// namespace NES::Nautilus::IR