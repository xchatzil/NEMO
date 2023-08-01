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

#ifndef NES_COMMON_INCLUDE_UTIL_NONBLOCKINGMONOTONICSEQQUEUE_HPP_
#define NES_COMMON_INCLUDE_UTIL_NONBLOCKINGMONOTONICSEQQUEUE_HPP_

#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <array>
#include <assert.h>
#include <atomic>
#include <memory>

namespace NES::Util {

/**
 * @brief This class implements a non blocking monotonic sequence queue,
 * which is mainly used as a foundation for an efficient watermark processor.
 * This queue receives values of type T with an associated sequence number and
 * returns the value with the highest sequence number in strictly monotonic increasing order.
 *
 * Internally this queue is implemented as a linked-list of blocks and each block stores a array of <seq,T> pairs.
 * |------- |       |------- |
 * | s1, s2 | ----> | s3, s4 |
 * |------- |       | ------ |
 *
 * The high level flow is the following:
 * If we receive the following sequence of input pairs <seq,T>:
 * Note that the events with seq 3 and 5 are received out-of-order.
 *
 * [<1,T1>,<2,T2>,<4,T4>,<6,T6>,<3,T3>,<7,T7>,<5,T5>]
 *
 * GetCurrentValue will return the following sequence of current values:
 * [<1,T1>,<2,T2>,<2,T2>,<2,T2>,<2,T2>,<4,T6>,<7,T7>]
 *
 *
 * @tparam T
 * @tparam blockSize
 */
template<class T, uint64_t blockSize = 10000>
class NonBlockingMonotonicSeqQueue {
  private:
    /**
     * @brief Container, which contains the sequence number and the value.
     */
    struct Container {
        uint64_t seq;
        T value;
    };

    /**
     * @brief Block of values, which is one element in the linked-list.
     * If the next block exists *next* contains the reference.
     */
    class Block {
      public:
        Block(uint64_t blockIndex) : blockIndex(blockIndex){};
        ~Block() = default;
        const uint64_t blockIndex;
        std::array<Container, blockSize> log = {};
        std::shared_ptr<Block> next = std::shared_ptr<Block>();
    };

  public:
    NonBlockingMonotonicSeqQueue() : head(std::make_shared<Block>(0)), currentSeq(0) {}
    ~NonBlockingMonotonicSeqQueue() {}

    /**
     * @brief Emplace a new element to the queue.
     * This method can be called concurrently.
     * However, only one element with a given sequence number can be inserted.
     * @param sequenceNumber of the new element.
     * @param value of the new value.
     * @throws RuntimeException if an element with the same sequence number was already inserted.
     */
    void emplace(uint64_t sequenceNumber, T value) {
        // First emplace the value to the specific block of the sequenceNumber.
        // After this call it is safe to assume that a block, which contains the sequenceNumber exists.
        emplaceValueInBlock(sequenceNumber, value);
        // Try to shift the current sequence number
        shiftCurrentValue();
    }

    /**
     * @brief Returns the current value.
     * This method is thread save, however it is not guaranteed that current value dose not change concurrently.
     * @return T value
     */
    auto getCurrentValue() {
        auto currentBlock = std::atomic_load(&head);
        // get the current sequence number and access the associated block
        auto currentSequenceNumber = currentSeq.load();
        auto targetBlockIndex = currentSequenceNumber / blockSize;
        currentBlock = getTargetBlock(currentBlock, targetBlockIndex);
        // read the value from the correct slot.
        auto seqIndexInBlock = currentSequenceNumber % blockSize;
        auto& value = currentBlock->log[seqIndexInBlock];
        return value.value;
    }

  private:
    /**
     * @brief Emplace a value T to the specific location of the passed sequence number
     *
     * The method is split in two phased:
     * 1. Find the correct block for this sequence number. If the block not yet exists we add a new block to the linked-list.
     * 2. Place value at the correct slot associated with the sequence number.
     *
     * @param seq the sequence number of the value
     * @param value the value that should be stored.
     */
    void emplaceValueInBlock(uint64_t seq, T value) {
        // Each block contains blockSize elements and covers sequence numbers from
        // [blockIndex * blockSize] till [blockIndex * blockSize + blockSize]
        // Calculate the target block index, which contains the sequence number
        auto targetBlockIndex = seq / blockSize;
        // Lookup the current block
        auto currentBlock = std::atomic_load(&head);
        // if the blockIndex is smaller the target block index we travers the next block
        while (currentBlock->blockIndex < targetBlockIndex) {
            // append new block if the next block is a nullptr
            auto nextBlock = std::atomic_load(&currentBlock->next);
            if (nextBlock == nullptr) {
                auto newBlock = std::make_shared<Block>(currentBlock->blockIndex + 1);
                std::atomic_compare_exchange_weak(&currentBlock->next, &nextBlock, newBlock);
                // we don't care if this or another thread succeeds, as we just start over again in the loop
                // and use what ever is now stored in currentBlock.next
            } else {
                // move to the next block
                currentBlock = nextBlock;
            }
        }

        // check if we really found the correct block
        if (!(seq >= currentBlock->blockIndex * blockSize && seq < currentBlock->blockIndex * blockSize + blockSize)) {
            NES_THROW_RUNTIME_ERROR("The found block is wrong");
        }

        // Emplace value in block
        // It is safe to perform this operation without atomics as no other thread can't the same sequence number,
        // and thus can't modify this value.
        // Concurrent can also not happen yet as the currentSeq is only modified in shiftCurrent.
        auto seqIndexInBlock = seq % blockSize;
        currentBlock->log[seqIndexInBlock].seq = seq;
        currentBlock->log[seqIndexInBlock].value = value;
    }

    /**
     * @brief This method shifts tries to shift the current value.
     * To this end, it checks if the next expected sequence number (currentSeq + 1) is already inserted.
     * If the next sequence number is available it replaces the currentSeq with the next one.
     * If the next sequence number is in a new block this method also replaces the pointer to the next block.
     */
    void shiftCurrentValue() {
        auto checkForUpdate = true;
        while (checkForUpdate) {
            auto currentBlock = std::atomic_load(&head);
            // we are looking for the next sequence number
            auto currentSequenceNumber = currentSeq.load();
            // find the correct block, that contains the current sequence number.
            auto targetBlockIndex = currentSequenceNumber / blockSize;
            currentBlock = getTargetBlock(currentBlock, targetBlockIndex);

            // check if next value is set
            // next seqNumber
            auto nextSeqNumber = currentSequenceNumber + 1;
            if (nextSeqNumber % blockSize == 0) {
                // the next sequence number is the first element in the next block.
                auto nextBlock = std::atomic_load(&currentBlock->next);
                if (nextBlock != nullptr) {
                    // this will always be the first element
                    auto& value = nextBlock->log[0];
                    if (value.seq == nextSeqNumber) {
                        // Modify currentSeq and head
                        if (std::atomic_compare_exchange_weak(&currentSeq, &currentSequenceNumber, nextSeqNumber)) {
                            NES_TRACE("Swap HEAD: remove " << currentBlock->blockIndex << " - " << currentBlock.use_count());
                            std::atomic_compare_exchange_weak(&head, &currentBlock, nextBlock);
                        };
                        continue;
                    }
                }
            } else {
                auto seqIndexInBlock = nextSeqNumber % blockSize;
                auto& value = currentBlock->log[seqIndexInBlock];
                if (value.seq == nextSeqNumber) {
                    // the next sequence number is still in the current block thus we only have to exchange the currentSeq.
                    std::atomic_compare_exchange_weak(&currentSeq, &currentSequenceNumber, nextSeqNumber);
                    continue;
                }
            }
            checkForUpdate = false;
        }
    }

    /**
     * @brief This function traverses the linked list of blocks, till the target block index is found.
     * It assumes, that the target block index exists. If not, the function throws a runtime exception.
     * @param currentBlock the start block, usually the head.
     * @param targetBlockIndex the target address
     * @return the found block, which contains the target block index.
     */
    std::shared_ptr<Block> getTargetBlock(std::shared_ptr<Block> currentBlock, uint64_t targetBlockIndex) {
        while (currentBlock->blockIndex < targetBlockIndex) {
            // append new block if the next block is a nullptr
            auto nextBlock = std::atomic_load(&currentBlock->next);
            if (!nextBlock) {
                NES_THROW_RUNTIME_ERROR("The next block dose not exists. This should not happen here.");
            }
            // move to the next block
            currentBlock = nextBlock;
        }
        return currentBlock;
    }

    // Stores a reference to the current block
    std::shared_ptr<Block> head;
    // Stores the current sequence number
    std::atomic<uint64_t> currentSeq;
};

}// namespace NES::Util

#endif// NES_COMMON_INCLUDE_UTIL_NONBLOCKINGMONOTONICSEQQUEUE_HPP_
