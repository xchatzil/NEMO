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

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <utility>
#include <z3++.h>

namespace NES {
SinkLogicalOperatorNode::SinkLogicalOperatorNode(const SinkDescriptorPtr& sinkDescriptor, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), sinkDescriptor(sinkDescriptor) {}

SinkDescriptorPtr SinkLogicalOperatorNode::getSinkDescriptor() { return sinkDescriptor; }

void SinkLogicalOperatorNode::setSinkDescriptor(SinkDescriptorPtr sd) { this->sinkDescriptor = std::move(sd); }

bool SinkLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<SinkLogicalOperatorNode>()->getId() == id;
}

bool SinkLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<SinkLogicalOperatorNode>()) {
        auto sinkOperator = rhs->as<SinkLogicalOperatorNode>();
        return sinkOperator->getSinkDescriptor()->equal(sinkDescriptor);
    }
    return false;
};

bool SinkLogicalOperatorNode::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    return LogicalUnaryOperatorNode::inferSchema(typeInferencePhaseContext);
}

std::string SinkLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SINK(" << id << ": {" << sinkDescriptor->toString() << "})";
    return ss.str();
}

OperatorNodePtr SinkLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void SinkLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("Inferring String signature for " << operatorNode->toString());

    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperatorNode>()->getHashBasedSignature();
    signatureStream << "SINK()." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
