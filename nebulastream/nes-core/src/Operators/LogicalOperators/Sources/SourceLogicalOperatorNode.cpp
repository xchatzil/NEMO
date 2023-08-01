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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <utility>
#include <z3++.h>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(SourceDescriptorPtr const& sourceDescriptor, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), OriginIdAssignmentOperator(id), sourceDescriptor(sourceDescriptor) {}

SourceLogicalOperatorNode::SourceLogicalOperatorNode(SourceDescriptorPtr const& sourceDescriptor,
                                                     OperatorId id,
                                                     OriginId originId)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), OriginIdAssignmentOperator(id, originId),
      sourceDescriptor(sourceDescriptor) {}

bool SourceLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<SourceLogicalOperatorNode>()->getId() == id;
}

bool SourceLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<SourceLogicalOperatorNode>()) {
        auto sourceOperator = rhs->as<SourceLogicalOperatorNode>();
        return sourceOperator->getSourceDescriptor()->equal(sourceDescriptor);
    }
    return false;
}

std::string SourceLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << id << "," << sourceDescriptor->getLogicalSourceName() << "," << sourceDescriptor->toString() << ")";
    return ss.str();
}

SourceDescriptorPtr SourceLogicalOperatorNode::getSourceDescriptor() { return sourceDescriptor; }

bool SourceLogicalOperatorNode::inferSchema(Optimizer::TypeInferencePhaseContext&) {
    inputSchema = sourceDescriptor->getSchema();
    outputSchema = sourceDescriptor->getSchema();
    return true;
}

void SourceLogicalOperatorNode::setSourceDescriptor(SourceDescriptorPtr sourceDescriptor) {
    this->sourceDescriptor = std::move(sourceDescriptor);
}

void SourceLogicalOperatorNode::setProjectSchema(SchemaPtr schema) { projectSchema = std::move(schema); }

OperatorNodePtr SourceLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createSourceOperator(sourceDescriptor, id, originId);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    if (copy->instanceOf<SourceLogicalOperatorNode>()) {
        copy->as<SourceLogicalOperatorNode>()->setProjectSchema(projectSchema);
    }
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void SourceLogicalOperatorNode::inferStringSignature() {
    //Update the signature
    auto hashCode = hashGenerator("SOURCE(" + sourceDescriptor->getLogicalSourceName() + ")");
    hashBasedSignature[hashCode] = {"SOURCE(" + sourceDescriptor->getLogicalSourceName() + ")"};
}

void SourceLogicalOperatorNode::inferInputOrigins() {
    // Data sources have no input origins.
}

std::vector<OriginId> SourceLogicalOperatorNode::getOutputOriginIds() { return OriginIdAssignmentOperator::getOutputOriginIds(); }

}// namespace NES
