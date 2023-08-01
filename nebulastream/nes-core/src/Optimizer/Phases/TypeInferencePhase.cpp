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
#include <API/AttributeField.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <Plans/Query/QueryPlan.hpp>

#include <utility>
namespace NES::Optimizer {

TypeInferencePhase::TypeInferencePhase(Catalogs::Source::SourceCatalogPtr sourceCatalog, Catalogs::UDF::UdfCatalogPtr udfCatalog)
    : sourceCatalog(std::move(sourceCatalog)), udfCatalog(std::move(udfCatalog)) {
    NES_DEBUG("TypeInferencePhase()");
}

TypeInferencePhasePtr TypeInferencePhase::create(Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                 Catalogs::UDF::UdfCatalogPtr udfCatalog) {
    return std::make_shared<TypeInferencePhase>(TypeInferencePhase(std::move(sourceCatalog), std::move(udfCatalog)));
}

QueryPlanPtr TypeInferencePhase::execute(QueryPlanPtr queryPlan) {
    try {

        auto typeInferencePhaseContext = TypeInferencePhaseContext(sourceCatalog, udfCatalog);

        // first we have to check if all source operators have a correct source descriptors
        auto sources = queryPlan->getSourceOperators();

        if (!sources.empty() && !sourceCatalog) {
            NES_WARNING("TypeInferencePhase: No SourceCatalog specified!");
        }

        for (const auto& source : sources) {
            auto sourceDescriptor = source->getSourceDescriptor();

            // if the source descriptor has no schema set and is only a logical source we replace it with the correct
            // source descriptor form the catalog.
            if (sourceDescriptor->instanceOf<LogicalSourceDescriptor>() && sourceDescriptor->getSchema()->empty()) {
                auto logicalSourceName = sourceDescriptor->getLogicalSourceName();
                SchemaPtr schema = Schema::create();
                if (!sourceCatalog->containsLogicalSource(logicalSourceName)) {
                    NES_ERROR("Source name: " + logicalSourceName + " not registered.");
                }
                auto originalSchema = sourceCatalog->getSchemaForLogicalSource(logicalSourceName);
                schema = schema->copyFields(originalSchema);
                schema->setLayoutType(originalSchema->getLayoutType());
                std::string qualifierName = logicalSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR;
                //perform attribute name resolution
                for (auto& field : schema->fields) {
                    if (!field->getName().starts_with(qualifierName)) {
                        field->setName(qualifierName + field->getName());
                    }
                }
                sourceDescriptor->setSchema(schema);
                NES_DEBUG("TypeInferencePhase: update source descriptor for source " << logicalSourceName
                                                                                     << " with schema: " << schema->toString());
            }
        }

        // now we have to infer the input and output schemas for the whole query.
        // to this end we call at each sink the infer method to propagate the schemata across the whole query.
        auto sinks = queryPlan->getSinkOperators();
        for (auto& sink : sinks) {
            if (!sink->inferSchema(typeInferencePhaseContext)) {
                throw Exceptions::RuntimeException("TypeInferencePhase: Failed!");
            }
        }
        NES_DEBUG("TypeInferencePhase: we inferred all schemas");
        return queryPlan;
    } catch (std::exception& e) {
        NES_ERROR("TypeInferencePhase: Exception occurred during type inference phase " << e.what());
        auto queryId = queryPlan->getQueryId();
        throw TypeInferenceException(queryId, e.what());
    }
}

}// namespace NES::Optimizer