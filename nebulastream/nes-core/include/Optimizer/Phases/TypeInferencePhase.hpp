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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASE_HPP_
#include <memory>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Node;
using NodePtr = std::shared_ptr<Node>;
class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}// namespace UDF

}// namespace Catalogs

}// namespace NES

namespace NES::Optimizer {

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

/**
 * @brief The type inference phase receives and query plan and infers all input and output schemata for all operators.
 * If this is not possible it throws an Runtime exception.
 */
class TypeInferencePhase {
  public:
    /**
     * @brief Factory method to create a type inference phase.
     * @return TypeInferencePhasePtr
     */
    static TypeInferencePhasePtr create(Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                        Catalogs::UDF::UdfCatalogPtr udfCatalog);

    /**
     * @brief Performs type inference on the given query plan.
     * This involves the following steps.
     * 1. Replacing a logical source descriptor with the correct source descriptor form the source catalog.
     * 2. Propagate the input and output schemas from source operators to the sink operators.
     * 3. If a operator contains expression, we infer the result stamp of this operators.
     * @param QueryPlanPtr the query plan
     * @throws RuntimeException if it was not possible to infer the data types of schemas and expression
     * @return QueryPlanPtr
     * @throws TypeInferenceException
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

  private:
    /**
     * @brief creates the corresponding source descriptor from a given source name.
     * @param logicalSourceName
     * @return SourceDescriptorPtr
     */
    SourceDescriptorPtr createSourceDescriptor(std::string sourceName);
    explicit TypeInferencePhase(Catalogs::Source::SourceCatalogPtr sourceCatalog, Catalogs::UDF::UdfCatalogPtr udfCatalog);
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
};
}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASE_HPP_
