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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_

#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <memory>
#include <vector>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

namespace Configurations {
class OptimizerConfiguration;
}

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

class Request;
using NESRequestPtr = std::shared_ptr<Request>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

}// namespace NES

namespace NES::Optimizer {

class GlobalQueryPlanUpdatePhase;
using GlobalQueryPlanUpdatePhasePtr = std::shared_ptr<GlobalQueryPlanUpdatePhase>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class OriginIdInferencePhase;
using OriginIdInferencePhasePtr = std::shared_ptr<OriginIdInferencePhase>;

class TopologySpecificQueryRewritePhase;
using TopologySpecificQueryRewritePhasePtr = std::shared_ptr<TopologySpecificQueryRewritePhase>;

class SignatureInferencePhase;
using SignatureInferencePhasePtr = std::shared_ptr<SignatureInferencePhase>;

class QueryMergerPhase;
using QueryMergerPhasePtr = std::shared_ptr<QueryMergerPhase>;

class MemoryLayoutSelectionPhase;
using MemoryLayoutSelectionPhasePtr = std::shared_ptr<MemoryLayoutSelectionPhase>;

/**
 * @brief This class is responsible for accepting a batch of query requests and then updating the Global Query Plan accordingly.
 */
class GlobalQueryPlanUpdatePhase {
  public:
    /**
     * @brief Create an instance of the GlobalQueryPlanUpdatePhase
     * @param queryCatalogService: the catalog of queryIdAndCatalogEntryMapping
     * @param sourceCatalog: the catalog of sources
     * @param globalQueryPlan: the input global query plan
     * @param optimizerConfiguration: configuration for the optimizer
     * @return Shared pointer for the GlobalQueryPlanUpdatePhase
     */
    static GlobalQueryPlanUpdatePhasePtr create(TopologyPtr topology,
                                                QueryCatalogServicePtr queryCatalogService,
                                                Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                GlobalQueryPlanPtr globalQueryPlan,
                                                z3::ContextPtr z3Context,
                                                const Configurations::OptimizerConfiguration optimizerConfiguration,
                                                Catalogs::UDF::UdfCatalogPtr udfCatalog);

    /**
     * @brief This method executes the Global Query Plan Update Phase on a batch of query requests
     * @param queryRequests: a batch of query requests (in the form of Query Catalog Entry) to be processed to update global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(const std::vector<NESRequestPtr>& nesRequests);

  private:
    explicit GlobalQueryPlanUpdatePhase(TopologyPtr topology,
                                        QueryCatalogServicePtr queryCatalogService,
                                        const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                        GlobalQueryPlanPtr globalQueryPlan,
                                        z3::ContextPtr z3Context,
                                        const Configurations::OptimizerConfiguration optimizerConfiguration,
                                        const Catalogs::UDF::UdfCatalogPtr& udfCatalog);

    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    TopologySpecificQueryRewritePhasePtr topologySpecificQueryRewritePhase;
    Optimizer::QueryMergerPhasePtr queryMergerPhase;
    Optimizer::SignatureInferencePhasePtr signatureInferencePhase;
    OriginIdInferencePhasePtr originIdInferencePhase;
    MemoryLayoutSelectionPhasePtr setMemoryLayoutPhase;
    z3::ContextPtr z3Context;
};
}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_PHASES_GLOBALQUERYPLANUPDATEPHASE_HPP_
