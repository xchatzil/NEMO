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

#include <Runtime/TupleBuffer.hpp>
#include <Sources/MaterializedViewSource.hpp>
#include <Views/MaterializedView.hpp>

namespace NES::Experimental::MaterializedView {

MaterializedViewSource::MaterializedViewSource(SchemaPtr schema,
                                               Runtime::BufferManagerPtr bufferManager,
                                               Runtime::QueryManagerPtr queryManager,
                                               OperatorId operatorId,
                                               OriginId originId,
                                               size_t numSourceLocalBuffers,
                                               GatheringMode::Value gatheringMode,
                                               std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                                               MaterializedViewPtr view)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(successors)),
      view(std::move(view)){};

std::optional<Runtime::TupleBuffer> MaterializedViewSource::receiveData() { return view->receiveData(); };

std::string MaterializedViewSource::toString() const { return Configurations::MATERIALIZEDVIEW_SOURCE_CONFIG; };

SourceType MaterializedViewSource::getType() const { return MATERIALIZEDVIEW_SOURCE; };

uint64_t MaterializedViewSource::getViewId() const { return view->getId(); }

}// namespace NES::Experimental::MaterializedView