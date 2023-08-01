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
#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES::QueryCompilation {

QueryCompilerOptions::OutputBufferOptimizationLevel QueryCompilerOptions::getOutputBufferOptimizationLevel() const {
    return outputBufferOptimizationLevel;
};

void QueryCompilerOptions::setOutputBufferOptimizationLevel(QueryCompilerOptions::OutputBufferOptimizationLevel level) {
    this->outputBufferOptimizationLevel = level;
};

void QueryCompilerOptions::setNumSourceLocalBuffers(uint64_t num) { this->numSourceLocalBuffers = num; }

uint64_t QueryCompilerOptions::getNumSourceLocalBuffers() const { return numSourceLocalBuffers; }

QueryCompilerOptionsPtr QueryCompilerOptions::createDefaultOptions() {
    auto options = QueryCompilerOptions();
    options.setCompilationStrategy(OPTIMIZE);
    options.setPipeliningStrategy(OPERATOR_FUSION);
    options.setFilterProcessingStrategy(BRANCHED);
    options.setNumSourceLocalBuffers(64);
    options.setOutputBufferOptimizationLevel(ALL);
    options.setWindowingStrategy(DEFAULT);
    options.setQueryCompiler(DEFAULT_QUERY_COMPILER);
    return std::make_shared<QueryCompilerOptions>(options);
}
QueryCompilerOptions::PipeliningStrategy QueryCompilerOptions::getPipeliningStrategy() const { return pipeliningStrategy; }

void QueryCompilerOptions::setPipeliningStrategy(QueryCompilerOptions::PipeliningStrategy pipeliningStrategy) {
    this->pipeliningStrategy = pipeliningStrategy;
}

void QueryCompilerOptions::setQueryCompiler(NES::QueryCompilation::QueryCompilerOptions::QueryCompiler queryCompiler) {
    this->queryCompiler = queryCompiler;
}

QueryCompilerOptions::QueryCompiler QueryCompilerOptions::getQueryCompiler() const { return queryCompiler; }

QueryCompilerOptions::CompilationStrategy QueryCompilerOptions::getCompilationStrategy() const { return compilationStrategy; }

void QueryCompilerOptions::setCompilationStrategy(QueryCompilerOptions::CompilationStrategy compilationStrategy) {
    this->compilationStrategy = compilationStrategy;
}

void QueryCompilerOptions::setFilterProcessingStrategy(FilterProcessingStrategy filterProcessingStrategy) {
    this->filterProcessingStrategy = filterProcessingStrategy;
}

QueryCompilerOptions::FilterProcessingStrategy QueryCompilerOptions::getFilterProcessingStrategy() const {
    return filterProcessingStrategy;
}
QueryCompilerOptions::WindowingStrategy QueryCompilerOptions::getWindowingStrategy() const { return windowingStrategy; }
void QueryCompilerOptions::setWindowingStrategy(QueryCompilerOptions::WindowingStrategy windowingStrategy) {
    QueryCompilerOptions::windowingStrategy = windowingStrategy;
}

}// namespace NES::QueryCompilation