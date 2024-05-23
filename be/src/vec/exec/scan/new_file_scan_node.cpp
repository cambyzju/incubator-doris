// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/scan/new_file_scan_node.h"

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <ostream>

#include "common/config.h"
#include "common/object_pool.h"
#include "runtime/client_cache.h"
#include "vec/exec/scan/vfile_scanner.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {
class DescriptorTbl;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {

using apache::thrift::transport::TTransportException;

NewFileScanNode::NewFileScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs) {
    _output_tuple_id = tnode.file_scan_node.tuple_id;
    _table_name = tnode.file_scan_node.__isset.table_name ? tnode.file_scan_node.table_name : "";
}

Status NewFileScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::init(tnode, state));
    return Status::OK();
}

Status NewFileScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::prepare(state));
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.count(id()) > 0) {
        TFileScanRangeParams& params = state->get_query_ctx()->file_scan_range_params_map[id()];
        _output_tuple_id = params.dest_tuple_id;
    }
    return Status::OK();
}

void NewFileScanNode::set_scan_ranges(RuntimeState* state,
                                      const std::vector<TScanRangeParams>& scan_ranges) {
    _max_scanners =
            config::doris_scanner_thread_pool_thread_num / state->query_parallel_instance_num();
    _max_scanners = std::max(std::max(_max_scanners, state->parallel_scan_max_scanners_count()), 1);
    // For select * from table limit 10; should just use one thread.
    if (should_run_serial()) {
        _max_scanners = 1;
    }
    if (scan_ranges.size() == 1) {
        auto scan_range = scan_ranges[0].scan_range.ext_scan_range.file_scan_range;
        if (scan_range.__isset.split_source) {
            auto split_source = scan_range.split_source;
            _split_source = std::make_shared<RemoteSplitSourceConnector>(
                    state, split_source.split_source_id, split_source.num_splits);
        }
    }
    if (_split_source == nullptr) {
        _split_source = std::make_shared<LocalSplitSourceConnector>(scan_ranges);
    }
    _max_scanners = std::min(_max_scanners, _split_source->num_scan_ranges());
    if (scan_ranges.size() > 0 &&
        scan_ranges[0].scan_range.ext_scan_range.file_scan_range.__isset.params) {
        // for compatibility.
        // in new implement, the tuple id is set in prepare phase
        _output_tuple_id =
                scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params.dest_tuple_id;
    }
}

Status NewFileScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());
    return Status::OK();
}

Status NewFileScanNode::_process_conjuncts() {
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    return Status::OK();
}

Status NewFileScanNode::_init_scanners(std::list<VScannerSPtr>* scanners) {
    if (_split_source->num_scan_ranges() == 0) {
        _eos = true;
        return Status::OK();
    }

    size_t shard_num = std::min<size_t>(
            config::doris_scanner_thread_pool_thread_num / _state->query_parallel_instance_num(),
            _max_scanners);
    shard_num = std::max(shard_num, (size_t)1);
    _kv_cache.reset(new ShardedKVCache(shard_num));
    for (int i = 0; i < _max_scanners; ++i) {
        std::unique_ptr<VFileScanner> scanner =
                VFileScanner::create_unique(_state, this, _limit_per_scanner, _split_source,
                                            runtime_profile(), _kv_cache.get());
        RETURN_IF_ERROR(
                scanner->prepare(_conjuncts, &_colname_to_value_range, &_colname_to_slot_id));
        scanners->push_back(std::move(scanner));
    }
    return Status::OK();
}

std::string NewFileScanNode::get_name() {
    return fmt::format("VFILE_SCAN_NODE({0})", _table_name);
}

}; // namespace doris::vectorized
