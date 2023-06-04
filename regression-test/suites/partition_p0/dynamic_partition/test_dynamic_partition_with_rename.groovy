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

suite("test_dynamic_partition_with_rename") {
    def tbl = "test_dynamic_partition_with_rename"
    sql "drop table if exists ${tbl}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbl}
        ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1,k2)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="1",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1")
        """
    result = sql "show partitions from ${tbl}"
    assertEquals(7, result.size())

    // rename distributed column, then try to add too more dynamic partition
    sql "alter table ${tbl} rename column k1 renamed_k1"
    sql """ ADMIN SET FRONTEND CONFIG ('dynamic_partition_check_interval_seconds' = '1') """
    sql """ alter table ${tbl} set('dynamic_partition.end'='5') """
    result = sql "show partitions from ${tbl}"
    for (def retry = 0; retry < 15; retry++) {
        if (result.size() == 9) {
            break;
        }
        logger.info("wait dynamic partition scheduler, sleep 1s")
        sleep(1000);
        result = sql "show partitions from ${tbl}"
    }
    assertEquals(9, result.size())
    for (def line = 0; line < result.size(); line++) {
        // XXX: DistributionKey at pos(7), next maybe impl by sql meta
        assertEquals("renamed_k1", result.get(line).get(7))
    }

    sql "drop table ${tbl}"
}
