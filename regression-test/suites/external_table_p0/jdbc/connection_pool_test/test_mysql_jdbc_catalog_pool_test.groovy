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

suite("test_mysql_jdbc_catalog_pool_test", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        def poolOptions = [true, false]

        poolOptions.each { poolEnabled ->
            String poolState = poolEnabled ? "pool_true" : "pool_false"
            String catalog_name = "mysql_jdbc_catalog_${poolState}";

            sql """ drop catalog if exists ${catalog_name} """
            sql """ create catalog if not exists ${catalog_name} properties(
                        "type"="jdbc",
                        "user"="root",
                        "password"="123456",
                        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "com.mysql.cj.jdbc.Driver",
                        "enable_connection_pool" = "${poolEnabled}"
            );"""

            def tasks = (1..5).collect {
                Thread.start {
                    sql """ switch ${catalog_name} """
                    sql """ use doris_test; """
                    qt_mysql_all_types """ select * from all_types order by tinyint_u; """
                }
            }

            tasks*.join()

            sql """ refresh catalog ${catalog_name} """

            def refreshTasks = (1..5).collect {
                Thread.start {
                    sql """ switch ${catalog_name} """
                    sql """ use doris_test; """
                    qt_mysql_all_types_refresh """ select * from all_types order by tinyint_u; """
                }
            }

            refreshTasks*.join()

            sql """ drop catalog if exists ${catalog_name} """
        }
    }
}


