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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcHive2Client extends JdbcClient {
    protected JdbcHive2Client(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
        Connection conn = null;
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            conn = super.getConnection();
        } catch (JdbcClientException e) {
            closeClient();
            throw new JdbcClientException("Failed to initialize JdbcHive2Client: %s", e.getMessage());
        } finally {
            close(conn);
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    protected String getCatalogName(Connection conn) throws SQLException {
        return "";
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String hiveType = fieldSchema.getDataTypeName().orElse("unknown");
        switch (hiveType) {
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "INT":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "TIMESTAMP":
            case "DATETIME": {
                // hive can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DECIMAL": {
                int precision = fieldSchema.requiredColumnSize();
                int scale = fieldSchema.requiredDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "CHAR":
                return ScalarType.createCharType(fieldSchema.requiredColumnSize());
            case "VARCHAR":
                return ScalarType.createVarcharType(fieldSchema.requiredColumnSize());
            case "STRING":
            case "BINARY":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
