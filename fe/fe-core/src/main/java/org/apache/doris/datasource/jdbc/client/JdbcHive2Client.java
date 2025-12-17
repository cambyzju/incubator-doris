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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

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
        // For mysql type: "INT UNSIGNED":
        // fieldSchema.getDataTypeName().orElse("unknown").split(" ")[0] == "INT"
        // fieldSchema.getDataTypeName().orElse("unknown").split(" ")[1] == "UNSIGNED"
        String[] typeFields = fieldSchema.getDataTypeName().orElse("unknown").split(" ");
        String mysqlType = typeFields[0];
        // For unsigned int, should extend the type.
        if (typeFields.length > 1 && "UNSIGNED".equals(typeFields[1])) {
            switch (mysqlType) {
                case "TINYINT":
                    return Type.SMALLINT;
                case "SMALLINT":
                case "MEDIUMINT":
                    return Type.INT;
                case "INT":
                    return Type.BIGINT;
                case "BIGINT":
                    return Type.LARGEINT;
                case "DECIMAL": {
                    int precision = fieldSchema.requiredColumnSize() + 1;
                    int scale = fieldSchema.requiredDecimalDigits();
                    return createDecimalOrStringType(precision, scale);
                }
                case "DOUBLE":
                    // As of MySQL 8.0.17, the UNSIGNED attribute is deprecated
                    // for columns of type FLOAT, DOUBLE, and DECIMAL (and any synonyms)
                    // https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html
                    // The maximum value may cause errors due to insufficient accuracy
                    return Type.DOUBLE;
                case "FLOAT":
                    return Type.FLOAT;
                default:
                    throw new JdbcClientException("Unknown UNSIGNED type of mysql, type: [" + mysqlType + "]");
            }
        }
        switch (mysqlType) {
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
            case "YEAR":
                return Type.SMALLINT;
            case "MEDIUMINT":
            case "INT":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "TIMESTAMP":
            case "DATETIME": {
                // mysql can support microsecond
                // use columnSize to calculate the precision of timestamp/datetime
                int columnSize = fieldSchema.requiredColumnSize();
                int scale = columnSize > 19 ? columnSize - 20 : 0;
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
            case "BIT":
                if (fieldSchema.requiredColumnSize() == 1) {
                    return Type.BOOLEAN;
                } else {
                    return ScalarType.createStringType();
                }
            case "JSON":
            case "TIME":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "STRING":
            case "SET":
            case "BINARY":
            case "VARBINARY":
            case "ENUM":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }

    /**
     * get all columns like DatabaseMetaData.getColumns in mysql-jdbc-connector
     */
    private Map<String, String> getColumnsDataTypeUseQuery(String remoteDbName, String remoteTableName) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        Map<String, String> fieldToType = Maps.newHashMap();

        StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
        queryBuf.append("`").append(remoteTableName).append("`");
        queryBuf.append(" FROM ");
        queryBuf.append("`").append(remoteDbName).append("`");
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(queryBuf.toString());
            while (resultSet.next()) {
                // get column name
                String fieldName = resultSet.getString("Field");
                // get original type name
                String typeName = resultSet.getString("Type");
                fieldToType.put(fieldName, typeName);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get jdbc columns info for remote table `%s.%s`: %s",
                    remoteDbName, remoteTableName, Util.getRootCauseMessage(e));
        } finally {
            close(resultSet, stmt, conn);
        }
        return fieldToType;
    }

    private Type dorisTypeToDoris(JdbcFieldSchema fieldSchema) {
        String type = fieldSchema.getDataTypeName().orElse("unknown").toUpperCase();
        if (type == null || type.isEmpty()) {
            return Type.UNSUPPORTED;
        }

        String upperType = type.toUpperCase();

        // For ARRAY type
        if (upperType.startsWith("ARRAY")) {
            String innerType = upperType.substring(6, upperType.length() - 1).trim();
            JdbcFieldSchema innerFieldSchema = new JdbcFieldSchema(fieldSchema);
            innerFieldSchema.setDataTypeName(Optional.of(innerType));
            Type arrayInnerType = dorisTypeToDoris(innerFieldSchema);
            return ArrayType.create(arrayInnerType, true);
        }

        int openParen = upperType.indexOf("(");
        String baseType = (openParen == -1) ? upperType : upperType.substring(0, openParen);

        switch (baseType) {
            case "BOOL":
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "TINYINT":
                return Type.TINYINT;
            case "INT":
                return Type.INT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "BIGINT":
                return Type.BIGINT;
            case "LARGEINT":
                return Type.LARGEINT;
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DECIMAL":
            case "DECIMALV3": {
                int precision = fieldSchema.requiredColumnSize();
                int scale = fieldSchema.requiredDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "DATE":
            case "DATEV2":
                return ScalarType.createDateV2Type();
            case "DATETIME":
            case "DATETIMEV2": {
                int scale = (openParen == -1) ? 6
                        : Integer.parseInt(upperType.substring(openParen + 1, upperType.length() - 1));
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "CHAR":
            case "CHARACTER":
                return ScalarType.createCharType(fieldSchema.requiredColumnSize());
            case "VARCHAR":
                return ScalarType.createVarcharType(fieldSchema.requiredColumnSize());
            case "STRING":
            case "TEXT":
            case "JSON":
            case "JSONB":
                return ScalarType.createStringType();
            case "HLL":
                return ScalarType.createHllType();
            case "BITMAP":
                return Type.BITMAP;
            default:
                return Type.UNSUPPORTED;
        }
    }
}
