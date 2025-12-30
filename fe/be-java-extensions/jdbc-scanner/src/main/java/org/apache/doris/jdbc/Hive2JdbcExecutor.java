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

package org.apache.doris.jdbc;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnType.Type;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorTable;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.stream.Collectors;

public class Hive2JdbcExecutor extends BaseJdbcExecutor {
    private static final Logger LOG = Logger.getLogger(Hive2JdbcExecutor.class);

    private static final Gson gson = new Gson();

    public Hive2JdbcExecutor(byte[] thriftParams) throws Exception {
        /* hive-jdbc driver do not support rollback, we should set autocommit=true to avoid hikari call rollback:
            java.sql.SQLFeatureNotSupportedException: Method not supported
            at shade.doris.hive-jdbc.org.apache.hive.jdbc.HiveConnection.rollback(HiveConnection.java:1340)
            at com.zaxxer.hikari.pool.ProxyConnection.close(ProxyConnection.java:250)
            at org.apache.doris.jdbc.BaseJdbcExecutor.closeResources(BaseJdbcExecutor.java:174)
            at org.apache.doris.jdbc.BaseJdbcExecutor.close(BaseJdbcExecutor.java:149)
        */
        super(thriftParams, true);
    }

    @Override
    public void openTrans() throws JdbcExecutorException {
        throw new JdbcExecutorException("jdbc:hive2 do not support transaction, please use hive catalog instead.");
    }

    @Override
    public void commitTrans() throws JdbcExecutorException {
        throw new JdbcExecutorException("jdbc:hive2 do not support transaction, please use hive catalog instead.");
    }

    @Override
    public void rollbackTrans() throws JdbcExecutorException {
        throw new JdbcExecutorException("jdbc:hive2 do not support transaction, please use hive catalog instead.");
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (outputTable.getColumnType(i).getType() == Type.ARRAY) {
                block.add(new Object[batchSizeNum]);
            } else {
                block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
            }
        }
    }

    @Override
    protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) throws SQLException {
        switch (type.getType()) {
            case BOOLEAN:
                boolean boolVal = resultSet.getBoolean(columnIndex + 1);
                return resultSet.wasNull() ? null : boolVal;
            case TINYINT:
                byte tinyIntVal = resultSet.getByte(columnIndex + 1);
                return resultSet.wasNull() ? null : tinyIntVal;
            case SMALLINT:
                short smallIntVal = resultSet.getShort(columnIndex + 1);
                return resultSet.wasNull() ? null : smallIntVal;
            case INT:
                int intVal = resultSet.getInt(columnIndex + 1);
                return resultSet.wasNull() ? null : intVal;
            case BIGINT:
                long bigIntVal = resultSet.getLong(columnIndex + 1);
                return resultSet.wasNull() ? null : bigIntVal;
            case FLOAT:
                float floatVal = resultSet.getFloat(columnIndex + 1);
                return resultSet.wasNull() ? null : floatVal;
            case DOUBLE:
                double doubleVal = resultSet.getDouble(columnIndex + 1);
                return resultSet.wasNull() ? null : doubleVal;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                BigDecimal decimalVal = resultSet.getBigDecimal(columnIndex + 1);
                return resultSet.wasNull() ? null : decimalVal;
            case DATE:
            case DATEV2:
                Date dateVal = resultSet.getDate(columnIndex + 1);
                return resultSet.wasNull() ? null : dateVal.toLocalDate();
            case DATETIME:
            case DATETIMEV2:
                Timestamp timestampVal = resultSet.getTimestamp(columnIndex + 1);
                return resultSet.wasNull() ? null : timestampVal.toLocalDateTime();
            case CHAR:
            case VARCHAR:
            case STRING:
            case ARRAY:
                String stringVal = resultSet.getString(columnIndex + 1);
                return resultSet.wasNull() ? null : stringVal;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        if (columnType.getType() == Type.ARRAY) {
            return createConverter(
                    (Object input) -> convertArray(input, columnType.getChildTypes().get(0)),
                    List.class);
        } else {
            return null;
        }
    }

    private java.lang.reflect.Type getListTypeForArray(ColumnType type) {
        switch (type.getType()) {
            case BOOLEAN:
                return new TypeToken<List<Boolean>>() {
                }.getType();
            case TINYINT:
                return new TypeToken<List<Byte>>() {
                }.getType();
            case SMALLINT:
                return new TypeToken<List<Short>>() {
                }.getType();
            case INT:
                return new TypeToken<List<Integer>>() {
                }.getType();
            case BIGINT:
                return new TypeToken<List<Long>>() {
                }.getType();
            case FLOAT:
                return new TypeToken<List<Float>>() {
                }.getType();
            case DOUBLE:
                return new TypeToken<List<Double>>() {
                }.getType();
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new TypeToken<List<BigDecimal>>() {
                }.getType();
            case DATE:
            case DATEV2:
                return new TypeToken<List<LocalDate>>() {
                }.getType();
            case DATETIME:
            case DATETIMEV2:
                return new TypeToken<List<LocalDateTime>>() {
                }.getType();
            case CHAR:
            case VARCHAR:
            case STRING:
                return new TypeToken<List<String>>() {
                }.getType();
            case ARRAY:
                java.lang.reflect.Type childType = getListTypeForArray(type.getChildTypes().get(0));
                TypeToken<?> token = TypeToken.getParameterized(List.class, childType);
                return token.getType();
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    private Object convertArray(Object input, ColumnType columnType) {
        if (input == null) {
            return null;
        }
        java.lang.reflect.Type listType = getListTypeForArray(columnType);
        if (columnType.getType() == Type.BOOLEAN) {
            List<?> list = gson.fromJson((String) input, List.class);
            return list.stream().map(item -> {
                if (item == null) {
                    return null;
                } else if (item instanceof Boolean) {
                    return item;
                } else if (item instanceof Number) {
                    return ((Number) item).intValue() != 0;
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to Boolean.");
                }
            }).collect(Collectors.toList());
        } else if (columnType.getType() == Type.DATE || columnType.getType() == Type.DATEV2) {
            List<?> list = gson.fromJson((String) input, List.class);
            return list.stream().map(item -> {
                if (item == null) {
                    return null;
                } else if (item instanceof String) {
                    return LocalDate.parse((String) item);
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to LocalDate.");
                }
            }).collect(Collectors.toList());
        } else if (columnType.getType() == Type.DATETIME || columnType.getType() == Type.DATETIMEV2) {
            List<?> list = gson.fromJson((String) input, List.class);
            return list.stream().map(item -> {
                if (item == null) {
                    return null;
                } else if (item instanceof String) {
                    // Hive timestamp type support up to 9 decimal places of precision
                    return LocalDateTime.parse(
                            (String) item,
                            new DateTimeFormatterBuilder()
                                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                                    .toFormatter());
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to LocalDateTime.");
                }
            }).collect(Collectors.toList());
        } else if (columnType.getType() == Type.ARRAY) {
            ColumnType childType = columnType.getChildTypes().get(0);
            List<?> rawList = gson.fromJson((String) input, List.class);
            return rawList.stream()
                    .map(element -> {
                        String elementJson = gson.toJson(element);
                        return convertArray(elementJson, childType);
                    })
                    .collect(Collectors.toList());
        } else {
            return gson.fromJson((String) input, listType);
        }
    }
}
