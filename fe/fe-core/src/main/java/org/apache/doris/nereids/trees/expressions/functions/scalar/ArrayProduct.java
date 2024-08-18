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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecisionForArrayItemAgg;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_product'. This class is generated by GenerateFunction.
 */
public class ArrayProduct extends ScalarFunction implements ExplicitlyCastableSignature,
        ComputePrecisionForArrayItemAgg, UnaryExpression, AlwaysNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(DoubleType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(BooleanType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(TinyIntType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(SmallIntType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(IntegerType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(BigIntType.INSTANCE)),
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(LargeIntType.INSTANCE)),
            FunctionSignature.ret(DecimalV3Type.WILDCARD).args(ArrayType.of(DecimalV3Type.WILDCARD)),
            FunctionSignature.ret(DecimalV2Type.SYSTEM_DEFAULT).args(ArrayType.of(DecimalV2Type.SYSTEM_DEFAULT)),
            FunctionSignature.ret(DoubleType.INSTANCE).args(ArrayType.of(FloatType.INSTANCE))
    );

    /**
     * constructor with 1 argument.
     */
    public ArrayProduct(Expression arg) {
        super("array_product", arg);
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayProduct withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new ArrayProduct(children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayProduct(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
