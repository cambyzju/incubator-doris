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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'random'. This class is generated by GenerateFunction.
 */
public class Random extends ScalarFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(),
            FunctionSignature.ret(DoubleType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(BigIntType.INSTANCE, BigIntType.INSTANCE)
    );

    /**
     * constructor with 0 argument.
     */
    public Random() {
        super("random");
    }

    /**
     * constructor with 1 argument.
     */
    public Random(Expression arg) {
        super("random", arg);
        // align with original planner behavior, refer to: org/apache/doris/analysis/Expr.getBuiltinFunction()
        Preconditions.checkState(arg instanceof Literal, "The param of rand function must be literal");
    }

    /**
     * constructor with 2 argument.
     */
    public Random(Expression lchild, Expression rchild) {
        super("random", lchild, rchild);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        // align with original planner behavior, refer to:
        // org/apache/doris/analysis/Expr.getBuiltinFunction()
        for (Expression child : children()) {
            if (!child.isLiteral()) {
                throw new AnalysisException("The param of rand function must be literal ");
            }
        }
    }

    /**
     * custom compute nullable.
     */
    @Override
    public boolean nullable() {
        if (arity() > 0) {
            return children().stream().anyMatch(Expression::nullable);
        } else {
            return false;
        }
    }

    /**
     * withChildren.
     */
    @Override
    public Random withChildren(List<Expression> children) {
        if (children.isEmpty()) {
            return new Random();
        } else if (children.size() == 1) {
            return new Random(children.get(0));
        } else if (children.size() == 2) {
            return new Random(children.get(0), children.get(1));
        }
        throw new AnalysisException("random function only accept 0-2 arguments");
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitRandom(this, context);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
