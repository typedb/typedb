/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.expression;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.time.LocalDateTime;
import java.util.List;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.FUNCTION_NOT_RECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.OPERATION_NOT_RECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ExpressionFactory {

    public static Expression<?> constant(com.vaticle.typeql.lang.pattern.expression.Expression.Constant<?> constant) {
        if (constant.isBoolean()) return new ExpressionImpl.Constant.Boolean(constant.asBoolean().value());
        else if (constant.isLong()) return new ExpressionImpl.Constant.Long(constant.asLong().value());
        else if (constant.isDouble()) return new ExpressionImpl.Constant.Double(constant.asDouble().value());
        else if (constant.isString()) return new ExpressionImpl.Constant.String(constant.asString().value());
        else if (constant.isDateTime()) return new ExpressionImpl.Constant.DateTime(constant.asDateTime().value());
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static Expression<?> var(Identifier.Variable id, Encoding.ValueType<?> valueType) {
        if (valueType == Encoding.ValueType.BOOLEAN) return new ExpressionImpl.Var.Boolean(id);
        else if (valueType == Encoding.ValueType.LONG) return new ExpressionImpl.Var.Long(id);
        else if (valueType == Encoding.ValueType.DOUBLE) return new ExpressionImpl.Var.Double(id);
        else if (valueType == Encoding.ValueType.STRING) return new ExpressionImpl.Var.String(id);
        else if (valueType == Encoding.ValueType.DATETIME) return new ExpressionImpl.Var.DateTime(id);
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static <T> Expression<?> operation(TypeQLToken.Expression.Operation operation, List<Expression<T>> arguments) {
        assert iterate(arguments).map(Expression::returnType).toSet().size() == 1;
        switch (operation) {
            case ADD:
                return ExpressionImpl.Function.Add.of(arguments);
            case SUBTRACT:
                return ExpressionImpl.Function.Subtract.of(arguments);
            case MULTIPLY:
                return ExpressionImpl.Function.Multiply.of(arguments);
            case DIVIDE:
                return ExpressionImpl.Function.Divide.of(arguments);
            case MODULO:
                return ExpressionImpl.Function.Modulo.of(arguments);
            case POWER:
                return ExpressionImpl.Function.Power.of(arguments);
            default:
                throw TypeDBException.of(OPERATION_NOT_RECOGNISED, operation);
        }
    }

    public static <T> Expression<?> function(TypeQLToken.Expression.Function function, List<Expression<T>> arguments) {
        assert iterate(arguments).map(Expression::returnType).toSet().size() == 1;
        switch (function) {
            case MAX:
                return ExpressionImpl.Function.Maximum.of(arguments);
            case MIN:
                return ExpressionImpl.Function.Minimum.of(arguments);
            case FLOOR:
                return ExpressionImpl.Function.Floor.of(arguments);
            case CEIL:
                return ExpressionImpl.Function.Ceiling.of(arguments);
            case ROUND:
                return ExpressionImpl.Function.Round.of(arguments);
            case ABS:
                return ExpressionImpl.Function.AbsoluteValue.of(arguments);
            default:
                throw TypeDBException.of(FUNCTION_NOT_RECOGNISED, function);
        }
    }

    public static Expression<java.lang.Long> convertToLong(Expression<?> expression) {
        return ExpressionImpl.Function.Long.of(expression);
    }

    public static Expression<java.lang.Double> convertToDouble(Expression<?> expression) {
        return ExpressionImpl.Function.Double.of(expression);
    }

    public static Expression<java.lang.Boolean> convertToBoolean(Expression<?> expression) {
        return ExpressionImpl.Function.Boolean.of(expression);
    }

    public static Expression<java.lang.String> convertToString(Expression<?> expression) {
        return ExpressionImpl.Function.String.of(expression);
    }

    public static Expression<LocalDateTime> convertToDateTime(Expression<?> expression) {
        return ExpressionImpl.Function.DateTime.of(expression);
    }
}
