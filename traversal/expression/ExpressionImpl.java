/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.traversal.expression;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.vertex.Value;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.util.Double.equalsApproximate;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.EVALUATION_ERROR_DIVISION_BY_ZERO;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.ARGUMENT_COUNT_MISMATCH;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.ILLEGAL_CONVERSION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.ILLEGAL_FUNCTION_ARGUMENT_TYPE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Function.ABS;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Function.CEIL;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Function.FLOOR;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Function.MAX;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Function.MIN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Function.ROUND;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Operation.ADD;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Operation.DIVIDE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Operation.MODULO;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Operation.MULTIPLY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Operation.POWER;
import static com.vaticle.typeql.lang.common.TypeQLToken.Expression.Operation.SUBTRACT;

abstract class ExpressionImpl {

    abstract static class Var<VALUE> implements Expression<VALUE> {
        final Identifier.Variable id;

        private Var(Identifier.Variable id) {
            this.id = id;
        }

        @Override
        public java.lang.String toString() {
            return id.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Var<?> var = (Var<?>) o;
            return id.equals(var.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        static class Boolean extends Var<java.lang.Boolean> implements Expression.Boolean {
            Boolean(Identifier.Variable id) {
                super(id);
            }

            @Override
            public java.lang.Boolean evaluate(Map<Identifier, Value<?>> varValues) {
                return varValues.get(id).asBoolean().value();
            }
        }

        static class Long extends Var<java.lang.Long> implements Expression.Long {
            Long(Identifier.Variable id) {
                super(id);
            }

            @Override
            public java.lang.Long evaluate(Map<Identifier, Value<?>> varValues) {
                return varValues.get(id).asLong().value();
            }
        }

        static class Double extends Var<java.lang.Double> implements Expression.Double {
            Double(Identifier.Variable id) {
                super(id);
            }

            @Override
            public java.lang.Double evaluate(Map<Identifier, Value<?>> varValues) {
                return varValues.get(id).asDouble().value();
            }
        }

        static class String extends Var<java.lang.String> implements Expression.String {
            String(Identifier.Variable id) {
                super(id);
            }

            @Override
            public java.lang.String evaluate(Map<Identifier, Value<?>> varValues) {
                return varValues.get(id).asString().value();
            }
        }

        static class DateTime extends Var<LocalDateTime> implements Expression.DateTime {
            DateTime(Identifier.Variable id) {
                super(id);
            }

            @Override
            public LocalDateTime evaluate(Map<Identifier, Value<?>> varValues) {
                return varValues.get(id).asDateTime().value();
            }
        }
    }

    abstract static class Constant<VALUE> implements Expression<VALUE> {
        private final VALUE value;

        Constant(VALUE value) {
            this.value = value;
        }

        @Override
        public VALUE evaluate(Map<Identifier, Value<?>> varValues) {
            return value;
        }

        @Override
        public java.lang.String toString() {
            return value.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Constant<?> constant = (Constant<?>) o;
            return value.equals(constant.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        static class Boolean extends Constant<java.lang.Boolean> implements Expression.Boolean {
            Boolean(java.lang.Boolean value) {
                super(value);
            }
        }

        static class Long extends Constant<java.lang.Long> implements Expression.Long {
            Long(java.lang.Long value) {
                super(value);
            }
        }

        static class Double extends Constant<java.lang.Double> implements Expression.Double {
            Double(java.lang.Double value) {
                super(value);
            }
        }

        static class String extends Constant<java.lang.String> implements Expression.String {
            String(java.lang.String value) {
                super(value);
            }
        }

        static class DateTime extends Constant<LocalDateTime> implements Expression.DateTime {
            DateTime(LocalDateTime value) {
                super(value);
            }
        }
    }

    abstract static class Function<ARGS_VALUE, VALUE> implements Expression<VALUE> {

        private final java.lang.String functionName;
        private final boolean isInfix;
        private final BiFunction<List<Expression<ARGS_VALUE>>, Map<Identifier, Value<?>>, VALUE> evaluator;
        final List<Expression<ARGS_VALUE>> args;

        private Function(java.lang.String functionName, boolean isInfix,
                         BiFunction<List<Expression<ARGS_VALUE>>, Map<Identifier, Value<?>>, VALUE> evaluator,
                         List<Expression<ARGS_VALUE>> args) {
            this.functionName = functionName;
            this.isInfix = isInfix;
            this.evaluator = evaluator;
            this.args = args;
        }

        @Override
        public VALUE evaluate(Map<Identifier, Value<?>> varValues) {
            return evaluator.apply(args, varValues);
        }

        @Override
        public java.lang.String toString() {
            if (isInfix) return args.stream().map(Object::toString).collect(Collectors.joining(functionName));
            else {
                return functionName + "(" + args.stream().map(Expression::toString).collect(Collectors.joining(", ")) + ")";
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Function<?, ?> other = (Function<?, ?>) o;
            return functionName.equals(other.functionName) && args.equals(other.args);
        }

        @Override
        public int hashCode() {
            return Objects.hash(functionName, args);
        }

        static class Boolean<T> extends Function<T, java.lang.Boolean> implements Expression.Boolean {
            private static final java.lang.String NAME = "bool"; // internal function name

            private Boolean(java.lang.String functionName, boolean isInfix,
                            BiFunction<List<Expression<T>>, Map<Identifier, Value<?>>, java.lang.Boolean> evaluator,
                            List<Expression<T>> args) {
                super(functionName, isInfix, evaluator, args);
            }

            static Expression<java.lang.Boolean> of(Expression<?> arg) {
                if (arg.isBoolean()) return arg.asBoolean();
                else throw TypeDBException.of(ILLEGAL_CONVERSION, NAME, arg);
            }
        }

        static class Long<T> extends Function<T, java.lang.Long> implements Expression.Long {
            private static final java.lang.String NAME = "long"; // internal function name

            private Long(java.lang.String functionName, boolean isInfix,
                         BiFunction<List<Expression<T>>, Map<Identifier, Value<?>>, java.lang.Long> evaluator,
                         List<Expression<T>> args) {
                super(functionName, isInfix, evaluator, args);
            }

            static Expression<java.lang.Long> of(Expression<?> arg) {
                if (arg.isLong()) return arg.asLong();
                else throw TypeDBException.of(ILLEGAL_CONVERSION, NAME, arg);
            }
        }

        static class Double<T> extends Function<T, java.lang.Double> implements Expression.Double {
            private static final java.lang.String NAME = "double"; // internal function name

            private Double(java.lang.String functionName, boolean isInfix,
                           BiFunction<List<Expression<T>>, Map<Identifier, Value<?>>, java.lang.Double> evaluator,
                           List<Expression<T>> args) {
                super(functionName, isInfix, evaluator, args);
            }

            static Expression<java.lang.Double> of(Expression<?> arg) {
                if (arg.isDouble()) return arg.asDouble();
                else if (arg.isLong()) {
                    return new Function.Double<>(
                            NAME, false,
                            (args, varVals) -> args.get(0).asLong().evaluate(varVals).doubleValue(),
                            list(arg)
                    );
                } else throw TypeDBException.of(ILLEGAL_CONVERSION, NAME, arg);
            }
        }

        static class String<T> extends Function<T, java.lang.String> implements Expression.String {
            private static final java.lang.String NAME = "string"; // internal function name

            private String(java.lang.String functionName, boolean isInfix,
                           BiFunction<List<Expression<T>>, Map<Identifier, Value<?>>, java.lang.String> evaluator,
                           List<Expression<T>> args) {
                super(functionName, isInfix, evaluator, args);
            }

            static Expression<java.lang.String> of(Expression<?> arg) {
                if (arg.isString()) return arg.asString();
                else throw TypeDBException.of(ILLEGAL_CONVERSION, NAME, arg);
            }
        }

        static class DateTime<T> extends Function<T, LocalDateTime> implements Expression.DateTime {
            private static final java.lang.String NAME = "datetime"; // internal function name

            private DateTime(java.lang.String functionName, boolean isInfix,
                             BiFunction<List<Expression<T>>, Map<Identifier, Value<?>>, LocalDateTime> evaluator,
                             List<Expression<T>> args) {
                super(functionName, isInfix, evaluator, args);
            }

            static Expression<LocalDateTime> of(Expression<?> arg) {
                if (arg.isDateTime()) return arg.asDateTime();
                else throw TypeDBException.of(ILLEGAL_CONVERSION, NAME, arg);
            }
        }

        static abstract class Maximum {
            static <T> Expression<?> of(List<Expression<T>> arguments) {
                assert !arguments.isEmpty();
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Long<>(MAX.toString(), false, Maximum::maxLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(MAX.toString(), false, Maximum::maxDouble, argsAsDouble);
                } else throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, MAX, arguments.get(0).returnType());
            }

            private static java.lang.Long maxLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() != 0;
                return iterate(args).map(arg -> arg.evaluate(varValues)).stream().max(java.lang.Long::compare).get();
            }

            private static java.lang.Double maxDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() != 0;
                return iterate(args).map(arg -> arg.evaluate(varValues)).stream().max(java.lang.Double::compare).get();
            }
        }

        static class Minimum {
            static <T> Expression<?> of(List<Expression<T>> arguments) {
                assert !arguments.isEmpty();
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Long<>(MIN.toString(), false, Minimum::minLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(MIN.toString(), false, Minimum::minDouble, argsAsDouble);
                } else throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, MIN, arguments.get(0).returnType());
            }

            private static java.lang.Long minLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() != 0;
                return iterate(args).map(arg -> arg.evaluate(varValues)).stream().min(java.lang.Long::compare).get();
            }

            private static java.lang.Double minDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() != 0;
                return iterate(args).map(arg -> arg.evaluate(varValues)).stream().min(java.lang.Double::compare).get();
            }
        }

        static class Floor {
            private static final int ARGS_COUNT = 1;

            static <T> Expression<java.lang.Long> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, FLOOR,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                Expression<T> expression = arguments.get(0);
                if (expression.isLong()) return expression.asLong();
                else if (expression.isDouble()) {
                    return new ExpressionImpl.Function.Long<>(FLOOR.toString(), false, Floor::floorDouble, list(expression.asDouble()));
                } else throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, FLOOR, expression.returnType());
            }

            private static java.lang.Long floorDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return Math.round(Math.floor(args.get(0).evaluate(varValues)));
            }
        }

        static class Ceiling {
            private static final int ARGS_COUNT = 1;

            static <T> Expression<java.lang.Long> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, CEIL,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                Expression<T> expression = arguments.get(0);
                if (expression.isLong()) return expression.asLong();
                else if (expression.isDouble()) {
                    return new ExpressionImpl.Function.Long<>(CEIL.toString(), false, Ceiling::ceilDouble, list(expression.asDouble()));
                } else throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, CEIL, expression.returnType());
            }

            private static java.lang.Long ceilDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return Math.round(Math.ceil(args.get(0).evaluate(varValues)));
            }
        }

        static class Round {
            private static final int ARGS_COUNT = 1;

            static <T> Expression<java.lang.Long> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, ROUND,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                Expression<T> expression = arguments.get(0);
                if (expression.isLong()) return expression.asLong();
                else if (expression.isDouble()) {
                    return new ExpressionImpl.Function.Long<>(ROUND.toString(), false, Round::roundDouble, list(expression.asDouble()));
                } else throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, ROUND, expression.returnType());
            }

            private static java.lang.Long roundDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return Math.round(args.get(0).evaluate(varValues));
            }
        }

        static class AbsoluteValue {
            private static final int ARGS_COUNT = 1;

            static <T> Expression<?> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, ABS,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                Expression<T> expression = arguments.get(0);
                if (expression.isLong()) {
                    return new ExpressionImpl.Function.Long<>(ABS.toString(), false, AbsoluteValue::absLong, list(expression.asLong()));
                } else if (expression.isDouble()) {
                    return new ExpressionImpl.Function.Double<>(ABS.toString(), false, AbsoluteValue::absDouble, list(expression.asDouble()));
                } else throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, ABS, expression.returnType());
            }

            private static java.lang.Long absLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return Math.abs(args.get(0).evaluate(varValues));
            }

            private static java.lang.Double absDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return Math.abs(args.get(0).evaluate(varValues));
            }
        }

        static class Add {
            static <T> Expression<?> of(List<Expression<T>> arguments) {
                assert !arguments.isEmpty();
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Long<>(ADD.toString(), true, Add::addLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(ADD.toString(), true, Add::addDouble, argsAsDouble);
                } else throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, ADD, arguments.get(0).returnType());
            }

            private static java.lang.Long addLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert !args.isEmpty();
                return args.stream().map(arg -> arg.evaluate(varValues)).reduce(0L, java.lang.Long::sum);
            }

            private static java.lang.Double addDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert !args.isEmpty();
                return args.stream().map(arg -> arg.evaluate(varValues)).reduce(0.0, java.lang.Double::sum);
            }
        }

        static class Subtract {
            private static final int ARGS_COUNT = 2;

            static <T> Expression<?> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, SUBTRACT,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Long<>(SUBTRACT.toString(), true, Subtract::subLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(SUBTRACT.toString(), true, Subtract::subDouble, argsAsDouble);
                } else
                    throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, SUBTRACT, arguments.get(0).returnType());
            }

            private static java.lang.Long subLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return args.get(0).evaluate(varValues) - args.get(1).evaluate(varValues);
            }

            private static java.lang.Double subDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return args.get(0).evaluate(varValues) - args.get(1).evaluate(varValues);
            }
        }

        static class Multiply {
            static <T> Expression<?> of(List<Expression<T>> arguments) {
                assert !arguments.isEmpty();
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Long<>(MULTIPLY.toString(), true, Multiply::mulLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(MULTIPLY.toString(), true, Multiply::mulDouble, argsAsDouble);
                } else
                    throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, MULTIPLY, arguments.get(0).returnType());
            }

            private static java.lang.Long mulLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert !args.isEmpty();
                return args.stream().map(arg -> arg.evaluate(varValues)).reduce(1L, (a, b) -> a * b);
            }

            private static java.lang.Double mulDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert !args.isEmpty();
                return args.stream().map(arg -> arg.evaluate(varValues)).reduce(1.0, (a, b) -> a * b);
            }
        }

        static class Divide {
            private static final int ARGS_COUNT = 2;

            static <T> Expression<?> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, DIVIDE,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Double<>(DIVIDE.toString(), true, Divide::divLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(DIVIDE.toString(), true, Divide::divDouble, argsAsDouble);
                } else
                    throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, DIVIDE, arguments.get(0).returnType());
            }

            private static java.lang.Double divLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                java.lang.Long divisor = args.get(1).evaluate(varValues);
                if (divisor == 0) {
                    throw TypeDBException.of(EVALUATION_ERROR_DIVISION_BY_ZERO, args.get(0), args.get(1));
                }
                return (double) args.get(0).evaluate(varValues) / divisor;
            }

            private static java.lang.Double divDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                java.lang.Double divisor = args.get(1).evaluate(varValues);
                if (equalsApproximate(0.0, divisor)) {
                    throw TypeDBException.of(EVALUATION_ERROR_DIVISION_BY_ZERO, args.get(0), args.get(1));
                }
                return args.get(0).evaluate(varValues) / divisor;
            }
        }

        static class Modulo {
            private static final int ARGS_COUNT = 2;

            static <T> Expression<?> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, MODULO,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Long<>(MODULO.toString(), true, Modulo::modLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(MODULO.toString(), true, Modulo::modDouble, argsAsDouble);
                } else
                    throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, MODULO, arguments.get(0).returnType());
            }

            private static java.lang.Long modLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return args.get(0).evaluate(varValues) % args.get(1).evaluate(varValues);
            }

            private static java.lang.Double modDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return args.get(0).evaluate(varValues) % args.get(1).evaluate(varValues);
            }
        }

        static class Power {
            private static final int ARGS_COUNT = 2;

            static <T> Expression<?> of(List<Expression<T>> arguments) {
                if (arguments.size() != ARGS_COUNT) {
                    throw TypeDBException.of(
                            ARGUMENT_COUNT_MISMATCH, POWER,
                            ARGS_COUNT, arguments.size(),
                            arguments.stream().map(Object::toString).collect(Collectors.joining(", "))
                    );
                }
                // TODO: can Java's type system cast without inducing a runtime copy?
                if (arguments.get(0).isLong()) {
                    List<Expression<java.lang.Long>> argsAsLong = iterate(arguments).map(Expression::asLong).toList();
                    return new ExpressionImpl.Function.Long<>(POWER.toString(), true, Power::powLong, argsAsLong);
                } else if (arguments.get(0).isDouble()) {
                    List<Expression<java.lang.Double>> argsAsDouble = iterate(arguments).map(Expression::asDouble).toList();
                    return new ExpressionImpl.Function.Double<>(POWER.toString(), true, Power::powDouble, argsAsDouble);
                } else
                    throw TypeDBException.of(ILLEGAL_FUNCTION_ARGUMENT_TYPE, POWER, arguments.get(0).returnType());
            }

            private static java.lang.Long powLong(List<Expression<java.lang.Long>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return (long) Math.pow(args.get(0).evaluate(varValues), args.get(1).evaluate(varValues));
            }

            private static java.lang.Double powDouble(List<Expression<java.lang.Double>> args, Map<Identifier, Value<?>> varValues) {
                assert args.size() == ARGS_COUNT;
                return Math.pow(args.get(0).evaluate(varValues), args.get(1).evaluate(varValues));
            }
        }
    }
}
