/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.query.property;

import grakn.core.graql.query.Token;
import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.StatementAttribute;
import graql.util.StringUtil;

import java.time.LocalDateTime;
import java.util.stream.Stream;

/**
 * Represents the {@code value} property on an attribute.
 * This property can be queried or inserted.
 * This property matches only resources whose value matches the given a value predicate.
 */
public class ValueProperty<T> extends VarProperty {

    private final Operation<T> operation;

    public ValueProperty(Operation<T> operation) {
        if (operation == null) {
            throw new NullPointerException("Null operation");
        }
        this.operation = operation;
    }

    public Operation<T> operation() {
        return operation;
    }

    @Override
    public String keyword() {
        return Token.Property.VALUE.toString();
    }

    @Override
    public String property() {
        return operation().toString();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public String toString() {
        return property();
    }

    @Override
    public Stream<Statement> statements() {
        if (operation instanceof Operation.Comparison.Variable) {
            return Stream.of(((Operation.Comparison.Variable) operation).value());
        } else {
            return Stream.empty();
        }
    }

    @Override
    public Class statementClass() {
        return StatementAttribute.class;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ValueProperty) {
            ValueProperty that = (ValueProperty) o;
            return (this.operation.equals(that.operation));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.operation.hashCode();
        return h;
    }

    public abstract static class Operation<T> {

        private final Token.Comparator comparator;
        private final T value;

        Operation(Token.Comparator comparator, T value) {
            this.comparator = comparator;
            this.value = value;
        }

        public Token.Comparator comparator() {
            return comparator;
        }

        public T value() {
            return value;
        }

        public boolean isValueEquality() {
            return comparator.equals(Token.Comparator.EQV) && !(this instanceof Comparison.Variable);
        }

        @Override
        public String toString() {
            return comparator.toString() + Token.Char.SPACE + StringUtil.valueToString(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Operation that = (Operation) o;

            return (this.comparator().equals(that.comparator()) &&
                    this.value().equals(that.value()));
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^= this.comparator().hashCode();
            h *= 1000003;
            h ^= this.value().hashCode();
            return h;
        }

        public abstract static class Assignment<T> extends Operation<T> {

            Assignment(T value) {
                super(Token.Comparator.EQV, value);
            }

            public java.lang.String toString() {
                return StringUtil.valueToString(value());
            }

            public static class Number<N extends java.lang.Number> extends Assignment<N> {

                public Number(N value){
                    super(value);
                }
            }

            public static class Boolean extends Assignment<java.lang.Boolean> {

                public Boolean(boolean value) {
                    super(value);
                }
            }

            public static class String extends Assignment<java.lang.String> {

                public String(java.lang.String value) {
                    super(value);
                }
            }

            public static class DateTime extends Assignment<LocalDateTime> {

                public DateTime(LocalDateTime value) {
                    super(value);
                }
            }
        }

        public abstract static class Comparison<T> extends Operation<T> {

            Comparison(Token.Comparator comparator, T value) {
                super(comparator, value);
            }

            public static Comparison<?> of(Token.Comparator comparator, Object value) {
                if (value instanceof Integer) {
                    return new ValueProperty.Operation.Comparison.Number<>(comparator, (Integer) value);
                } else if (value instanceof Long) {
                    return new ValueProperty.Operation.Comparison.Number<>(comparator, (Long) value);
                } else if (value instanceof Float) {
                    return new ValueProperty.Operation.Comparison.Number<>(comparator, (Float) value);
                } else if (value instanceof Double) {
                    return new ValueProperty.Operation.Comparison.Number<>(comparator, (Double) value);
                } else if (value instanceof java.lang.Boolean) {
                    return new ValueProperty.Operation.Comparison.Boolean(comparator, (java.lang.Boolean) value);
                } else if (value instanceof java.lang.String) {
                    return new ValueProperty.Operation.Comparison.String(comparator, (java.lang.String) value);
                } else if (value instanceof LocalDateTime) {
                    return new ValueProperty.Operation.Comparison.DateTime(comparator, (LocalDateTime) value);
                } else {
                    throw new UnsupportedOperationException("Unsupported Value Comparison for class: " + value.getClass());
                }
            }

            public static class Number<N extends java.lang.Number> extends Comparison<N> {

                public Number(Token.Comparator comparator, N value) {
                    super(comparator, value);
                }
            }

            public static class Boolean extends Comparison<java.lang.Boolean> {

                public Boolean(Token.Comparator comparator, boolean value) {
                    super(comparator, value);
                }
            }

            public static class String extends Comparison<java.lang.String> {

                public String(Token.Comparator comparator, java.lang.String value) {
                    super(comparator, value);
                }

                @Override
                public java.lang.String toString() {
                    StringBuilder operation = new StringBuilder();

                    operation.append(comparator()).append(Token.Char.SPACE);
                    if (comparator().equals(Token.Comparator.LIKE)) {
                        operation.append("\"").append(value().replaceAll("/", "\\\\/")).append("\"");
                    } else {
                        operation.append(StringUtil.quoteString(value()));
                    }

                    return operation.toString();
                }
            }

            public static class DateTime extends Comparison<LocalDateTime> {

                public DateTime(Token.Comparator comparator, LocalDateTime value) {
                    super(comparator, value);
                }
            }

            public static class Variable extends Comparison<Statement> {

                public Variable(Token.Comparator comparator, Statement value) {
                    super(comparator, value);
                }

                public java.lang.String toString() {
                    return comparator().toString() + Token.Char.SPACE + value().getPrintableName();
                }
            }
        }
    }
}
