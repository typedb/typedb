/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.expression;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.vertex.Value;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.time.LocalDateTime;
import java.util.Map;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Expression<T> {

    default boolean isBoolean() {
        return false;
    }

    default boolean isLong() {
        return false;
    }

    default boolean isDouble() {
        return false;
    }

    default boolean isString() {
        return false;
    }

    default boolean isDateTime() {
        return false;
    }

    default Expression<java.lang.Boolean> asBoolean() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Expression.Boolean.class));
    }

    default Expression<java.lang.Long> asLong() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Expression.Long.class));
    }

    default Expression<java.lang.Double> asDouble() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Expression.Double.class));
    }

    default Expression<java.lang.String> asString() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Expression.String.class));
    }

    default Expression<LocalDateTime> asDateTime() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Expression.DateTime.class));
    }

    Encoding.ValueType<T> returnType();

    T evaluate(Map<Identifier, Value<?>> varValues);

    interface Boolean extends Expression<java.lang.Boolean> {

        @Override
        default Encoding.ValueType<java.lang.Boolean> returnType() {
            return Encoding.ValueType.BOOLEAN;
        }

        @Override
        default boolean isBoolean() {
            return true;
        }

        @Override
        default Expression.Boolean asBoolean() {
            return this;
        }
    }

    interface Long extends Expression<java.lang.Long> {

        @Override
        default Encoding.ValueType<java.lang.Long> returnType() {
            return Encoding.ValueType.LONG;
        }

        @Override
        default boolean isLong() {
            return true;
        }

        @Override
        default Expression.Long asLong() {
            return this;
        }
    }

    interface Double extends Expression<java.lang.Double> {

        @Override
        default Encoding.ValueType<java.lang.Double> returnType() {
            return Encoding.ValueType.DOUBLE;
        }

        @Override
        default boolean isDouble() {
            return true;
        }

        @Override
        default Expression.Double asDouble() {
            return this;
        }
    }

    interface String extends Expression<java.lang.String> {

        @Override
        default Encoding.ValueType<java.lang.String> returnType() {
            return Encoding.ValueType.STRING;
        }

        @Override
        default boolean isString() {
            return true;
        }

        @Override
        default Expression.String asString() {
            return this;
        }
    }

    interface DateTime extends Expression<LocalDateTime> {

        @Override
        default Encoding.ValueType<java.time.LocalDateTime> returnType() {
            return Encoding.ValueType.DATETIME;
        }

        @Override
        default boolean isDateTime() {
            return true;
        }

        @Override
        default Expression.DateTime asDateTime() {
            return this;
        }
    }
}
