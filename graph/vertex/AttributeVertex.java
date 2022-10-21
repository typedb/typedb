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

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;

import java.time.LocalDateTime;

public interface AttributeVertex<VALUE> extends ThingVertex {

    /**
     * Returns the IID of this {@code AttributeVertex}.
     *
     * @return the IID of this {@code AttributeVertex}
     */
    VertexIID.Attribute<VALUE> iid();

    /**
     * Returns the {@code ValueType} of this {@code Attribute}
     *
     * @return the {@code ValueType} of this {@code Attribute}
     */
    Encoding.ValueType<VALUE> valueType();

    /**
     * Returns the literal value stored in the vertex, if it represents an attribute.
     *
     * @return the literal value stored in the vertex
     */
    VALUE value();

    AttributeVertex.Write<VALUE> toWrite();

    AttributeVertex.Write<VALUE> asWrite();

    boolean isValue();

    AttributeVertex.Value<VALUE> asValue();

    AttributeVertex.Value<VALUE> toValue();

    boolean isBoolean();

    boolean isLong();

    boolean isDouble();

    boolean isString();

    boolean isDateTime();

    AttributeVertex<Boolean> asBoolean();

    AttributeVertex<Long> asLong();

    AttributeVertex<Double> asDouble();

    AttributeVertex<String> asString();

    AttributeVertex<LocalDateTime> asDateTime();

    interface Write<VALUE> extends ThingVertex.Write, AttributeVertex<VALUE> {

        AttributeVertex.Write<VALUE> asWrite();

        AttributeVertex.Write<Boolean> asBoolean();

        AttributeVertex.Write<Long> asLong();

        AttributeVertex.Write<Double> asDouble();

        AttributeVertex.Write<String> asString();

        AttributeVertex.Write<LocalDateTime> asDateTime();

    }

    /**
     * An vertex wrapper that sorts based on its value, then type -- as opposed to type, then value.
     */
    interface Value<VALUE> extends AttributeVertex<VALUE> {

        AttributeVertex<VALUE> toAttribute();

        @Override
        int compareTo(Vertex<?, ?> other);
    }

}
