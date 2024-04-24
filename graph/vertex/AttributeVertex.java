/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;

import java.time.LocalDateTime;

public interface AttributeVertex<VALUE> extends ThingVertex, Value<VALUE> {

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

    boolean isValueSortable();

    ValueSortable<VALUE> asValueSortable();

    ValueSortable<VALUE> toValueSortable();

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
    interface ValueSortable<VALUE> extends AttributeVertex<VALUE> {

        AttributeVertex<VALUE> toAttribute();

        @Override
        int compareTo(Vertex<?, ?> other);
    }

}
