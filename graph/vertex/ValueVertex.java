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

import com.vaticle.typedb.core.common.exception.TypeDBException;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.VALUES_NOT_COMPARABLE;

public interface ValueVertex<VALUE> extends ThingVertex {

    AttributeVertex<VALUE> attributeVertex();

    @Override
    default int compareTo(Vertex<?, ?> o) {
        if (o instanceof ValueVertex) {
            ValueVertex<?> other = (ValueVertex<?>) o;
            if (attributeVertex().valueType().comparableTo(other.attributeVertex().valueType())) {
                return attributeVertex().valueType().
            } else {
                throw TypeDBException.of(VALUES_NOT_COMPARABLE, attributeVertex().value(), attributeVertex().valueType(),
                        other.attributeVertex().value(), other.attributeVertex().valueType());
            }
        } else return iid().compareTo(o.iid());

    }
}
