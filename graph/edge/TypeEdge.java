/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.graph.edge;

import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

/**
 * An edge between two {@code TypeVertex}.
 *
 * This edge can only have a encoding of type {@code Encoding.Edge.Type}.
 */
public interface TypeEdge extends Edge<Encoding.Edge.Type, EdgeIID.Type, TypeVertex> {

    @Override
    TypeVertex from();

    @Override
    TypeVertex to();

    /**
     * Returns the type vertex overridden by the head of this type edge.
     *
     * @return the type vertex overridden by the head of this type edge
     */
    TypeVertex overridden();

    /**
     * Sets the head vertex of this type edge to be overridden by a given type vertex.
     *
     * @param overridden the type vertex to override by the head vertex
     */
    void overridden(TypeVertex overridden);
}
