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

import com.vaticle.typedb.core.common.collection.Bytes.ByteComparable;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import java.util.Optional;

/**
 * An edge between two {@code ThingVertex}.
 *
 * This edge can only have a encoding of type {@code Encoding.Edge.Thing}.
 */
public interface ThingEdge extends Edge<Encoding.Edge.Thing, EdgeIID.Thing, ThingVertex>, ByteComparable<ThingEdge> {

    ThingVertex from();

    VertexIID.Thing fromIID();

    ThingVertex to();

    VertexIID.Thing toIID();

    Optional<? extends ThingVertex> optimised();

    void isInferred(boolean isInferred);

    boolean isInferred();
}
