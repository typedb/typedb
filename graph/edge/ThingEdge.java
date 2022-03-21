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

import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.EdgeViewIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;

import java.util.Optional;

/**
 * An directed edge between two {@code ThingVertex}.
 */
public interface ThingEdge extends Edge<Encoding.Edge.Thing, ThingVertex> {

    ThingVertex from();

    VertexIID.Thing fromIID();

    ThingVertex to();

    VertexIID.Thing toIID();

    Optional<ThingVertex> optimised();

    boolean isInferred();

    View.Forward forwardView();

    View.Backward backwardView();

    interface View<T extends View<T>> extends Comparable<T> {

        EdgeViewIID.Thing iid();

        ThingEdge edge();

        interface Forward extends View<Forward> {

            @Override
            int compareTo(Forward other);
        }

        interface Backward extends View<Backward> {

            @Override
            int compareTo(Backward other);
        }
    }
}
