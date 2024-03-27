/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.edge;

import com.vaticle.typedb.core.common.parameters.Concept.Existence;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.EdgeViewIID;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
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

    Existence existence();

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
