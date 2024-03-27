/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.edge;

import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.EdgeViewIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import java.util.Optional;
import java.util.Set;

/**
 * An edge between two {@code TypeVertex}.
 *
 * This edge can only have a encoding of type {@code Encoding.Edge.Type}.
 */
public interface TypeEdge extends Edge<Encoding.Edge.Type, TypeVertex> {

    @Override
    TypeVertex from();

    @Override
    TypeVertex to();

    /**
     * Returns the type vertex overridden by the head of this type edge.
     *
     * @return the type vertex overridden by the head of this type edge
     */
    Optional<TypeVertex> overridden();

    /**
     * Sets the head vertex of this type edge to be overridden by a given type vertex.
     *
     * @param overridden the type vertex to override by the head vertex
     */
    void setOverridden(TypeVertex overridden);

    void unsetOverridden();

    Set<Annotation> annotations();

    void setAnnotations(Set<Annotation> annotations);

    View.Forward forwardView();

    View.Backward backwardView();

    interface View<T extends TypeEdge.View<T>> extends Comparable<T> {

        EdgeViewIID.Type iid();

        TypeEdge edge();

        interface Forward extends TypeEdge.View<TypeEdge.View.Forward> {

            @Override
            int compareTo(Forward other);
        }

        interface Backward extends TypeEdge.View<TypeEdge.View.Backward> {

            @Override
            int compareTo(Backward other);
        }
    }
}
