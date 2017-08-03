/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal.structure;

import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;

import javax.annotation.Nullable;

/**
 * <p>
 *     Represent an Edge in a Grakn Graph
 * </p>
 *
 * <p>
 *    Wraps a tinkerpop {@link Edge} constraining it to the Grakn Object Model.
 * </p>
 *
 * @author fppt
 */
public class EdgeElement extends AbstractElement<Edge, Schema.EdgeProperty> {

    public EdgeElement(AbstractGraknGraph graknGraph, Edge e){
        super(graknGraph, e, Schema.PREFIX_EDGE);
    }

    /**
     * Deletes the edge between two concepts and adds both those concepts for re-validation in case something goes wrong
     */
    public void delete(){
        element().remove();
    }

    @Override
    public int hashCode() {
        return element().hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        EdgeElement edge = (EdgeElement) object;

        return element().id().equals(edge.id());
    }

    /**
     *
     * @return The source of the edge.
     */
    @Nullable
    public VertexElement source(){
        return graph().factory().buildVertexElement(element().outVertex());
    }

    /**
     *
     * @return The target of the edge
     */
    @Nullable
    public VertexElement target(){
        return graph().factory().buildVertexElement(element().inVertex());
    }
}
