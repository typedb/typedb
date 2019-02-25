/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.kb.structure;

import grakn.core.server.kb.Schema;
import grakn.core.server.session.TransactionOLTP;
import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * Represent an Edge in a Transaction
 * Wraps a tinkerpop Edge constraining it to the Grakn Object Model.
 */
public class EdgeElement extends AbstractElement<Edge, Schema.EdgeProperty> {

    public EdgeElement(TransactionOLTP graknGraph, Edge e) {
        super(graknGraph, e, Schema.PREFIX_EDGE);
    }

    /**
     * Deletes the edge between two concepts and adds both those concepts for re-validation in case something goes wrong
     */
    public void delete() {
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

    public VertexElement source() {
        return tx().factory().buildVertexElement(element().outVertex());
    }

    public VertexElement target() {
        return tx().factory().buildVertexElement(element().inVertex());
    }
}
