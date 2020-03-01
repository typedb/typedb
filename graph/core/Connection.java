/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.core;

import grakn.core.graph.graphdb.types.TypeDefinitionDescription;
import grakn.core.graph.graphdb.types.system.BaseKey;

/**
 * Connection contains schema constraints from outgoing vertex to incoming vertex through an edge.
 *
 */
public class Connection {
    private final VertexLabel incomingVertexLabel;
    private final VertexLabel outgoingVertexLabel;
    private final String edgeLabel;
    private final JanusGraphEdge connectionEdge;

    public Connection(JanusGraphEdge connectionEdge) {
        this.outgoingVertexLabel = (VertexLabel) connectionEdge.outVertex();
        this.incomingVertexLabel = (VertexLabel) connectionEdge.inVertex();
        TypeDefinitionDescription value = connectionEdge.valueOrNull(BaseKey.SchemaDefinitionDesc);
        this.edgeLabel = (String) value.getModifier();
        this.connectionEdge = connectionEdge;
    }


    public Connection(String edgeLabel, VertexLabel outgoingVertexLabel, VertexLabel incomingVertexLabel) {
        this.outgoingVertexLabel = outgoingVertexLabel;
        this.incomingVertexLabel = incomingVertexLabel;
        this.edgeLabel = edgeLabel;
        this.connectionEdge = null;
    }

    /**
     *
     * @return a label from an EdgeLabel.
     */
    public String getEdgeLabel() {
        return edgeLabel;
    }

    /**
     *
     * @return a outgoing VertexLabel.
     */
    public VertexLabel getOutgoingVertexLabel() {
        return outgoingVertexLabel;
    }

    /**
     *
     * @return a incoming VertexLabel.
     */
    public VertexLabel getIncomingVertexLabel() {
        return incomingVertexLabel;
    }


    /**
     *
     * @return a internal connection edge is used for update.
     */
    public JanusGraphEdge getConnectionEdge() {
        return connectionEdge;
    }
}
