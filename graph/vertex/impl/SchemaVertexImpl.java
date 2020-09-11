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
 *
 */

package grakn.core.graph.vertex.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.adjacency.SchemaAdjacency;
import grakn.core.graph.edge.SchemaEdge;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.RuleVertex;
import grakn.core.graph.vertex.SchemaVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.core.common.exception.ErrorMessage.SchemaGraph.INVALID_SCHEMA_VERTEX_CASTING;


public abstract class SchemaVertexImpl<
        VERTEX_IID extends VertexIID.Schema,
        VERTEX_ENCODING extends Encoding.Vertex.Schema
        > extends VertexImpl<VERTEX_IID> implements SchemaVertex<VERTEX_IID, VERTEX_ENCODING> {

    protected final SchemaGraph graph;
    protected final SchemaAdjacency outs;
    protected final SchemaAdjacency ins;
    protected final AtomicBoolean isDeleted;

    protected String label;

    SchemaVertexImpl(SchemaGraph graph, VERTEX_IID iid, String label) {
        super(iid);
        this.graph = graph;
        this.label = label;
        this.outs = newAdjacency(Encoding.Direction.OUT);
        this.ins = newAdjacency(Encoding.Direction.IN);
        this.isDeleted = new AtomicBoolean(false);
    }

    public TypeVertex asType() {
        throw new GraknException(INVALID_SCHEMA_VERTEX_CASTING.message(TypeVertex.class.getCanonicalName()));
    }

    public RuleVertex asRule() {
        throw new GraknException(INVALID_SCHEMA_VERTEX_CASTING.message(RuleVertex.class.getCanonicalName()));
    }

    /**
     * Instantiates a new {@code TypeAdjacency} class
     *
     * @param direction the direction of the edges held in {@code TypeAdjacency}
     * @return the new {@code TypeAdjacency} class
     */
    protected abstract SchemaAdjacency newAdjacency(Encoding.Direction direction);

    @Override
    public SchemaGraph graph() {
        return graph;
    }

    @Override
    public SchemaAdjacency outs() {
        return outs;
    }

    @Override
    public SchemaAdjacency ins() {
        return ins;
    }

    @Override
    public void setModified() {
        if (!isModified) {
            isModified = true;
            graph.setModified();
        }
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public boolean isDeleted() {
        return isDeleted.get();
    }

    void deleteEdges() {
        ins.deleteAll();
        outs.deleteAll();
    }

    void commitEdges() {
        outs.forEach(SchemaEdge::commit);
        ins.forEach(SchemaEdge::commit);
    }

}
