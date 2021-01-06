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
import grakn.core.graph.DataGraph;
import grakn.core.graph.GraphManager;
import grakn.core.graph.adjacency.ThingAdjacency;
import grakn.core.graph.adjacency.impl.ThingAdjacencyImpl;
import grakn.core.graph.iid.EdgeIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.util.concurrent.atomic.AtomicBoolean;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_VERTEX_CASTING;
import static grakn.core.common.exception.ErrorMessage.Transaction.ILLEGAL_OPERATION;
import static grakn.core.graph.util.Encoding.Vertex.Thing.ATTRIBUTE;

public abstract class ThingVertexImpl extends VertexImpl<VertexIID.Thing> implements ThingVertex {

    protected final DataGraph graph;
    protected final GraphManager graphMgr;
    protected final ThingAdjacency outs;
    protected final ThingAdjacency ins;
    protected final AtomicBoolean isDeleted;
    protected boolean isInferred;

    ThingVertexImpl(DataGraph graph, VertexIID.Thing iid, boolean isInferred) {
        super(iid);
        this.graph = graph;
        this.graphMgr = new GraphManager(graph.schema(), graph);
        this.outs = newAdjacency(Encoding.Direction.Adjacency.OUT);
        this.ins = newAdjacency(Encoding.Direction.Adjacency.IN);
        this.isInferred = isInferred;
        this.isModified = false;
        this.isDeleted = new AtomicBoolean(false);
    }

    public static ThingVertexImpl of(DataGraph graph, VertexIID.Thing iid) {
        if (iid.encoding().equals(ATTRIBUTE)) {
            return AttributeVertexImpl.of(graph, iid.asAttribute());
        } else {
            return new ThingVertexImpl.Persisted(graph, iid);
        }
    }

    /**
     * Instantiates a new {@code ThingAdjacency} class
     *
     * @param direction the direction of the edges held in {@code ThingAdjacency}
     * @return the new {@code ThingAdjacency} class
     */
    protected abstract ThingAdjacency newAdjacency(Encoding.Direction.Adjacency direction);

    @Override
    public DataGraph graph() {
        return graph;
    }

    @Override
    public GraphManager graphs() {
        return graphMgr;
    }

    @Override
    public Encoding.Vertex.Thing encoding() {
        return iid.encoding();
    }

    @Override
    public ThingAdjacency outs() {
        return outs;
    }

    @Override
    public ThingAdjacency ins() {
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
    public TypeVertex type() {
        return graph.schema().convert(iid.type());
    }

    @Override
    public void isInferred(boolean isInferred) {
        this.isInferred = isInferred;
    }

    @Override
    public boolean isInferred() {
        return isInferred;
    }

    public boolean isDeleted() {
        return isDeleted.get();
    }

    @Override
    public boolean isThing() { return true; }

    @Override
    public boolean isAttribute() { return false; }

    @Override
    public ThingVertex asThing() { return this; }

    @Override
    public AttributeVertexImpl<?> asAttribute() {
        throw GraknException.of(INVALID_THING_VERTEX_CASTING, className(AttributeVertex.class));
    }

    void deleteEdges() {
        outs.deleteAll();
        ins.deleteAll();
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    void deleteVertexFromStorage() {
        graph.storage().delete(iid.bytes());
        graph.storage().delete(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
    }

    void commitEdges() {
        outs.commit();
        ins.commit();
    }

    public static class Buffered extends ThingVertexImpl {

        public Buffered(DataGraph graph, VertexIID.Thing iid, boolean isInferred) {
            super(graph, iid, isInferred);
            setModified();
        }

        @Override
        protected ThingAdjacency newAdjacency(Encoding.Direction.Adjacency direction) {
            return new ThingAdjacencyImpl.Buffered(this, direction);
        }

        @Override
        public Encoding.Status status() {
            return Encoding.Status.BUFFERED;
        }

        @Override
        public void commit() {
            if (isInferred) throw GraknException.of(ILLEGAL_OPERATION);
            commitVertex();
            commitEdges();
        }

        private void commitVertex() {
            graph.storage().put(iid.bytes());
            graph.storage().put(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromGraph();
            }
        }
    }

    public static class Persisted extends ThingVertexImpl {

        public Persisted(DataGraph graph, VertexIID.Thing iid) {
            super(graph, iid, false);
        }

        @Override
        protected ThingAdjacency newAdjacency(Encoding.Direction.Adjacency direction) {
            return new ThingAdjacencyImpl.Persisted(this, direction);
        }

        @Override
        public void isInferred(boolean isInferred) {
            throw GraknException.of(ILLEGAL_OPERATION);
        }

        @Override
        public Encoding.Status status() {
            return Encoding.Status.PERSISTED;
        }

        @Override
        public void commit() {
            commitEdges();
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromStorage();
                deleteVertexFromGraph();
            }
        }
    }
}
