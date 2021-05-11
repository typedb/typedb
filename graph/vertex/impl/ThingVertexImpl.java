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

package com.vaticle.typedb.core.graph.vertex.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.adjacency.impl.ThingAdjacencyImpl;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_VERTEX_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ATTRIBUTE;

public abstract class ThingVertexImpl extends VertexImpl<VertexIID.Thing> implements ThingVertex {

    protected final ThingGraph graph;
    protected final GraphManager graphMgr;
    protected final ThingAdjacency outs;
    protected final ThingAdjacency ins;
    protected final AtomicBoolean isDeleted;
    protected boolean isInferred;

    ThingVertexImpl(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
        super(iid);
        this.graph = graph;
        this.graphMgr = new GraphManager(graph.type(), graph);
        this.outs = newAdjacency(Encoding.Direction.Adjacency.OUT);
        this.ins = newAdjacency(Encoding.Direction.Adjacency.IN);
        this.isInferred = isInferred;
        this.isModified = false;
        this.isDeleted = new AtomicBoolean(false);
    }

    public static ThingVertexImpl of(ThingGraph graph, VertexIID.Thing iid) {
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
    public ThingGraph graph() {
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
    public TypeVertex type() {
        return graph.type().convert(iid.type());
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
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(AttributeVertex.class));
    }

    void deleteEdges() {
        outs.deleteAll();
        ins.deleteAll();
    }

    void deleteVertexFromGraph() {
        graph.delete(this);
    }

    void deleteVertexFromStorage() {
        graph.storage().deleteTracked(iid.bytes());
        graph.storage().deleteUntracked(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
    }

    void commitEdges() {
        outs.commit();
        ins.commit();
    }

    @Override
    public int compareTo(ThingVertex o) {
        return iid().compareTo(o.iid());
    }

    public static class Buffered extends ThingVertexImpl {

        public Buffered(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
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
            if (isInferred) throw TypeDBException.of(ILLEGAL_OPERATION);
            commitVertex();
            commitEdges();
        }

        private void commitVertex() {
            graph.storage().putTracked(iid.bytes());
            graph.storage().putUntracked(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
        }

        @Override
        public void delete() {
            if (isDeleted.compareAndSet(false, true)) {
                deleteEdges();
                deleteVertexFromGraph();
            }
        }

        @Override
        public void setModified() {
            if (!isModified) isModified = true;
        }
    }

    public static class Persisted extends ThingVertexImpl {

        public Persisted(ThingGraph graph, VertexIID.Thing iid) {
            super(graph, iid, false);
        }

        @Override
        protected ThingAdjacency newAdjacency(Encoding.Direction.Adjacency direction) {
            return new ThingAdjacencyImpl.Persisted(this, direction);
        }

        @Override
        public void isInferred(boolean isInferred) {
            throw TypeDBException.of(ILLEGAL_OPERATION);
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

        @Override
        public void setModified() {
            if (!isModified) {
                isModified = true;
                graph.setModified(iid);
            }
        }
    }
}
