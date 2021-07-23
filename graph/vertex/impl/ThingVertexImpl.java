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

    ThingVertexImpl(ThingGraph graph, VertexIID.Thing iid) {
        super(iid);
        this.graph = graph;
        this.graphMgr = new GraphManager(graph.type(), graph);
    }

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
    public TypeVertex type() {
        return graph.type().convert(iid.type());
    }

    @Override
    public boolean isInferred() {
        return false;
    }

    @Override
    public boolean isThing() {
        return true;
    }

    @Override
    public boolean isAttribute() {
        return false;
    }

    @Override
    public ThingVertex asThing() {
        return this;
    }

    @Override
    public boolean isWrite() {
        return false;
    }

    @Override
    public ThingVertex.Write asWrite() {
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(ThingVertex.Write.class));
    }

    @Override
    public int compareTo(ThingVertex o) {
        return iid.bytes().compareTo(o.iid().bytes());
    }

    public static class Read extends ThingVertexImpl {

        protected final ThingAdjacency outs;
        protected final ThingAdjacency ins;

        public Read(ThingGraph graph, VertexIID.Thing iid) {
            super(graph, iid);
            this.outs = new ThingAdjacencyImpl.Read(this, Encoding.Direction.Adjacency.OUT);
            this.ins = new ThingAdjacencyImpl.Read(this, Encoding.Direction.Adjacency.IN);
        }

        public static ThingVertexImpl.Read of(ThingGraph graph, VertexIID.Thing iid) {
            if (iid.encoding().equals(ATTRIBUTE)) {
                return AttributeVertexImpl.Read.of(graph, iid.asAttribute());
            } else {
                return new ThingVertexImpl.Read(graph, iid);
            }
        }

        @Override
        public Encoding.Status status() {
            return Encoding.Status.PERSISTED;
        }

        @Override
        public ThingAdjacency ins() {
            return ins;
        }

        @Override
        public ThingVertex.Write toWrite() {
            return graph.convertToWritable(iid);
        }

        @Override
        public ThingAdjacency outs() {
            return outs;
        }

        @Override
        public boolean isModified() {
            return false;
        }

        @Override
        public AttributeVertex<?> asAttribute() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(AttributeVertex.class));
        }
    }

    public static abstract class Write extends ThingVertexImpl implements ThingVertex.Write {

        protected final ThingAdjacency.Write outs;
        protected final ThingAdjacency.Write ins;
        protected final AtomicBoolean isDeleted;
        protected boolean isModified;

        Write(ThingGraph graph, VertexIID.Thing iid) {
            super(graph, iid);
            this.isModified = false;
            this.isDeleted = new AtomicBoolean(false);
            this.outs = newAdjacency(Encoding.Direction.Adjacency.OUT);
            this.ins = newAdjacency(Encoding.Direction.Adjacency.IN);
        }

        public static ThingVertexImpl.Write of(ThingGraph graph, VertexIID.Thing iid) {
            if (iid.encoding().equals(ATTRIBUTE)) {
                return AttributeVertexImpl.Write.of(graph, iid.asAttribute());
            } else {
                return new ThingVertexImpl.Write.Persisted(graph, iid);
            }
        }

        protected abstract ThingAdjacency.Write newAdjacency(Encoding.Direction.Adjacency direction);

        @Override
        public ThingAdjacency.Write outs() {
            return outs;
        }

        @Override
        public ThingAdjacency.Write ins() {
            return ins;
        }

        @Override
        public void isInferred(boolean isInferred) {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        public boolean isModified() {
            return isModified;
        }

        public boolean isDeleted() {
            return isDeleted.get();
        }

        void deleteVertexFromGraph() {
            graph.delete(this);
        }

        void deleteVertexFromStorage() {
            graph.storage().deleteTracked(iid.bytes());
            graph.storage().deleteUntracked(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
        }

        void deleteEdges() {
            outs.deleteAll();
            ins.deleteAll();
        }

        void commitEdges() {
            outs.commit();
            ins.commit();
        }

        void commitVertex() {
            graph.storage().putTracked(iid.bytes());
            graph.storage().putUntracked(EdgeIID.InwardsISA.of(type().iid(), iid).bytes());
        }

        @Override
        public boolean isWrite() {
            return true;
        }

        @Override
        public ThingVertex.Write asWrite() {
            return this;
        }

        @Override
        public ThingVertex.Write toWrite() {
            return this;
        }

        @Override
        public AttributeVertex.Write<?> asAttribute() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(AttributeVertex.Write.class));
        }

        public static class Buffered extends ThingVertexImpl.Write {

            protected boolean isInferred;

            public Buffered(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
                super(graph, iid);
                this.isInferred = isInferred;
                setModified();
            }

            @Override
            protected ThingAdjacency.Write newAdjacency(Encoding.Direction.Adjacency direction) {
                return new ThingAdjacencyImpl.Write.Buffered(this, direction);
            }

            @Override
            public Encoding.Status status() {
                return Encoding.Status.BUFFERED;
            }

            @Override
            public void isInferred(boolean isInferred) {
                this.isInferred = isInferred;
            }

            @Override
            public boolean isInferred() {
                return isInferred;
            }

            @Override
            public void commit() {
                if (isInferred) throw TypeDBException.of(ILLEGAL_OPERATION);
                commitVertex();
                commitEdges();
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

        public static class Persisted extends ThingVertexImpl.Write {

            public Persisted(ThingGraph graph, VertexIID.Thing iid) {
                super(graph, iid);
            }

            @Override
            protected ThingAdjacency.Write newAdjacency(Encoding.Direction.Adjacency direction) {
                return new ThingAdjacencyImpl.Write.Persisted(this, direction);
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
                    graph.setModified(iid());
                }
            }
        }
    }
}
