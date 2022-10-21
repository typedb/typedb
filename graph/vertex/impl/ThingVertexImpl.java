/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.encoding.iid.KeyIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.ThingGraph;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.adjacency.impl.ThingAdjacencyImpl;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_VERTEX_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Thing.ATTRIBUTE;

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
        throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(ThingVertex.Write.class));
    }

    public static class Read extends ThingVertexImpl {

        protected final ThingAdjacency.Out outs;
        protected final ThingAdjacency.In ins;

        public Read(ThingGraph graph, VertexIID.Thing iid) {
            super(graph, iid);
            this.outs = new ThingAdjacencyImpl.Read.Out(this);
            this.ins = new ThingAdjacencyImpl.Read.In(this);
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
        public ThingVertex.Write toWrite() {
            return graph.convertToWritable(iid);
        }

        @Override
        public ThingAdjacency.In ins() {
            return ins;
        }

        @Override
        public ThingAdjacency.Out outs() {
            return outs;
        }

        @Override
        public boolean isModified() {
            return false;
        }

        @Override
        public AttributeVertex<?> asAttribute() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(AttributeVertex.class));
        }
    }

    public static abstract class Write extends ThingVertexImpl implements ThingVertex.Write {

        protected final ThingAdjacency.Write.Out outs;
        protected final ThingAdjacency.Write.In ins;
        protected final AtomicBoolean isDeleted;
        protected boolean isModified;

        Write(ThingGraph graph, VertexIID.Thing iid) {
            super(graph, iid);
            this.isModified = false;
            this.isDeleted = new AtomicBoolean(false);
            this.outs = newOutAdjacency();
            this.ins = newInAdjacency();
        }

        public static ThingVertexImpl.Write of(ThingGraph graph, VertexIID.Thing iid) {
            if (iid.encoding().equals(ATTRIBUTE)) {
                return AttributeVertexImpl.Write.of(graph, iid.asAttribute());
            } else {
                return new ThingVertexImpl.Write.Persisted(graph, iid);
            }
        }

        protected abstract ThingAdjacency.Write.In newInAdjacency();

        protected abstract ThingAdjacency.Write.Out newOutAdjacency();

        @Override
        public ThingAdjacency.Write.Out outs() {
            return outs;
        }

        @Override
        public ThingAdjacency.Write.In ins() {
            return ins;
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
            graph.storage().deleteTracked(iid);
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
            graph.storage().putTracked(iid);
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
        public AttributeVertexImpl.Write<?> asAttribute() {
            throw TypeDBException.of(INVALID_THING_VERTEX_CASTING, className(getClass()), className(AttributeVertex.Write.class));
        }

        public static class Buffered extends ThingVertexImpl.Write {

            private final boolean isInferred;

            public Buffered(ThingGraph graph, VertexIID.Thing iid, boolean isInferred) {
                super(graph, iid);
                this.isInferred = isInferred;
                setModified();
            }

            @Override
            protected ThingAdjacency.Write.In newInAdjacency() {
                return new ThingAdjacencyImpl.Write.Buffered.In(this);
            }

            @Override
            protected ThingAdjacency.Write.Out newOutAdjacency() {
                return new ThingAdjacencyImpl.Write.Buffered.Out(this);
            }

            @Override
            public Encoding.Status status() {
                return Encoding.Status.BUFFERED;
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
            protected ThingAdjacency.Write.In newInAdjacency() {
                return new ThingAdjacencyImpl.Write.Persisted.In(this);
            }

            @Override
            protected ThingAdjacency.Write.Out newOutAdjacency() {
                return new ThingAdjacencyImpl.Write.Persisted.Out(this);
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

    /**
     * A Target vertex can emulate a Read or Write vertex for the purposes of seeking to a particular
     * vertex that may or may not exist on disk or in transaction-local write-vertex buffers. However
     * it should not be usable for anything else.
     */
    public static class Target extends ThingVertexImpl {

        private Target(ThingGraph graph, VertexIID.Thing iid) {
            super(graph, iid);
        }

        public static Target of(ThingGraph graph, VertexIID.Thing iid) {
            return new Target(graph, iid);
        }

        @Override
        public ThingVertex.Write toWrite() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public ThingAdjacency.Write.Out outs() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public ThingAdjacency.Write.In ins() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public AttributeVertex.Write<?> asAttribute() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public Encoding.Status status() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public boolean isModified() {
            return false;
        }
    }
}
