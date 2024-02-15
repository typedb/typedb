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

package com.vaticle.typedb.core.graph.edge.impl;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.EdgeViewIID;
import com.vaticle.typedb.core.encoding.iid.PropertyIID;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Transaction.ILLEGAL_OPERATION;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.KEY;
import static com.vaticle.typeql.lang.common.TypeQLToken.Annotation.UNIQUE;
import static java.util.Objects.hash;

/**
 * A Type Edge that connects two Type Vertices, and an overridden Type Vertex.
 */
public abstract class TypeEdgeImpl implements TypeEdge {

    final TypeGraph graph;
    final Encoding.Edge.Type encoding;
    final View.Forward forward;
    final View.Backward backward;
    private int hash;

    TypeEdgeImpl(TypeGraph graph, Encoding.Edge.Type encoding) {
        this.graph = graph;
        this.encoding = encoding;
        this.forward = new View.Forward(this);
        this.backward = new View.Backward(this);
    }

    @Override
    public View.Forward forwardView() {
        return forward;
    }

    @Override
    public View.Backward backwardView() {
        return backward;
    }

    abstract EdgeViewIID.Type computeForwardIID();

    abstract EdgeViewIID.Type computeBackwardIID();

    abstract VertexIID.Type fromIID();

    abstract VertexIID.Type toIID();

    void writeAnnotations(Set<Annotation> annotations) {
        for (Annotation annotation : annotations) {
            switch (annotation) {
                case KEY:
                    // TODO: put KEY property once the OWNS_KEY edge is removed as a separate edge type
                    // graph.storage().putUntracked(PropertyIID.TypeEdge.of(from.iid(), to.iid(), Encoding.Property.Edge.OWNS_PROPERTY_ANNOTATION_KEY));
                    break;
                case UNIQUE:
                    graph.storage().putUntracked(PropertyIID.TypeEdge.of(fromIID(), toIID(), Encoding.Property.Edge.OWNS_PROPERTY_ANNOTATION_UNIQUE));
                    break;
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }

    @Override
    public final boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof TypeEdgeImpl)) return false;
        TypeEdgeImpl that = (TypeEdgeImpl) object;
        return (this.encoding.equals(that.encoding) &&
                this.fromIID().equals(that.fromIID()) &&
                this.toIID().equals(that.toIID()) &&
                this.annotations().equals(that.annotations()));
    }

    @Override
    public final int hashCode() {
        if (hash == 0) hash = hash(encoding, fromIID().hashCode(), toIID().hashCode(), annotations());
        return hash;
    }

    private static abstract class View<T extends TypeEdge.View<T>> implements TypeEdge.View<T> {

        final TypeEdgeImpl edge;
        EdgeViewIID.Type iidCache = null;

        private View(TypeEdgeImpl edge) {
            this.edge = edge;
        }

        @Override
        public TypeEdge edge() {
            return edge;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || this.getClass() != object.getClass()) return false;
            return edge.equals(((TypeEdgeImpl.View<?>) object).edge);
        }

        @Override
        public int hashCode() {
            return edge.hashCode();
        }

        private static class Forward extends TypeEdgeImpl.View<TypeEdge.View.Forward> implements TypeEdge.View.Forward {

            private Forward(TypeEdgeImpl edge) {
                super(edge);
            }

            @Override
            public EdgeViewIID.Type iid() {
                if (iidCache == null) iidCache = edge.computeForwardIID();
                return iidCache;
            }

            @Override
            public int compareTo(TypeEdge.View.Forward other) {
                return iid().compareTo(other.iid());
            }
        }

        private static class Backward extends TypeEdgeImpl.View<TypeEdge.View.Backward> implements TypeEdge.View.Backward {

            private Backward(TypeEdgeImpl edge) {
                super(edge);
            }

            @Override
            public EdgeViewIID.Type iid() {
                if (iidCache == null) iidCache = edge.computeBackwardIID();
                return iidCache;
            }

            @Override
            public int compareTo(TypeEdge.View.Backward other) {
                return iid().compareTo(other.iid());
            }
        }
    }

    /**
     * A Buffered Type Edge that connects two Type Vertices, and an overridden Type Vertex.
     */
    public static class Buffered extends TypeEdgeImpl implements TypeEdge {

        private final TypeVertex from;
        private final TypeVertex to;
        private final AtomicBoolean committed;
        private final AtomicBoolean deleted;
        private TypeVertex overridden;
        private Set<Annotation> annotations;

        /**
         * Default constructor for {@code EdgeImpl.Buffered}.
         *
         * @param from     the tail vertex
         * @param encoding the edge {@code Encoding}
         * @param to       the head vertex
         */
        public Buffered(Encoding.Edge.Type encoding, TypeVertex from, TypeVertex to) {
            super(from.graph(), encoding);
            assert this.graph == to.graph();
            this.from = from;
            this.to = to;
            this.annotations = new HashSet<>();
            committed = new AtomicBoolean(false);
            deleted = new AtomicBoolean(false);
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
        }

        @Override
        public EdgeViewIID.Type computeForwardIID() {
            return EdgeViewIID.Type.of(from().iid(), encoding.forward(), to().iid());
        }

        @Override
        public EdgeViewIID.Type computeBackwardIID() {
            return EdgeViewIID.Type.of(to().iid(), encoding.backward(), from().iid());
        }

        @Override
        public TypeVertex from() {
            return from;
        }

        @Override
        public TypeVertex to() {
            return to;
        }

        @Override
        VertexIID.Type fromIID() {
            return from.iid();
        }

        @Override
        VertexIID.Type toIID() {
            return to.iid();
        }

        @Override
        public Optional<TypeVertex> overridden() {
            return Optional.ofNullable(overridden);
        }

        @Override
        public void setOverridden(TypeVertex overridden) {
            this.overridden = overridden;
        }

        @Override
        public void unsetOverridden() {
            this.overridden = null;
        }

        @Override
        public Set<Annotation> annotations() {
            return annotations;
        }

        @Override
        public void setAnnotations(Set<Annotation> annotations) {
            this.annotations = annotations;
        }

        /**
         * Deletes this {@code Edge} from connecting between two {@code Vertex}.
         * <p>
         * A {@code TypeEdgeImpl.Buffered} can only exist in the adjacency cache of
         * each {@code Vertex}, and does not exist in storage.
         */
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from.outs().remove(this);
                to.ins().remove(this);
                if (from instanceof Persisted && to instanceof Persisted) {
                    graph.storage().deleteUntracked(forward.iid());
                    graph.storage().deleteUntracked(backward.iid());
                }
            }
        }

        @Override
        public boolean isDeleted() {
            return deleted.get();
        }

        /**
         * Commit operation of a buffered type edge.
         * <p>
         * This operation can only be performed once, and thus protected by {@code committed} boolean.
         * Then we check for each direction of this edge, whether they need to be persisted to storage.
         * It's possible that an edge only has a {@code encoding.out()} (most likely an optimisation edge)
         * and therefore will not have an inward edge to be persisted onto storage.
         */
        @Override
        public void commit() {
            if (committed.compareAndSet(false, true)) {
                // re-compute IID since new vertices change IID on commit, so a cached edge IID may not be valid anymore
                if (overridden != null) {
                    // TODO: Store overridden as an edge property instead in 3.0
                    graph.storage().putUntracked(computeForwardIID(), overridden.iid().bytes());
                    graph.storage().putUntracked(computeBackwardIID(), overridden.iid().bytes());
                } else {
                    graph.storage().putUntracked(computeForwardIID());
                    graph.storage().putUntracked(computeBackwardIID());
                }
                writeAnnotations(annotations);
            }
        }
    }

    public static class Target extends TypeEdgeImpl implements TypeEdge {

        private final TypeVertex from;
        private final TypeVertex to;

        public Target(Encoding.Edge.Type encoding, TypeVertex from, TypeVertex to) {
            super(from.graph(), encoding);
            this.from = from;
            this.to = to;
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
        }

        @Override
        EdgeViewIID.Type computeForwardIID() {
            return EdgeViewIID.Type.of(from.iid(), encoding.forward(), to.iid());
        }

        @Override
        EdgeViewIID.Type computeBackwardIID() {
            return EdgeViewIID.Type.of(to.iid(), encoding.backward(), from.iid());
        }

        @Override
        public TypeVertex from() {
            return from;
        }

        @Override
        public TypeVertex to() {
            return to;
        }

        @Override
        VertexIID.Type fromIID() {
            return from.iid();
        }

        @Override
        VertexIID.Type toIID() {
            return to.iid();
        }

        @Override
        public Optional<TypeVertex> overridden() {
            return Optional.empty();
        }

        @Override
        public void setOverridden(TypeVertex overridden) {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public void unsetOverridden() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public Set<Annotation> annotations() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public void setAnnotations(Set<Annotation> annotations) {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public void delete() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public boolean isDeleted() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }

        @Override
        public void commit() {
            throw TypeDBException.of(ILLEGAL_OPERATION);
        }
    }

    /**
     * Persisted Type Edge that connects two Type Vertices, and an overridden Type Vertex
     */
    public static class Persisted extends TypeEdgeImpl implements TypeEdge {

        private final VertexIID.Type fromIID;
        private final VertexIID.Type toIID;
        private final Encoding.Edge.Type encoding;
        private final AtomicBoolean deleted;
        private TypeVertex from;
        private TypeVertex to;
        private final AtomicBoolean committed;
        private Either<VertexIID.Type, TypeVertex> overridden;
        private Set<Annotation> annotations;

        /**
         * Default constructor for {@code Edge.Persisted}.
         * <p>
         * The edge can be constructed from an {@code iid} that represents
         * either an inwards or outwards pointing edge. Thus, we extract the
         * {@code start} and {@code end} of it, and use the {@code infix} of the
         * edge {@code iid} to determine the direction, and which vertex becomes
         * {@code fromIID} or {@code toIID}.
         * <p>
         * The head of this edge may or may not be overriding another vertex.
         * If it does the {@code overriddenIID} will not be null.
         *
         * @param graph the graph comprised of all the vertices
         * @param iid   the {@code iid} of a persisted edge
         */
        public Persisted(TypeGraph graph, EdgeViewIID.Type iid, @Nullable VertexIID.Type overriddenIID) {
            super(graph, iid.encoding());

            if (iid.isForward()) {
                fromIID = iid.start();
                toIID = iid.end();
            } else {
                fromIID = iid.end();
                toIID = iid.start();
            }
            encoding = iid.encoding();
            deleted = new AtomicBoolean(false);
            overridden = Either.first(overriddenIID);
            annotations = null;
            committed = new AtomicBoolean(false);
        }

        @Override
        public Encoding.Edge.Type encoding() {
            return encoding;
        }

        @Override
        public EdgeViewIID.Type computeForwardIID() {
            return EdgeViewIID.Type.of(fromIID, encoding.forward(), toIID);
        }

        public EdgeViewIID.Type computeBackwardIID() {
            return EdgeViewIID.Type.of(toIID, encoding.backward(), fromIID);
        }

        @Override
        public TypeVertex from() {
            if (from != null) return from;
            from = graph.convert(fromIID);
            from.outs().cache(this);
            return from;
        }

        @Override
        public TypeVertex to() {
            if (to != null) return to;
            to = graph.convert(toIID);
            to.ins().cache(this);
            return to;
        }

        @Override
        VertexIID.Type fromIID() {
            return fromIID;
        }

        @Override
        VertexIID.Type toIID() {
            return toIID;
        }

        @Override
        public Optional<TypeVertex> overridden() {
            if (overridden.isFirst()) {
                overridden = Either.second(overridden.first() != null ? graph.convert(overridden.first()) : null);
            }
            return Optional.ofNullable(overridden.second());
        }

        /**
         * Set the head type vertex of this type edge to override a given type vertex.
         * <p>
         * Once the property has been set, we write to storage immediately as this type edge
         * does not buffer information in memory before being persisted.
         *
         * @param overridden the type vertex to override by the head
         */
        @Override
        public void setOverridden(TypeVertex overridden) {
            this.overridden = Either.second(overridden);
        }

        @Override
        public void unsetOverridden() {
            this.overridden = Either.second(null);
        }

        @Override
        public Set<Annotation> annotations() {
            if (annotations == null) annotations = fetchAnnotations();
            return annotations;
        }

        private Set<Annotation> fetchAnnotations() {
            Set<Annotation> annotations = new HashSet<>();
            for (Encoding.Property.Edge encoding : Encoding.Property.Edge.values()) {
                if (encoding.isAnnotation() && graph.storage().get(PropertyIID.TypeEdge.of(fromIID(), toIID(), encoding)) != null) {
                    if (encoding == Encoding.Property.Edge.OWNS_PROPERTY_ANNOTATION_UNIQUE) {
                        annotations.add(UNIQUE);
                        // TODO: read KEY property once the OWNS_KEY edge is removed as a separate edge type
//                    } else if (encoding == Encoding.Property.Edge.OWNS_PROPERTY_ANNOTATION_KEY) {
//                        annotations.add(KEY)
//                    }
                    } else {
                        throw TypeDBException.of(ILLEGAL_STATE);
                    }
                }
            }
            // backwards-compatible annotation
            if (encoding == Encoding.Edge.Type.OWNS_KEY) annotations.add(KEY);
            return annotations;
        }

        @Override
        public void setAnnotations(Set<Annotation> annotations) {
            deleteAnnotations();
            writeAnnotations(annotations);
            this.annotations = annotations;
        }

        private void deleteAnnotations() {
            Set<Annotation> persistedAnnotations = annotations();
            for (Annotation annotation : persistedAnnotations) {
                switch (annotation) {
                    case KEY:
                        // TODO: delete KEY property once the OWNS_KEY edge is removed as a separate edge type
                        // graph.storage().deleteUntracked(PropertyIID.TypeEdge.of(from.iid(), to.iid(), Encoding.Property.Edge.OWNS_PROPERTY_ANNOTATION_KEY));
                        break;
                    case UNIQUE:
                        graph.storage().deleteUntracked(PropertyIID.TypeEdge.of(from.iid(), to.iid(), Encoding.Property.Edge.OWNS_PROPERTY_ANNOTATION_UNIQUE));
                        break;
                    default:
                        throw TypeDBException.of(ILLEGAL_STATE);
                }
            }
        }

        /**
         * Delete operation of a persisted edge.
         * <p>
         * This operation can only be performed once, and thus protected by
         * {@code isDelete} atomic boolean. We mark both from and to vertices
         * as modified, and delete both directions of this edge from the graph storage.
         */
        @Override
        public void delete() {
            if (deleted.compareAndSet(false, true)) {
                from().outs().remove(this);
                to().ins().remove(this);
                graph.storage().deleteUntracked(forwardView().iid());
                graph.storage().deleteUntracked(backwardView().iid());
                deleteAnnotations();
            }
        }

        @Override
        public boolean isDeleted() {
            return deleted.get();
        }

        @Override
        public void commit() {
            if (committed.compareAndSet(false, true) && overridden.isSecond()) {
                if (overridden.second() != null) {
                    // TODO: Store overridden as an edge property instead in 3.0
                    graph.storage().putUntracked(forward.iid(), overridden.second().iid().bytes());
                    graph.storage().putUntracked(backward.iid(), overridden.second().iid().bytes());
                } else {
                    graph.storage().putUntracked(forward.iid());
                    graph.storage().putUntracked(backward.iid());
                }
            }
        }
    }
}
