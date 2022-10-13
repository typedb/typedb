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

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.PrefixIID;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalEdge;
import com.vaticle.typedb.core.traversal.planner.PlannerEdge;
import com.vaticle.typedb.core.traversal.predicate.Predicate.Value;
import com.vaticle.typedb.core.traversal.scanner.GraphIterator;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.loop;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
import static com.vaticle.typedb.core.encoding.Encoding.Direction.Edge.BACKWARD;
import static com.vaticle.typedb.core.encoding.Encoding.Direction.Edge.FORWARD;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.ISA;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.HAS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.PLAYING;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.RELATING;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Optimised.ROLEPLAYER;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.RELATES;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.SUB;
import static com.vaticle.typedb.core.encoding.Encoding.Prefix.VERTEX_ATTRIBUTE;
import static com.vaticle.typedb.core.encoding.Encoding.Prefix.VERTEX_ROLE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Thing.RELATION;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;

public abstract class ProcedureEdge<
        VERTEX_FROM extends ProcedureVertex<?, ?>, VERTEX_TO extends ProcedureVertex<?, ?>
        > extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    private final Encoding.Direction.Edge direction;
    private final int hash;

    private ProcedureEdge(VERTEX_FROM from, VERTEX_TO to, Encoding.Direction.Edge direction, String symbol) {
        super(from, to, symbol);
        this.direction = direction;
        this.hash = Objects.hash(from(), to(), direction);
    }

    public static ProcedureEdge<?, ?> of(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
                                         PlannerEdge.Directional<?, ?> plannerEdge) {
        Encoding.Direction.Edge dir = plannerEdge.direction();
        if (plannerEdge.isEqual()) {
            return new Equal(from, to, dir);
        } else if (plannerEdge.isPredicate()) {
            return new Predicate(from.asThing(), to.asThing(), dir, plannerEdge.asPredicate().predicate());
        } else if (plannerEdge.isNative()) {
            return Native.of(from, to, plannerEdge.asNative());
        } else {
            throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    public static ProcedureEdge<?, ?> of(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
                                         StructureEdge<?, ?> structureEdge, boolean isForward) {
        Encoding.Direction.Edge dir = isForward ? FORWARD : BACKWARD;
        if (structureEdge.isEqual()) {
            return new Equal(from, to, dir);
        } else if (structureEdge.isPredicate()) {
            return new Predicate(from.asThing(), to.asThing(), dir, structureEdge.asPredicate().predicate());
        } else if (structureEdge.isNative()) {
            return Native.of(from, to, structureEdge.asNative(), isForward);
        } else {
            throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    public abstract Forwardable<? extends Vertex<?, ?>, Order.Asc> branch(
            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
    );

    public abstract boolean isClosure(
            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
    );

    public Encoding.Direction.Edge direction() {
        return direction;
    }

    public boolean onlyStartsFromAttributeType() {
        return false;
    }

    public boolean onlyStartsFromRelationType() {
        return false;
    }

    public boolean onlyStartsFromRoleType() {
        return false;
    }

    public boolean onlyStartsFromThingType() {
        return false;
    }

    public boolean isRelating() {
        return false;
    }

    public Native.Thing.Relating asRelating() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Native.Thing.Relating.class));
    }

    public boolean isRolePlayer() {
        return false;
    }

    public Native.Thing.RolePlayer asRolePlayer() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Native.Thing.RolePlayer.class));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcedureEdge<?, ?> that = (ProcedureEdge<?, ?>) o;
        return from().equals(that.from()) && to().equals(that.to()) && direction == that.direction && symbol.equals(that.symbol);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        if (direction.isForward()) {
            return String.format("(%s *--[%s]--> %s)", from.id(), symbol, to.id());
        } else {
            return String.format("(%s <--[%s]--* %s)", from.id(), symbol, to.id());
        }
    }

    public abstract ProcedureEdge<?, ?> reverse();

    public static class Equal extends ProcedureEdge<ProcedureVertex<?, ?>, ProcedureVertex<?, ?>> {

        Equal(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
              Encoding.Direction.Edge direction) {
            super(from, to, direction, TypeQLToken.Predicate.Equality.EQ.toString());
        }

        @Override
        public Forwardable<? extends Vertex<?, ?>, Order.Asc> branch(
                GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
        ) {
            if (fromVertex.isThing()) {
                if (to.isType()) return emptySorted();
                else return to.asThing().iterateAndFilter(fromVertex.asThing(), params, ASC);
            } else if (fromVertex.isType()) {
                if (to.isThing()) return emptySorted();
                else return to.asType().filter(iterateSorted(ASC, fromVertex.asType()));
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params) {
            assert fromVertex != null && toVertex != null;
            return fromVertex.equals(toVertex);
        }

        @Override
        public ProcedureEdge<?, ?> reverse() {
            Encoding.Direction.Edge reverseDirection = direction().isForward() ? BACKWARD : FORWARD;
            return new Equal(to, from, reverseDirection);
        }
    }

    static class Predicate extends ProcedureEdge<ProcedureVertex.Thing, ProcedureVertex.Thing> {

        private final com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate;

        Predicate(ProcedureVertex.Thing from, ProcedureVertex.Thing to, Encoding.Direction.Edge direction,
                  com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate) {
            super(from, to, direction, predicate.toString());
            this.predicate = predicate;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && ((Predicate) o).predicate.equals(predicate);
        }

        @Override
        public Forwardable<? extends Vertex<?, ?>, Order.Asc> branch(
                GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
        ) {
            assert fromVertex.isThing() && fromVertex.asThing().isAttribute();

            Forwardable<? extends ThingVertex, Order.Asc> toIter;
            if (to.props().hasIID()) toIter = to.iterateAndFilterFromIID(graphMgr, params, ASC);
            else toIter = to.iterateAndFilterFromTypes(graphMgr, params, ASC, false);

            return toIter.filter(toVertex -> {
                AttributeVertex<?> from = fromVertex.asThing().asAttribute();
                AttributeVertex<?> to = toVertex.asAttribute();
                return predicate.apply(from.isValue() ? from.toValue().toAttribute() : from, to.isValue() ? to.toValue().toAttribute() : to);
            });
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params) {
            assert fromVertex.isThing() && fromVertex.asThing().isAttribute() &&
                    toVertex.isThing() && toVertex.asThing().isAttribute();
            return predicate.apply(fromVertex.asThing().asAttribute(), toVertex.asThing().asAttribute());
        }

        @Override
        public ProcedureEdge<?, ?> reverse() {
            throw TypeDBException.of(UNSUPPORTED_OPERATION);
        }
    }

    public static abstract class Native<
            VERTEX_NATIVE_FROM extends ProcedureVertex<?, ?>, VERTEX_NATIVE_TO extends ProcedureVertex<?, ?>
            > extends ProcedureEdge<VERTEX_NATIVE_FROM, VERTEX_NATIVE_TO> {

        private Native(VERTEX_NATIVE_FROM from, VERTEX_NATIVE_TO to, Encoding.Direction.Edge direction, Encoding.Edge encoding) {
            super(from, to, direction, encoding.name());
        }

        static Native<?, ?> of(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to, PlannerEdge.Native.Directional<?, ?> edge) {
            boolean isForward = edge.direction().isForward();
            if (edge.isIsa()) {
                boolean isTransitive = edge.asIsa().isTransitive();
                if (isForward) return new Isa.Forward(from.asThing(), to.asType(), isTransitive);
                else return new Isa.Backward(from.asType(), to.asThing(), isTransitive);
            } else if (edge.isType()) {
                return Native.Type.of(from.asType(), to.asType(), edge.asType());
            } else if (edge.isThing()) {
                return Native.Thing.of(from.asThing(), to.asThing(), edge.asThing());
            } else {
                throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        }

        public static ProcedureEdge<?, ?> of(
                ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to, StructureEdge.Native<?, ?> edge, boolean isForward
        ) {
            if (edge.encoding().equals(ISA)) {
                boolean isTransitive = edge.isTransitive();
                if (isForward) return new Isa.Forward(from.asThing(), to.asType(), isTransitive);
                else return new Isa.Backward(from.asType(), to.asThing(), isTransitive);
            } else if (edge.encoding().isType()) {
                return Native.Type.of(from.asType(), to.asType(), edge, isForward);
            } else if (edge.encoding().isThing()) {
                return Native.Thing.of(from.asThing(), to.asThing(), edge, isForward);
            } else {
                throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        }

        static abstract class Isa<
                VERTEX_ISA_FROM extends ProcedureVertex<?, ?>, VERTEX_ISA_TO extends ProcedureVertex<?, ?>
                > extends Native<VERTEX_ISA_FROM, VERTEX_ISA_TO> {

            final boolean isTransitive;

            private Isa(VERTEX_ISA_FROM from, VERTEX_ISA_TO to, Encoding.Direction.Edge direction, boolean isTransitive) {
                super(from, to, direction, ISA);
                this.isTransitive = isTransitive;
            }

            Forwardable<TypeVertex, Order.Asc> isaTypes(ThingVertex thing) {
                if (!isTransitive) return iterateSorted(ASC, thing.type());
                else return iterateSorted(thing.type().graph().getSupertypes(thing.type()), ASC);
            }

            @Override
            public String toString() {
                return super.toString() + String.format(" { isTransitive: %s }", isTransitive);
            }

            static class Forward extends Isa<ProcedureVertex.Thing, ProcedureVertex.Type> {

                Forward(ProcedureVertex.Thing thing, ProcedureVertex.Type type, boolean isTransitive) {
                    super(thing, type, FORWARD, isTransitive);
                }

                @Override
                public Forwardable<? extends Vertex<?, ?>, Order.Asc> branch(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                ) {
                    assert fromVertex.isThing();
                    Forwardable<TypeVertex, Order.Asc> iter = isaTypes(fromVertex.asThing());
                    return to.filter(iter);
                }

                @Override
                public boolean isClosure(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                ) {
                    assert fromVertex.isThing() && toVertex.isType();
                    return isaTypes(fromVertex.asThing()).findFirst(toVertex.asType()).isPresent();
                }

                @Override
                public ProcedureEdge<?, ?> reverse() {
                    return new Backward(to(), from(), isTransitive);
                }
            }

            static class Backward extends Isa<ProcedureVertex.Type, ProcedureVertex.Thing> {

                Backward(ProcedureVertex.Type type, ProcedureVertex.Thing thing, boolean isTransitive) {
                    super(type, thing, BACKWARD, isTransitive);
                }

                @Override
                public Forwardable<? extends Vertex<?, ?>, Order.Asc> branch(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                ) {
                    assert fromVertex.isType();
                    TypeVertex type = fromVertex.asType();
                    Set<TypeVertex> isaTypes = isTransitive ? graphMgr.schema().getSubtypes(type) : set(type);
                    if (to.props().hasIID()) {
                        return to.iterateAndFilterFromIID(graphMgr, params, ASC).filter(vertex -> isaTypes.contains(vertex.type()));
                    } else {
                        FunctionalIterator<TypeVertex> toTypes = iterate(isaTypes).filter(v -> to.props().types().contains(v.properLabel()));
                        if (!toTypes.hasNext()) return emptySorted();
                        else return to.iterateAndFilterFromTypes(graphMgr, params, toTypes, ASC, false);
                    }
                }

                @Override
                public boolean isClosure(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                ) {
                    assert fromVertex.isType() && toVertex.isThing();
                    return isaTypes(toVertex.asThing()).findFirst(fromVertex.asType()).isPresent();
                }

                @Override
                public ProcedureEdge<?, ?> reverse() {
                    return new Forward(to(), from(), isTransitive);
                }
            }
        }

        public static abstract class Type extends Native<ProcedureVertex.Type, ProcedureVertex.Type> {

            private Type(ProcedureVertex.Type from, ProcedureVertex.Type to, Encoding.Direction.Edge direction, Encoding.Edge encoding) {
                super(from, to, direction, encoding);
            }

            static Native.Type of(ProcedureVertex.Type from, ProcedureVertex.Type to, PlannerEdge.Native.Type.Directional edge) {
                boolean isForward = edge.direction().isForward();
                boolean isTransitive = edge.isTransitive();

                if (edge.isSub()) {
                    if (isForward) return new Sub.Forward(from, to, isTransitive);
                    else return new Sub.Backward(from, to, isTransitive);
                } else if (edge.isOwns()) {
                    if (isForward) return new Owns.Forward(from, to, edge.asOwns().isKey());
                    else return new Owns.Backward(from, to, edge.asOwns().isKey());
                } else if (edge.isPlays()) {
                    if (isForward) return new Plays.Forward(from, to);
                    else return new Plays.Backward(from, to);
                } else if (edge.isRelates()) {
                    if (isForward) return new Relates.Forward(from, to);
                    else return new Relates.Backward(from, to);
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            static ProcedureEdge<?, ?> of(
                    ProcedureVertex.Type from, ProcedureVertex.Type to, StructureEdge.Native<?, ?> edge, boolean isForward
            ) {
                switch (edge.encoding().asType()) {
                    case SUB:
                        if (isForward) return new Sub.Forward(from, to, edge.isTransitive());
                        else return new Sub.Backward(from, to, edge.isTransitive());
                    case OWNS:
                        if (isForward) return new Owns.Forward(from, to, false);
                        else return new Owns.Backward(from, to, false);
                    case OWNS_KEY:
                        if (isForward) return new Owns.Forward(from, to, true);
                        else return new Owns.Backward(from, to, true);
                    case PLAYS:
                        if (isForward) return new Plays.Forward(from, to);
                        else return new Plays.Backward(from, to);
                    case RELATES:
                        if (isForward) return new Relates.Forward(from, to);
                        else return new Relates.Backward(from, to);
                    default:
                        throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            static abstract class Sub extends Type {

                final boolean isTransitive;

                private Sub(ProcedureVertex.Type from, ProcedureVertex.Type to, Encoding.Direction.Edge direction, boolean isTransitive) {
                    super(from, to, direction, SUB);
                    this.isTransitive = isTransitive;
                }

                Forwardable<TypeVertex, Order.Asc> superTypes(TypeVertex type) {
                    if (!isTransitive) return type.outs().edge(SUB).to();
                    else {
                        TreeSet<TypeVertex> superTypes = new TreeSet<>();
                        loop(type, Objects::nonNull, v -> v.outs().edge(SUB).to().firstOrNull()).forEachRemaining(superTypes::add);
                        return iterateSorted(superTypes, ASC);
                    }
                }

                @Override
                public boolean equals(Object o) {
                    return super.equals(o) && ((Sub) o).isTransitive == isTransitive;
                }

                @Override
                public String toString() {
                    return super.toString() + String.format(" { isTransitive: %s }", isTransitive);
                }

                static class Forward extends Sub {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, boolean isTransitive) {
                        super(from, to, FORWARD, isTransitive);
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        Forwardable<TypeVertex, Order.Asc> iterator = superTypes(fromVertex.asType());
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        return superTypes(fromVertex.asType()).findFirst(toVertex.asType()).isPresent();
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Sub.Backward(to, from, isTransitive);
                    }
                }

                static class Backward extends Sub {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, boolean isTransitive) {
                        super(from, to, BACKWARD, isTransitive);
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType();
                        Forwardable<TypeVertex, Order.Asc> iter;
                        TypeVertex type = fromVertex.asType();
                        if (!isTransitive) iter = type.ins().edge(SUB).from();
                        else iter = iterateSorted(graphMgr.schema().getSubtypes(type), ASC);
                        return to.filter(iter);
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        return superTypes(toVertex.asType()).findFirst(fromVertex.asType()).isPresent();
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Sub.Forward(to, from, isTransitive);
                    }
                }
            }

            static abstract class Owns extends Type {

                final boolean isKey;

                private Owns(ProcedureVertex.Type from, ProcedureVertex.Type to, Encoding.Direction.Edge direction, boolean isKey) {
                    super(from, to, direction, OWNS);
                    this.isKey = isKey;
                }

                @Override
                public boolean equals(Object o) {
                    return super.equals(o) && ((Owns) o).isKey == isKey;
                }

                @Override
                public String toString() {
                    return super.toString() + String.format(" { isKey: %s }", isKey);
                }

                static class Forward extends Owns {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, boolean isKey) {
                        super(from, to, FORWARD, isKey);
                    }

                    @Override
                    public boolean onlyStartsFromThingType() {
                        return true;
                    }

                    private NavigableSet<TypeVertex> ownedAttributeTypes(GraphManager graphMgr, TypeVertex fromVertex) {
                        return isKey ?
                                graphMgr.schema().ownedKeyAttributeTypes(fromVertex) :
                                graphMgr.schema().ownedAttributeTypes(fromVertex);
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType();
                        return to.filter(iterateSorted(ownedAttributeTypes(graphMgr, fromVertex.asType()), ASC));
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType() && toVertex.isType();
                        return ownedAttributeTypes(graphMgr, fromVertex.asType()).contains(toVertex.asType());
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Owns.Backward(to, from, isKey);
                    }
                }

                static class Backward extends Owns {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, boolean isKey) {
                        super(from, to, BACKWARD, isKey);
                    }

                    @Override
                    public boolean onlyStartsFromAttributeType() {
                        return true;
                    }

                    private NavigableSet<TypeVertex> ownersOfAttributeType(GraphManager graphMgr, TypeVertex attType) {
                        return isKey ?
                                graphMgr.schema().ownersOfAttributeTypeKey(attType) :
                                graphMgr.schema().ownersOfAttributeType(attType);
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType();
                        return to.filter(iterateSorted(ownersOfAttributeType(graphMgr, fromVertex.asType()), ASC));
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        assert fromVertex.isType() && toVertex.isType();
                        return ownersOfAttributeType(graphMgr, fromVertex.asType()).contains(toVertex.asType());
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Owns.Forward(to, from, isKey);
                    }
                }
            }

            static abstract class Plays extends Type {

                private Plays(ProcedureVertex.Type from, ProcedureVertex.Type to, Encoding.Direction.Edge direction) {
                    super(from, to, direction, PLAYS);
                }

                static class Forward extends Plays {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    public boolean onlyStartsFromThingType() {
                        return true;
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType();
                        return to.filter(iterateSorted(graphMgr.schema().playedRoleTypes(fromVertex.asType()), ASC));
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType() && toVertex.isType();
                        return graphMgr.schema().playedRoleTypes(fromVertex.asType()).contains(toVertex.asType());
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Plays.Backward(to, from);
                    }
                }

                static class Backward extends Plays {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    public boolean onlyStartsFromRoleType() {
                        return true;
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType();
                        return to.filter(iterateSorted(graphMgr.schema().playersOfRoleType(fromVertex.asType()), ASC));
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType() && toVertex.isType();
                        return graphMgr.schema().playersOfRoleType(fromVertex.asType()).contains(toVertex.asType());
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Plays.Forward(to, from);
                    }
                }
            }

            static abstract class Relates extends Type {

                private Relates(ProcedureVertex.Type from, ProcedureVertex.Type to, Encoding.Direction.Edge direction) {
                    super(from, to, direction, RELATES);
                }

                static class Forward extends Relates {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    public boolean onlyStartsFromRelationType() {
                        return true;
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType();
                        return to.filter(iterateSorted(graphMgr.schema().relatedRoleTypes(fromVertex.asType()), ASC));
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        return graphMgr.schema().relatedRoleTypes(fromVertex.asType()).contains(toVertex.asType());
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Relates.Backward(to, from);
                    }
                }

                static class Backward extends Relates {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    public boolean onlyStartsFromRoleType() {
                        return true;
                    }

                    @Override
                    public Forwardable<TypeVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType();
                        return to.filter(iterateSorted(graphMgr.schema().relationsOfRoleType(fromVertex.asType()), ASC));
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isType() && toVertex.isType();
                        return graphMgr.schema().relationsOfRoleType(fromVertex.asType()).contains(toVertex.asType());
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Relates.Forward(to, from);
                    }
                }
            }
        }

        public static abstract class Thing extends Native<ProcedureVertex.Thing, ProcedureVertex.Thing> {

            private Thing(ProcedureVertex.Thing from, ProcedureVertex.Thing to, Encoding.Direction.Edge direction, Encoding.Edge encoding) {
                super(from, to, direction, encoding);
            }

            static Thing of(ProcedureVertex.Thing from, ProcedureVertex.Thing to, PlannerEdge.Native.Thing.Directional edge) {
                boolean isForward = edge.direction().isForward();

                if (edge.isHas()) {
                    if (isForward) return new Has.Forward(from, to);
                    else return new Has.Backward(from, to);
                } else if (edge.isPlaying()) {
                    if (isForward) return new Playing.Forward(from, to);
                    else return new Playing.Backward(from, to);
                } else if (edge.isRelating()) {
                    if (isForward) return new Relating.Forward(from, to);
                    else return new Relating.Backward(from, to);
                } else if (edge.isRolePlayer()) {
                    PlannerEdge.Native.Thing.RolePlayer.Directional rp = edge.asRolePlayer();
                    if (isForward) return new RolePlayer.Forward(from, to, rp.repetition(), rp.roleTypes());
                    else return new RolePlayer.Backward(from, to, rp.repetition(), rp.roleTypes());
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            static ProcedureEdge<ProcedureVertex.Thing, ProcedureVertex.Thing> of(
                    ProcedureVertex.Thing from, ProcedureVertex.Thing to, StructureEdge.Native<?, ?> edge, boolean isForward
            ) {
                Encoding.Edge.Thing encoding = edge.encoding().asThing();
                if (encoding == HAS) {
                    if (isForward) return new Has.Forward(from, to);
                    else return new Has.Backward(from, to);
                } else if (encoding == RELATING) {
                    if (isForward) return new Relating.Forward(from, to);
                    else return new Relating.Backward(from, to);
                } else if (encoding == PLAYING) {
                    if (isForward) return new Playing.Forward(from, to);
                    else return new Playing.Backward(from, to);
                } else if (encoding == ROLEPLAYER) {
                    StructureEdge.Native.RolePlayer rp = edge.asRolePlayer();
                    if (isForward) return new RolePlayer.Forward(from, to, rp.repetition(), rp.types());
                    else return new RolePlayer.Backward(from, to, rp.repetition(), rp.types());
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            public abstract Forwardable<? extends ThingVertex, Order.Asc> branch(
                    GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
            );

            Optional<ThingVertex> backwardBranchToIIDFiltered(
                    GraphManager graphMgr, ThingVertex fromVertex, Encoding.Edge.Thing encoding,
                    VertexIID.Thing toIID, Set<Label> allowedToTypes
            ) {
                ThingVertex toVertex = graphMgr.data().getReadable(toIID);
                if (toVertex != null && fromVertex.ins().edge(encoding, toVertex) != null &&
                        allowedToTypes.contains(toVertex.type().properLabel())) {
                    return Optional.of(toVertex);
                } else {
                    return Optional.empty();
                }
            }

            Forwardable<ThingVertex, Order.Asc> forwardBranchToRole(
                    GraphManager graphMgr, Vertex<?, ?> fromVertex, Encoding.Edge.Thing.Base encoding
            ) {
                assert !to.props().hasIID() && to.props().predicates().isEmpty();
                ThingVertex relationOrPlayer = fromVertex.asThing();
                TypeVertex type = relationOrPlayer.type();
                Set<TypeVertex> roleTypes = type.isRelationType() ?
                        graphMgr.schema().relatedRoleTypes(type) : graphMgr.schema().playedRoleTypes(type);
                return iterate(roleTypes)
                        .filter(rt -> to.props().types().contains(rt.properLabel()))
                        .mergeMapForwardable(t -> relationOrPlayer.outs().edge(encoding, PrefixIID.of(VERTEX_ROLE), t.iid()).to(), ASC);
            }

            static abstract class Has extends Thing {

                private Has(ProcedureVertex.Thing from, ProcedureVertex.Thing to, Encoding.Direction.Edge direction) {
                    super(from, to, direction, HAS);
                }

                static class Forward extends Has {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    public Forwardable<? extends ThingVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing();
                        ThingVertex owner = fromVertex.asThing();
                        if (to.props().hasIID()) {
                            Optional<AttributeVertex<?>> attributeVertex = branchToIID(graphMgr, params, owner);
                            if (attributeVertex.isPresent()) {
                                return to.iterateAndFilterPredicates(attributeVertex.get(), params, ASC);
                            } else return emptySorted(ASC);
                        } else {
                            Optional<Value<?, ?>> eq = iterate(to.props().predicates()).filter(p -> p.operator().equals(EQ)).first();
                            if (eq.isPresent()) {
                                return to.iterateAndFilterPredicates(branchToEq(graphMgr, params, owner, eq.get()), params, ASC, false);
                            } else {
                                return to.mergeAndFilterPredicatesOnVertices(graphMgr, branchToTypes(graphMgr, owner), params, ASC, false);
                            }
                        }
                    }

                    private Optional<AttributeVertex<?>> branchToIID(
                            GraphManager graphMgr, Traversal.Parameters params, ThingVertex owner
                    ) {
                        assert to.props().hasIID() && to.id().isVariable();
                        VertexIID.Thing iid = params.getIID(to.id().asVariable());
                        AttributeVertex<?> att = iid.isAttribute() ? graphMgr.data().getReadable(iid.asAttribute()) : null;
                        if (att != null && to.props().types().contains(att.type().properLabel()) && owner.outs().edge(HAS, att) != null) {
                            return Optional.of(att);
                        } else return Optional.empty();
                    }

                    private List<AttributeVertex<?>> branchToEq(
                            GraphManager graphMgr, Traversal.Parameters params, ThingVertex owner, Value<?, ?> eq
                    ) {
                        return iterate(to.attributesEqual(graphMgr, params, eq)).filter(a -> owner.outs().edge(HAS, a.asAttribute()) != null).toList();
                    }

                    private List<Pair<TypeVertex, Forwardable<ThingVertex, Order.Asc>>> branchToTypes(
                            GraphManager graphMgr, ThingVertex owner
                    ) {
                        Set<TypeVertex> types = graphMgr.schema().ownedAttributeTypes(owner.type());
                        return iterate(types)
                                .filter(t -> to.props().types().contains(t.properLabel()))
                                .map(t -> new Pair<>(t, owner.outs().edge(HAS, PrefixIID.of(VERTEX_ATTRIBUTE), t.iid()).to()))
                                .toList();
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().outs().edge(HAS, toVertex.asThing()) != null;
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Backward(to, from);
                    }
                }

                static class Backward extends Has {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    public Forwardable<? extends ThingVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing() && fromVertex.asThing().isAttribute();
                        AttributeVertex<?> att = fromVertex.asThing().asAttribute();

                        if (to.props().hasIID()) {
                            Optional<ThingVertex> toVertex = backwardBranchToIIDFiltered(graphMgr, att, HAS, params.getIID(to.id().asVariable()), to.props().types());
                            if (toVertex.isPresent()) return to.iterateAndFilterPredicates(toVertex.get(), params, ASC);
                            else return emptySorted();
                        } else {
                            Set<TypeVertex> owners = graphMgr.schema().ownersOfAttributeType(att.type());
                            return to.mergeAndFilterPredicatesOnVertices(
                                    graphMgr,
                                    iterate(owners).filter(owner -> to.props().types().contains(owner.properLabel()))
                                            .map(t -> new Pair<>(t, att.ins().edge(HAS, PrefixIID.of(t.encoding().instance()), t.iid()).from()))
                                            .toList(),
                                    params, ASC, false
                            );
                        }
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                             Vertex<?, ?> toVertex, Traversal.Parameters params) {
                        return fromVertex.asThing().ins().edge(HAS, toVertex.asThing()) != null;
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Forward(to, from);
                    }
                }
            }

            static abstract class Playing extends Thing {

                private Playing(ProcedureVertex.Thing from, ProcedureVertex.Thing to, Encoding.Direction.Edge direction) {
                    super(from, to, direction, PLAYING);
                }

                static class Forward extends Playing {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    public Forwardable<ThingVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing();
                        return forwardBranchToRole(graphMgr, fromVertex, PLAYING);
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        return fromVertex.asThing().outs().edge(PLAYING, toVertex.asThing()) != null;
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Backward(to, from);
                    }
                }

                static class Backward extends Playing {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    public Forwardable<? extends ThingVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing();
                        ThingVertex role = fromVertex.asThing();
                        Set<Label> toTypes = to.props().types();
                        Forwardable<ThingVertex, Order.Asc> iter;

                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            Optional<ThingVertex> toVertex = backwardBranchToIIDFiltered(graphMgr, role, PLAYING, params.getIID(to.id().asVariable()), toTypes);
                            if (toVertex.isPresent()) return to.iterateAndFilter(toVertex.get(), params, ASC);
                            else return emptySorted();
                        } else {
                            Set<TypeVertex> players = graphMgr.schema().playersOfRoleType(role.type());
                            return to.mergeAndFilterPredicatesOnVertices(
                                    graphMgr,
                                    iterate(players).filter(player -> toTypes.contains(player.properLabel()))
                                            .map(t -> new Pair<>(t, role.ins().edge(PLAYING, PrefixIID.of(t.encoding().instance()), t.iid()).from()))
                                            .toList(),
                                    params, ASC, false
                            );
                        }
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        return fromVertex.asThing().ins().edge(PLAYING, toVertex.asThing()) != null;
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Forward(to, from);
                    }
                }
            }

            public static abstract class Relating extends Thing {

                private Relating(ProcedureVertex.Thing from, ProcedureVertex.Thing to, Encoding.Direction.Edge direction) {
                    super(from, to, direction, RELATING);
                }

                @Override
                public boolean isRelating() {
                    return true;
                }

                @Override
                public Relating asRelating() {
                    return this;
                }

                static class Forward extends Relating {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    public Forwardable<? extends ThingVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing();
                        return forwardBranchToRole(graphMgr, fromVertex, RELATING);
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        return fromVertex.asThing().outs().edge(RELATING, toVertex.asThing()) != null;
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Backward(to, from);
                    }
                }

                static class Backward extends Relating {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    public Forwardable<? extends ThingVertex, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing() && to.props().predicates().isEmpty();
                        ThingVertex role = fromVertex.asThing();
                        Set<Label> toTypes = to.props().types();
                        Forwardable<? extends ThingVertex, Order.Asc> iter;

                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            Optional<ThingVertex> toVertex = backwardBranchToIIDFiltered(graphMgr, role, RELATING, params.getIID(to.id().asVariable()), toTypes);
                            return toVertex.map(thingVertex -> iterateSorted(ASC, thingVertex)).orElseGet(SortedIterators.Forwardable::emptySorted);
                        } else {
                            Set<TypeVertex> relations = graphMgr.schema().relationsOfRoleType(role.type());
                            iter = iterate(relations).filter(rel -> toTypes.contains(rel.properLabel()))
                                    .mergeMapForwardable(t -> role.ins().edge(RELATING, PrefixIID.of(RELATION), t.iid()).from(), ASC);
                        }
                        return iter;
                    }

                    @Override
                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                    ) {
                        return fromVertex.asThing().ins().edge(RELATING, toVertex.asThing()) != null;
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Forward(to, from);
                    }

                }
            }

            public static abstract class RolePlayer extends Thing {

                final int repetition;
                final Set<Label> roleTypes;

                private RolePlayer(
                        ProcedureVertex.Thing from, ProcedureVertex.Thing to, Encoding.Direction.Edge direction,
                        int repetition, Set<Label> roleTypes
                ) {
                    super(from, to, direction, ROLEPLAYER);
                    this.repetition = repetition;
                    this.roleTypes = roleTypes;
                }

                public abstract Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchEdge(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                );

                public abstract boolean isClosure(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params,
                        GraphIterator.Scope withinScope
                );

                @Override
                public Forwardable<? extends ThingVertex, Order.Asc> branch(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                ) {
                    throw TypeDBException.of(ILLEGAL_OPERATION);
                }

                @Override
                public boolean isClosure(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters params
                ) {
                    throw TypeDBException.of(ILLEGAL_OPERATION);
                }

                public Identifier.Variable scope() {
                    if (direction().isForward()) return from.id().asVariable();
                    else return to.id().asVariable();
                }

                private Set<Label> roleTypes() {
                    return roleTypes;
                }

                @Override
                public boolean isRolePlayer() {
                    return true;
                }

                @Override
                public RolePlayer asRolePlayer() {
                    return this;
                }

                @Override
                public boolean equals(Object o) {
                    return super.equals(o) && ((RolePlayer) o).repetition == repetition &&
                            ((RolePlayer) o).roleTypes.equals(roleTypes);
                }

                @Override
                public String toString() {
                    return super.toString() + String.format(" { repetition: %d, roleTypes: %s }", repetition, roleTypes);
                }

                public boolean overlaps(RolePlayer other, Traversal.Parameters params) {
                    assert direction().equals(other.direction());
                    Set<Label> roleTypeIntersection = intersection(roleTypes(), other.roleTypes());
                    Set<Label> playerIntersection = intersection(to().props().types(), other.to().props().types());
                    boolean typesIntersect = !roleTypeIntersection.isEmpty() && !playerIntersection.isEmpty();
                    if (typesIntersect && to().props().hasIID() && other.to().props().hasIID()) {
                        return params.getIID(to().id().asVariable()).equals(params.getIID(other.to().id().asVariable()));
                    } else return typesIntersect;
                }

                static class Forward extends RolePlayer {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int repetition, Set<Label> roleTypes) {
                        super(from, to, FORWARD, repetition, roleTypes);
                    }

                    @Override
                    public Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchEdge(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing() && !roleTypes.isEmpty();
                        ThingVertex rel = fromVertex.asThing();

                        Set<TypeVertex> relationRoleTypes = graphMgr.schema().relatedRoleTypes(rel.type());
                        FunctionalIterator<TypeVertex> instanceRoleTypes = iterate(relationRoleTypes)
                                .filter(rt -> this.roleTypes.contains(rt.properLabel()));
                        if (to.props().hasIID()) {
                            return to.iterateAndFilterPredicatesOnEdges(branchToIID(graphMgr, params, rel, instanceRoleTypes), params, ASC);
                        } else {
                            return to.mergeAndFilterPredicatesOnEdges(graphMgr, branchToTypes(graphMgr, rel, instanceRoleTypes), params, ASC);
                        }
                    }

                    private List<KeyValue<ThingVertex, ThingVertex>> branchToIID(
                            GraphManager graphMgr, Traversal.Parameters params, ThingVertex rel,
                            FunctionalIterator<TypeVertex> roleTypes
                    ) {
                        assert to.id().isVariable();
                        ThingVertex player = graphMgr.data().getReadable(params.getIID(to.id().asVariable()));
                        List<KeyValue<ThingVertex, ThingVertex>> toAndRole = new ArrayList<>();
                        if (player != null) {
                            roleTypes.forEachRemaining(rt ->
                                    rel.outs().edge(ROLEPLAYER, rt, player.iid())
                                            .toAndOptimised().forEachRemaining(toAndRole::add)
                            );
                        }
                        return toAndRole;
                    }

                    private List<Pair<TypeVertex, Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc>>> branchToTypes(
                            GraphManager graphMgr, ThingVertex rel, FunctionalIterator<TypeVertex> roleTypes
                    ) {
                        return roleTypes.flatMap(rt ->
                                iterate(to.props().types())
                                        .map(l -> graphMgr.schema().getType(l))
                                        .filter(t -> graphMgr.schema().playersOfRoleType(rt).contains(t))
                                        .map(t -> new Pair<>(t, rel.outs()
                                                .edge(ROLEPLAYER, rt, PrefixIID.of(t.encoding().instance()), t.iid())
                                                .toAndOptimised())
                                        )
                        ).toList();
                    }

                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                            Traversal.Parameters params, GraphIterator.Scope scope
                    ) {
                        ThingVertex rel = fromVertex.asThing();
                        ThingVertex player = toVertex.asThing();
                        Set<TypeVertex> relationRoleTypes = graphMgr.schema().relatedRoleTypes(rel.type());
                        Set<TypeVertex> roleTypesPlayed = graphMgr.schema().playedRoleTypes(player.type());
                        Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> closures = iterate(relationRoleTypes)
                                .filter(rt -> roleTypes.contains(rt.properLabel()) && roleTypesPlayed.contains(rt))
                                .mergeMapForwardable(
                                        rt -> rel.outs()
                                                .edge(ROLEPLAYER, rt, player.iid().prefix(), player.iid().type())
                                                .toAndOptimised(),
                                        ASC
                                ).filter(kv -> kv.key().equals(player) &&
                                        scope.getRoleEdgeSource(kv.value()).map(source -> !source.equals(this)).orElse(true)
                                );
                        closures.forward(KeyValue.of(player, null));
                        Optional<KeyValue<ThingVertex, ThingVertex>> next = closures.first();
                        if (next.isPresent() && next.get().key().equals(player)) {
                            scope.record(this, next.get().value());
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Backward(to, from, repetition, roleTypes);
                    }
                }

                static class Backward extends RolePlayer {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int repetition, Set<Label> roleTypes) {
                        super(from, to, BACKWARD, repetition, roleTypes);
                    }

                    @Override
                    public Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchEdge(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                    ) {
                        assert fromVertex.isThing() && to.props().predicates().isEmpty() && !roleTypes.isEmpty();
                        ThingVertex player = fromVertex.asThing();
                        Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iter;

                        Set<TypeVertex> roleTypesPlayed = graphMgr.schema().playedRoleTypes(player.type());
                        FunctionalIterator<TypeVertex> roleTypeVertices = iterate(roleTypesPlayed)
                                .filter(rt -> roleTypes.contains(rt.properLabel()));
                        if (to.props().hasIID()) iter = branchToIID(graphMgr, params, player, roleTypeVertices);
                        else iter = branchToTypes(graphMgr, player, roleTypeVertices);
                        return iter;
                    }

                    private Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchToIID(
                            GraphManager graphMgr, Traversal.Parameters params, ThingVertex player,
                            FunctionalIterator<TypeVertex> roleTypeVertices
                    ) {
                        assert to.id().isVariable();
                        ThingVertex relation = graphMgr.data().getReadable(params.getIID(to.id().asVariable()));
                        if (relation == null) return emptySorted();
                        else {
                            Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iter = roleTypeVertices.mergeMapForwardable(
                                    rt -> player.ins()
                                            .edge(ROLEPLAYER, rt, relation.iid().prefix(), relation.iid().type())
                                            .fromAndOptimised(),
                                    ASC
                            ).filter(kv -> kv.key().equals(relation));
                            iter.forward(KeyValue.of(relation, null));
                            return iter;
                        }
                    }

                    private Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchToTypes(
                            GraphManager graphMgr, ThingVertex player, FunctionalIterator<TypeVertex> roleTypeVertices
                    ) {
                        return roleTypeVertices.flatMap(
                                rt -> iterate(to.props().types()).map(l -> graphMgr.schema().getType(l))
                                        .map(t -> player.ins()
                                                .edge(ROLEPLAYER, rt, PrefixIID.of(t.encoding().instance()), t.iid())
                                                .fromAndOptimised()
                                        )).mergeMapForwardable(Function.identity(), ASC);
                    }

                    public boolean isClosure(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                            Traversal.Parameters params, GraphIterator.Scope scope
                    ) {
                        ThingVertex player = fromVertex.asThing();
                        ThingVertex rel = toVertex.asThing();
                        Set<TypeVertex> relationRoleTypes = graphMgr.schema().relatedRoleTypes(rel.type());
                        Set<TypeVertex> roleTypesPlayed = graphMgr.schema().playedRoleTypes(player.type());
                        Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> closures = iterate(relationRoleTypes)
                                .filter(rt -> roleTypes.contains(rt.properLabel()) && roleTypesPlayed.contains(rt))
                                .mergeMapForwardable(
                                        rt -> player.ins().edge(ROLEPLAYER, rt, rel.iid().prefix(), rel.iid().type())
                                                .fromAndOptimised(),
                                        ASC
                                ).filter(kv -> kv.key().equals(rel) &&
                                        scope.getRoleEdgeSource(kv.value()).map(source -> !source.equals(this)).orElse(true)
                                );
                        closures.forward(KeyValue.of(rel, null));

                        Optional<KeyValue<ThingVertex, ThingVertex>> next = closures.first();
                        if (next.isPresent() && next.get().key().equals(rel)) {
                            scope.record(this, next.get().value());
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Forward(to, from, repetition, roleTypes);
                    }
                }
            }
        }
    }
}
