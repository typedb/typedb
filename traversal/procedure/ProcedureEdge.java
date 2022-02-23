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

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.TypeGraph;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.PrefixIID;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalEdge;
import com.vaticle.typedb.core.traversal.planner.PlannerEdge;
import com.vaticle.typedb.core.traversal.scanner.GraphIterator;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.iterator.Iterators.loop;
import static com.vaticle.typedb.core.common.iterator.Iterators.single;
import static com.vaticle.typedb.core.common.iterator.Iterators.tree;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.graph.common.Encoding.Direction.Edge.BACKWARD;
import static com.vaticle.typedb.core.graph.common.Encoding.Direction.Edge.FORWARD;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.ISA;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.HAS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.PLAYING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.RELATING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Optimised.ROLEPLAYER;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS_KEY;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.RELATES;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_ATTRIBUTE;
import static com.vaticle.typedb.core.graph.common.Encoding.Prefix.VERTEX_ROLE;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.RELATION;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ROLE_TYPE;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;
import static com.vaticle.typedb.core.traversal.procedure.ProcedureVertex.Thing.filterAttributes;

public abstract class ProcedureEdge<
        VERTEX_FROM extends ProcedureVertex<?, ?>, VERTEX_TO extends ProcedureVertex<?, ?>
        > extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    private final int order;
    private final Encoding.Direction.Edge direction;

    private ProcedureEdge(VERTEX_FROM from, VERTEX_TO to, int order, Encoding.Direction.Edge direction, String symbol) {
        super(from, to, symbol);
        this.order = order;
        this.direction = direction;
    }

    public static ProcedureEdge<?, ?> of(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
                                         PlannerEdge.Directional<?, ?> plannerEdge) {
        int order = plannerEdge.orderNumber();
        Encoding.Direction.Edge dir = plannerEdge.direction();
        if (plannerEdge.isEqual()) {
            return new Equal(from, to, order, dir);
        } else if (plannerEdge.isPredicate()) {
            return new Predicate(from.asThing(), to.asThing(), order, dir, plannerEdge.asPredicate().predicate());
        } else if (plannerEdge.isNative()) {
            return Native.of(from, to, plannerEdge.asNative());
        } else {
            throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    public static ProcedureEdge<?, ?> of(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
                                         StructureEdge<?, ?> structureEdge, int order, boolean isForward) {
        Encoding.Direction.Edge dir = isForward ? FORWARD : BACKWARD;
        if (structureEdge.isEqual()) {
            return new Equal(from, to, order, dir);
        } else if (structureEdge.isPredicate()) {
            return new Predicate(from.asThing(), to.asThing(), order, dir, structureEdge.asPredicate().predicate());
        } else if (structureEdge.isNative()) {
            return Native.of(from, to, structureEdge.asNative(), order, isForward);
        } else {
            throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    public abstract Seekable<? extends Vertex<?, ?>, Order.Asc> branch(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                       Traversal.Parameters params);

    public abstract boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                      Traversal.Parameters params);

    public int order() {
        return order;
    }

    public Encoding.Direction.Edge direction() {
        return direction;
    }

    public boolean isClosureEdge() {
        return to().isStartingVertex() || order() > to().branchEdge().order();
    }

    public boolean onlyStartsFromAttribute() {
        return false;
    }

    public boolean onlyStartsFromRelation() {
        return false;
    }

    public boolean onlyEndsAtRelation() {
        return false;
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

    public boolean isRolePlayer() {
        return false;
    }

    public Native.Thing.RolePlayer asRolePlayer() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Native.Thing.RolePlayer.class));
    }

    @Override
    public String toString() {
        if (direction.isForward()) {
            return String.format("%s: (%s *--[%s]--> %s)", order, from.id(), symbol, to.id());
        } else {
            return String.format("%s: (%s <--[%s]--* %s)", order, from.id(), symbol, to.id());
        }
    }

    public abstract ProcedureEdge<?, ?> reverse();

    public static class Equal extends ProcedureEdge<ProcedureVertex<?, ?>, ProcedureVertex<?, ?>> {

        Equal(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
              int order, Encoding.Direction.Edge direction) {
            super(from, to, order, direction, TypeQLToken.Predicate.Equality.EQ.toString());
        }

        @Override
        public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                  Traversal.Parameters params) {
            if (fromVertex.isThing()) {
                if (to.isType()) return emptySorted();
                else return to.asThing().filter(iterateSorted(ASC, fromVertex.asThing()), params);
            } else if (fromVertex.isType()) {
                if (to.isThing()) return emptySorted();
                else return to.asType().filter(iterateSorted(ASC, fromVertex.asType()));
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                 Vertex<?, ?> toVertex, Traversal.Parameters params) {
            assert fromVertex != null && toVertex != null;
            return fromVertex.equals(toVertex);
        }

        @Override
        public ProcedureEdge<?, ?> reverse() {
            Encoding.Direction.Edge reverseDirection = direction().isForward() ? BACKWARD : FORWARD;
            return new Equal(to, from, order(), reverseDirection);
        }
    }

    static class Predicate extends ProcedureEdge<ProcedureVertex.Thing, ProcedureVertex.Thing> {

        private final com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate;

        private Predicate(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                          Encoding.Direction.Edge direction, com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate) {
            super(from, to, order, direction, predicate.toString());
            this.predicate = predicate;
        }

        @Override
        public boolean onlyStartsFromAttribute() {
            return true;
        }

        @Override
        public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
            assert fromVertex.isThing() && fromVertex.asThing().isAttribute();

            Seekable<? extends ThingVertex, Order.Asc> toIter;
            if (to.props().hasIID()) {
                toIter = to.iterateAndFilterFromIID(graphMgr, params);
            } else if (!to.props().types().isEmpty()) {
                toIter = to.iterateAndFilterFromTypes(graphMgr, params);
            } else {
                // TODO this branch shouldn't exist anymore
                assert !to.isStartingVertex();
                toIter = iterate(fromVertex.asThing().asAttribute().valueType().comparables())
                        .flatMap(vt -> graphMgr.schema().attributeTypes(vt))
                        .mergeMap(ASC, at -> graphMgr.data().getReadable(at));
                if (!to.props().predicates().isEmpty()) {
                    toIter = to.filterPredicates(filterAttributes(toIter), params);
                }
            }

            return toIter.filter(toVertex -> predicate.apply(fromVertex.asThing().asAttribute(), toVertex.asAttribute()));
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                 Traversal.Parameters params) {
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

        private Native(VERTEX_NATIVE_FROM from, VERTEX_NATIVE_TO to,
                       int order, Encoding.Direction.Edge direction, Encoding.Edge encoding) {
            super(from, to, order, direction, encoding.name());
        }

        static Native<?, ?> of(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
                               PlannerEdge.Native.Directional<?, ?> edge) {
            boolean isForward = edge.direction().isForward();
            if (edge.isIsa()) {
                int orderNumber = edge.orderNumber();
                boolean isTransitive = edge.asIsa().isTransitive();
                if (isForward) return new Isa.Forward(from.asThing(), to.asType(), orderNumber, isTransitive);
                else return new Isa.Backward(from.asType(), to.asThing(), orderNumber, isTransitive);
            } else if (edge.isType()) {
                return Native.Type.of(from.asType(), to.asType(), edge.asType());
            } else if (edge.isThing()) {
                return Native.Thing.of(from.asThing(), to.asThing(), edge.asThing());
            } else {
                throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        }

        public static ProcedureEdge<?, ?> of(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
                                             StructureEdge.Native<?, ?> edge, int order, boolean isForward) {
            if (edge.encoding().equals(ISA)) {
                boolean isTransitive = edge.isTransitive();
                if (isForward) return new Isa.Forward(from.asThing(), to.asType(), order, isTransitive);
                else return new Isa.Backward(from.asType(), to.asThing(), order, isTransitive);
            } else if (edge.encoding().isType()) {
                return Native.Type.of(from.asType(), to.asType(), edge, order, isForward);
            } else if (edge.encoding().isThing()) {
                return Native.Thing.of(from.asThing(), to.asThing(), edge, order, isForward);
            } else {
                throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
        }

        static abstract class Isa<
                VERTEX_ISA_FROM extends ProcedureVertex<?, ?>, VERTEX_ISA_TO extends ProcedureVertex<?, ?>
                > extends Native<VERTEX_ISA_FROM, VERTEX_ISA_TO> {

            final boolean isTransitive;

            private Isa(VERTEX_ISA_FROM from, VERTEX_ISA_TO to, int order,
                        Encoding.Direction.Edge direction, boolean isTransitive) {
                super(from, to, order, direction, ISA);
                this.isTransitive = isTransitive;
            }

            Seekable<TypeVertex, Order.Asc> isaTypes(ThingVertex thing) {
                if (!isTransitive) return iterateSorted(ASC, thing.type());
                else {
                    TreeSet<TypeVertex> superTypes = new TreeSet<>();
                    loop(thing.type(), Objects::nonNull, v -> v.outs().edge(SUB).to().firstOrNull()).forEachRemaining(superTypes::add);
                    return iterateSorted(ASC, superTypes);
                }
            }

            @Override
            public String toString() {
                return super.toString() + String.format(" { isTransitive: %s }", isTransitive);
            }

            @Override
            public ProcedureEdge<?, ?> reverse() {
                throw TypeDBException.of(UNSUPPORTED_OPERATION);
            }

            static class Forward extends Isa<ProcedureVertex.Thing, ProcedureVertex.Type> {

                Forward(ProcedureVertex.Thing thing, ProcedureVertex.Type type, int order, boolean isTransitive) {
                    super(thing, type, order, FORWARD, isTransitive);
                }

                @Override
                public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params
                ) {
                    assert fromVertex.isThing();
                    Seekable<TypeVertex, Order.Asc> iter = isaTypes(fromVertex.asThing());
                    return to.filter(iter);
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                         Traversal.Parameters params) {
                    assert fromVertex.isThing() && toVertex.isType();
                    return isaTypes(fromVertex.asThing()).anyMatch(s -> s.equals(toVertex));
                }
            }

            static class Backward extends Isa<ProcedureVertex.Type, ProcedureVertex.Thing> {

                Backward(ProcedureVertex.Type type, ProcedureVertex.Thing thing, int order, boolean isTransitive) {
                    super(type, thing, order, BACKWARD, isTransitive);
                }

                @Override
                public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                    assert fromVertex.isType();
                    TypeVertex type = fromVertex.asType();
                    Set<Label> toTypes = to.props().types();
                    FunctionalIterator<TypeVertex> typeIter;

                    if (!isTransitive) typeIter = single(type);
                    else typeIter = tree(type, v -> v.ins().edge(SUB).from());

                    if (!toTypes.isEmpty()) typeIter = typeIter.filter(t -> toTypes.contains(t.properLabel()));
                    if (to.id().isVariable()) typeIter = typeIter.filter(t -> !t.encoding().equals(ROLE_TYPE));

                    Seekable<? extends ThingVertex, Order.Asc> iter = typeIter.mergeMap(ASC, t -> graphMgr.data().getReadable(t));
                    if (to.props().hasIID()) iter = to.filterIID(iter, params);
                    if (!to.props().predicates().isEmpty()) {
                        iter = to.filterPredicates(filterAttributes(iter), params);
                    }
                    return iter;
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                         Traversal.Parameters params) {
                    assert fromVertex.isType() && toVertex.isThing();
                    return isaTypes(toVertex.asThing()).anyMatch(s -> s.equals(fromVertex));
                }
            }
        }

        public static abstract class Type extends Native<ProcedureVertex.Type, ProcedureVertex.Type> {

            private Type(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                         Encoding.Direction.Edge direction, Encoding.Edge encoding) {
                super(from, to, order, direction, encoding);
            }

            static Native.Type of(ProcedureVertex.Type from, ProcedureVertex.Type to,
                                  PlannerEdge.Native.Type.Directional edge) {
                boolean isForward = edge.direction().isForward();
                boolean isTransitive = edge.isTransitive();
                int orderNumber = edge.orderNumber();

                if (edge.isSub()) {
                    if (isForward) return new Sub.Forward(from, to, orderNumber, isTransitive);
                    else return new Sub.Backward(from, to, orderNumber, isTransitive);
                } else if (edge.isOwns()) {
                    if (isForward) return new Owns.Forward(from, to, orderNumber, edge.asOwns().isKey());
                    else return new Owns.Backward(from, to, orderNumber, edge.asOwns().isKey());
                } else if (edge.isPlays()) {
                    if (isForward) return new Plays.Forward(from, to, orderNumber);
                    else return new Plays.Backward(from, to, orderNumber);
                } else if (edge.isRelates()) {
                    if (isForward) return new Relates.Forward(from, to, orderNumber);
                    else return new Relates.Backward(from, to, orderNumber);
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            static ProcedureEdge<?, ?> of(ProcedureVertex.Type from, ProcedureVertex.Type to,
                                          StructureEdge.Native<?, ?> edge, int order, boolean isForward) {
                switch (edge.encoding().asType()) {
                    case SUB:
                        if (isForward) return new Sub.Forward(from, to, order, edge.isTransitive());
                        else return new Sub.Backward(from, to, order, edge.isTransitive());
                    case OWNS:
                        if (isForward) return new Owns.Forward(from, to, order, false);
                        else return new Owns.Backward(from, to, order, false);
                    case OWNS_KEY:
                        if (isForward) return new Owns.Forward(from, to, order, true);
                        else return new Owns.Backward(from, to, order, true);
                    case PLAYS:
                        if (isForward) return new Plays.Forward(from, to, order);
                        else return new Plays.Backward(from, to, order);
                    case RELATES:
                        if (isForward) return new Relates.Forward(from, to, order);
                        else return new Relates.Backward(from, to, order);
                    default:
                        throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            static abstract class Sub extends Type {

                final boolean isTransitive;

                private Sub(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                            Encoding.Direction.Edge direction, boolean isTransitive) {
                    super(from, to, order, direction, SUB);
                    this.isTransitive = isTransitive;
                }

                Seekable<TypeVertex, Order.Asc> superTypes(TypeVertex type) {
                    if (!isTransitive) return type.outs().edge(SUB).to();
                    else {
                        TreeSet<TypeVertex> superTypes = new TreeSet<>();
                        loop(type, Objects::nonNull, v -> v.outs().edge(SUB).to().firstOrNull()).forEachRemaining(superTypes::add);
                        return iterateSorted(ASC, superTypes);
                    }
                }

                @Override
                public String toString() {
                    return super.toString() + String.format(" { isTransitive: %s }", isTransitive);
                }

                static class Forward extends Sub {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, FORWARD, isTransitive);
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        Seekable<TypeVertex, Order.Asc> iterator = superTypes(fromVertex.asType());
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return superTypes(fromVertex.asType()).anyMatch(v -> v.equals(toVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Sub.Backward(to, from, order(), isTransitive);
                    }
                }

                static class Backward extends Sub {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, BACKWARD, isTransitive);
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isType();
                        Seekable<TypeVertex, Order.Asc> iter;
                        TypeVertex type = fromVertex.asType();
                        if (!isTransitive) iter = type.ins().edge(SUB).from();
                        else {
                            TreeSet<TypeVertex> subtypes = new TreeSet<>();
                            tree(type, t -> t.ins().edge(SUB).from()).forEachRemaining(subtypes::add);
                            iter = iterateSorted(ASC, subtypes);
                        }
                        return to.filter(iter);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return superTypes(toVertex.asType()).anyMatch(v -> v.equals(fromVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Sub.Forward(to, from, order(), isTransitive);
                    }
                }
            }

            static abstract class Owns extends Type {

                final boolean isKey;

                private Owns(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                             Encoding.Direction.Edge direction, boolean isKey) {
                    super(from, to, order, direction, OWNS);
                    this.isKey = isKey;
                }

                @Override
                public String toString() {
                    return super.toString() + String.format(" { isKey: %s }", isKey);
                }

                static class Forward extends Owns {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isKey) {
                        super(from, to, order, FORWARD, isKey);
                    }

                    private Seekable<KeyValue<TypeVertex, TypeVertex>, Order.Asc> ownsAndOverridden(TypeVertex owner) {
                        if (isKey) return owner.outs().edge(OWNS_KEY).toAndOverridden();
                        else {
                            return owner.outs().edge(OWNS).toAndOverridden().merge(owner.outs().edge(OWNS_KEY).toAndOverridden());
                        }
                    }

                    private Seekable<TypeVertex, Order.Asc> ownedAttributeTypes(TypeVertex owner) {
                        Set<TypeVertex> overriddens = new HashSet<>();
                        FunctionalIterator<TypeVertex> supertypes;
                        Seekable<TypeVertex, Order.Asc> iterator;

                        supertypes = loop(owner, Objects::nonNull, o -> o.outs().edge(SUB).to().firstOrNull());
                        iterator = supertypes.mergeMap(ASC, o -> ownsAndOverridden(o).mapSorted(
                                ASC,
                                e -> {
                                    if (e.value() != null) overriddens.add(e.value());
                                    if (!overriddens.contains(e.key())) return e.key();
                                    else return null;
                                },
                                vertex -> KeyValue.of(vertex, null)
                        ).filter(Objects::nonNull));
                        return iterator;
                    }

                    @Override
                    public boolean onlyStartsFromThingType() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isType();
                        return to.filter(ownedAttributeTypes(fromVertex.asType()));
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return ownedAttributeTypes(fromVertex.asType()).anyMatch(at -> at.equals(toVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Owns.Backward(to, from, order(), isKey);
                    }
                }

                static class Backward extends Owns {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isKey) {
                        super(from, to, order, BACKWARD, isKey);
                    }

                    private FunctionalIterator<TypeVertex> overriddens(TypeVertex owner) {
                        if (isKey) return owner.outs().edge(OWNS_KEY).overridden().noNulls();
                        else return link(owner.outs().edge(OWNS).overridden().noNulls(),
                                owner.outs().edge(OWNS_KEY).overridden().noNulls());
                    }

                    private FunctionalIterator<TypeVertex> declaredOwnersOfAttType(TypeVertex attType) {
                        if (isKey) return attType.ins().edge(OWNS_KEY).from();
                        else return link(attType.ins().edge(OWNS).from(), attType.ins().edge(OWNS_KEY).from());
                    }

                    private FunctionalIterator<TypeVertex> ownersOfAttType(TypeVertex attType) {
                        return declaredOwnersOfAttType(attType).flatMap(owner -> tree(owner, o ->
                                o.ins().edge(SUB).from().filter(s -> overriddens(s).noneMatch(ov -> ov.equals(attType)))
                        ));
                    }

                    @Override
                    public boolean onlyStartsFromAttributeType() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isType();
                        // TODO: this should read from type graph cache
                        TreeSet<TypeVertex> ownerTypes = new TreeSet<>();
                        ownersOfAttType(fromVertex.asType()).forEachRemaining(ownerTypes::add);
                        return to.filter(iterateSorted(ASC, ownerTypes));
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return ownersOfAttType(fromVertex.asType()).anyMatch(o -> o.equals(toVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Owns.Forward(to, from, order(), isKey);
                    }
                }
            }

            static abstract class Plays extends Type {

                private Plays(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                              Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, PLAYS);
                }

                static class Forward extends Plays {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    private FunctionalIterator<TypeVertex> playedRoleTypes(TypeVertex player) {
                        Set<TypeVertex> overriddens = new HashSet<>();
                        FunctionalIterator<TypeVertex> supertypes, iterator;

                        supertypes = loop(player, Objects::nonNull, p -> p.outs().edge(SUB).to().firstOrNull());
                        iterator = supertypes.flatMap(s -> s.outs().edge(PLAYS).toAndOverridden().map(e -> {
                            if (e.value() != null) overriddens.add(e.value());
                            if (!overriddens.contains(e.key())) return e.key();
                            else return null;
                        }).noNulls());
                        return iterator;
                    }

                    @Override
                    public boolean onlyStartsFromThingType() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isType();
                        // TODO: this should read from type graph cache
                        TreeSet<TypeVertex> roleTypes = new TreeSet<>();
                        playedRoleTypes(fromVertex.asType()).forEachRemaining(roleTypes::add);
                        return to.filter(iterateSorted(ASC, roleTypes));
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return playedRoleTypes(fromVertex.asType()).anyMatch(rt -> rt.equals(toVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Plays.Backward(to, from, order());
                    }
                }

                static class Backward extends Plays {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    private FunctionalIterator<TypeVertex> playersOfRoleType(TypeVertex roleType) {
                        return roleType.ins().edge(PLAYS).from().flatMap(player -> tree(player, p ->
                                p.ins().edge(SUB).from().filter(s -> s.outs().edge(PLAYS).overridden()
                                        .noNulls().noneMatch(ov -> ov.equals(roleType)))));
                    }

                    @Override
                    public boolean onlyStartsFromRoleType() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isType();
                        // TODO: this should read from type graph cache
                        TreeSet<TypeVertex> playerTypes = new TreeSet<>();
                        playersOfRoleType(fromVertex.asType()).forEachRemaining(playerTypes::add);
                        return to.filter(iterateSorted(ASC, playerTypes));
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return playersOfRoleType(fromVertex.asType()).anyMatch(p -> p.equals(toVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Plays.Forward(to, from, order());
                    }
                }
            }

            static abstract class Relates extends Type {

                private Relates(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                                Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, RELATES);
                }

                static class Forward extends Relates {

                    Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    private FunctionalIterator<TypeVertex> relatedRoleTypes(TypeVertex relation) {
                        Set<TypeVertex> overriddens = new HashSet<>();
                        FunctionalIterator<TypeVertex> supertypes, iterator;

                        supertypes = loop(relation, Objects::nonNull, r -> r.outs().edge(SUB).to().firstOrNull());
                        iterator = supertypes.flatMap(s -> s.outs().edge(RELATES).toAndOverridden().map(e -> {
                            if (e.value() != null) overriddens.add(e.value());
                            if (!overriddens.contains(e.key())) return e.key();
                            else return null;
                        }).noNulls());
                        return iterator;
                    }

                    @Override
                    public boolean onlyStartsFromRelationType() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isType();
                        // TODO: this should read from type graph cache
                        TreeSet<TypeVertex> roleTypes = new TreeSet<>();
                        relatedRoleTypes(fromVertex.asType()).forEachRemaining(roleTypes::add);
                        return to.filter(iterateSorted(ASC, roleTypes));
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return relatedRoleTypes(fromVertex.asType()).anyMatch(rt -> rt.equals(toVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Relates.Backward(to, from, order());
                    }
                }

                static class Backward extends Relates {

                    Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    private FunctionalIterator<TypeVertex> relationsOfRoleType(TypeVertex roleType) {
                        return roleType.ins().edge(RELATES).from().flatMap(relation -> tree(relation, r ->
                                r.ins().edge(SUB).from().filter(s -> s.outs().edge(RELATES).overridden()
                                        .noNulls().noneMatch(ov -> ov.equals(roleType)))));
                    }

                    @Override
                    public boolean onlyStartsFromRoleType() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isType();
                        // TODO: this should read from type graph cache
                        TreeSet<TypeVertex> relations = new TreeSet<>();
                        relationsOfRoleType(fromVertex.asType()).forEachRemaining(relations::add);
                        return to.filter(iterateSorted(ASC, relations));
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return relationsOfRoleType(fromVertex.asType()).anyMatch(rel -> rel.equals(toVertex.asType()));
                    }

                    @Override
                    public ProcedureEdge<?, ?> reverse() {
                        return new Relates.Forward(to, from, order());
                    }
                }
            }
        }

        static abstract class Thing extends Native<ProcedureVertex.Thing, ProcedureVertex.Thing> {

            private Thing(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                          Encoding.Direction.Edge direction, Encoding.Edge encoding) {
                super(from, to, order, direction, encoding);
            }

            static Thing of(ProcedureVertex.Thing from, ProcedureVertex.Thing to,
                            PlannerEdge.Native.Thing.Directional edge) {
                boolean isForward = edge.direction().isForward();
                int orderNumber = edge.orderNumber();

                if (edge.isHas()) {
                    if (isForward) return new Has.Forward(from, to, orderNumber);
                    else return new Has.Backward(from, to, orderNumber);
                } else if (edge.isPlaying()) {
                    if (isForward) return new Playing.Forward(from, to, orderNumber);
                    else return new Playing.Backward(from, to, orderNumber);
                } else if (edge.isRelating()) {
                    if (isForward) return new Relating.Forward(from, to, orderNumber);
                    else return new Relating.Backward(from, to, orderNumber);
                } else if (edge.isRolePlayer()) {
                    PlannerEdge.Native.Thing.RolePlayer.Directional rp = edge.asRolePlayer();
                    if (isForward) return new RolePlayer.Forward(from, to, orderNumber, rp.roleTypes());
                    else return new RolePlayer.Backward(from, to, orderNumber, rp.roleTypes());
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            static ProcedureEdge<?, ?> of(ProcedureVertex.Thing from, ProcedureVertex.Thing to,
                                          StructureEdge.Native<?, ?> edge, int order, boolean isForward) {
                Encoding.Edge.Thing encoding = edge.encoding().asThing();
                if (encoding == HAS) {
                    if (isForward) return new Has.Forward(from, to, order);
                    else return new Has.Backward(from, to, order);
                } else if (encoding == RELATING) {
                    if (isForward) return new Relating.Forward(from, to, order);
                    else return new Relating.Backward(from, to, order);
                } else if (encoding == PLAYING) {
                    if (isForward) return new Playing.Forward(from, to, order);
                    else return new Playing.Backward(from, to, order);
                } else if (encoding == ROLEPLAYER) {
                    if (isForward) return new RolePlayer.Forward(from, to, order, edge.asRolePlayer().types());
                    else return new RolePlayer.Backward(from, to, order, edge.asRolePlayer().types());
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            @Override
            public ProcedureEdge<?, ?> reverse() {
                throw TypeDBException.of(UNSUPPORTED_OPERATION);
            }

            Seekable<ThingVertex, Order.Asc> backwardBranchToIIDFiltered(
                    GraphManager graphMgr, ThingVertex fromVertex,
                    Encoding.Edge.Thing encoding, VertexIID.Thing toIID, Set<Label> allowedToTypes) {
                ThingVertex toVertex = graphMgr.data().getReadable(toIID);
                if (toVertex != null && fromVertex.ins().edge(encoding, toVertex) != null &&
                        (allowedToTypes.isEmpty() || allowedToTypes.contains(toVertex.type().properLabel()))) {
                    return iterateSorted(ASC, toVertex);
                } else {
                    return emptySorted();
                }
            }

            Seekable<? extends Vertex<?, ?>, Order.Asc> forwardBranchToRole(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                            Encoding.Edge.Thing.Base encoding) {
                assert !to.props().hasIID() && to.props().predicates().isEmpty();
                ThingVertex relation = fromVertex.asThing();
                if (!to.props().types().isEmpty()) {
                    return iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                            .mergeMap(ASC, t -> relation.outs().edge(encoding, PrefixIID.of(VERTEX_ROLE), t.iid()).to());
                } else {
                    return relation.outs().edge(encoding).to();
                }
            }

            static abstract class Has extends Thing {

                private Has(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                            Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, HAS);
                }

                static class Forward extends Has {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isThing();
                        Seekable<? extends AttributeVertex<?>, Order.Asc> iter;
                        com.vaticle.typedb.core.traversal.predicate.Predicate.Value<?> eq = null;
                        ThingVertex owner = fromVertex.asThing();
                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            VertexIID.Thing iid = params.getIID(to.id().asVariable());
                            AttributeVertex<?> att;
                            if (!iid.isAttribute()) att = null;
                            else att = graphMgr.data().getReadable(iid.asAttribute());
                            if (att != null && owner.outs().edge(HAS, att) != null &&
                                    (to.props().types().contains(att.type().properLabel()))) {
                                iter = iterateSorted(ASC, att);
                            } else {
                                return emptySorted();
                            }
                        } else if (!to.props().types().isEmpty()) {
                            eq = iterate(to.props().predicates()).filter(p -> p.operator().equals(EQ)).firstOrNull();
                            if (eq != null) {
                                iter = to.iteratorOfAttributesWithTypes(graphMgr, params, eq)
                                        .filter(a -> owner.outs().edge(HAS, a.asAttribute()) != null);
                            } else {
                                iter = iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                                        .mergeMap(
                                                ASC, t -> owner.outs().edge(HAS, PrefixIID.of(VERTEX_ATTRIBUTE), t.iid()).to()
                                        ).mapSorted(ASC, ThingVertex::asAttribute, v -> v);
                            }
                        } else {
                            iter = owner.outs().edge(HAS).to().mapSorted(ASC, ThingVertex::asAttribute, v -> v);
                        }

                        if (to.props().predicates().isEmpty()) return iter;
                        else return to.filterPredicates(iter, params, eq);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().outs().edge(HAS, toVertex.asThing()) != null;
                    }
                }

                static class Backward extends Has {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public boolean onlyStartsFromAttribute() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isThing() && fromVertex.asThing().isAttribute();
                        Seekable<ThingVertex, Order.Asc> iter;
                        AttributeVertex<?> att = fromVertex.asThing().asAttribute();

                        if (to.props().hasIID()) {
                            iter = backwardBranchToIIDFiltered(graphMgr, att, HAS, params.getIID(to.id().asVariable()), to.props().types());
                        } else if (!to.props().types().isEmpty()) {
                            iter = iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                                    .mergeMap(ASC, t -> att.ins().edge(HAS, PrefixIID.of(t.encoding().instance()), t.iid()).from());
                        } else {
                            iter = att.ins().edge(HAS).from();
                        }

                        if (to.props().predicates().isEmpty()) return iter;
                        else return to.filterPredicates(iter.mapSorted(ASC, ThingVertex::asAttribute, v -> v), params);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                             Vertex<?, ?> toVertex, Traversal.Parameters params) {
                        return fromVertex.asThing().ins().edge(HAS, toVertex.asThing()) != null;
                    }
                }
            }

            static abstract class Playing extends Thing {

                private Playing(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                                Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, PLAYING);
                }

                static class Forward extends Playing {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isThing();
                        return forwardBranchToRole(graphMgr, fromVertex, PLAYING);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().outs().edge(PLAYING, toVertex.asThing()) != null;
                    }
                }

                static class Backward extends Playing {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isThing();
                        ThingVertex role = fromVertex.asThing();
                        Set<Label> toTypes = to.props().types();
                        Seekable<ThingVertex, Order.Asc> iter;

                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            iter = backwardBranchToIIDFiltered(graphMgr, role, PLAYING, params.getIID(to.id().asVariable()), toTypes);
                        } else if (!toTypes.isEmpty()) {
                            iter = iterate(toTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                    .mergeMap(ASC, t -> role.ins().edge(PLAYING, PrefixIID.of(t.encoding().instance()), t.iid()).from());
                        } else {
                            iter = role.ins().edge(PLAYING).from();
                        }

                        if (to.props().predicates().isEmpty()) return iter;
                        else return to.filterPredicates(iter.mapSorted(ASC, ThingVertex::asAttribute, v -> v), params);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().ins().edge(PLAYING, toVertex.asThing()) != null;
                    }
                }
            }

            static abstract class Relating extends Thing {

                private Relating(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                                 Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, RELATING);
                }

                static class Forward extends Relating {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public boolean onlyStartsFromRelation() {
                        return true;
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isThing();
                        return forwardBranchToRole(graphMgr, fromVertex, RELATING);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().outs().edge(RELATING, toVertex.asThing()) != null;
                    }
                }

                static class Backward extends Relating {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters params) {
                        assert fromVertex.isThing() && to.props().predicates().isEmpty();
                        ThingVertex role = fromVertex.asThing();
                        Set<Label> toTypes = to.props().types();
                        Seekable<? extends ThingVertex, Order.Asc> iter;

                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            iter = backwardBranchToIIDFiltered(graphMgr, role, RELATING, params.getIID(to.id().asVariable()), toTypes);
                        } else if (!toTypes.isEmpty()) {
                            iter = iterate(toTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                    .mergeMap(ASC, t -> role.ins().edge(RELATING, PrefixIID.of(RELATION), t.iid()).from());
                        } else {
                            iter = role.ins().edge(RELATING).from();
                        }
                        return iter;
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().ins().edge(RELATING, toVertex.asThing()) != null;
                    }

                    @Override
                    public boolean onlyEndsAtRelation() {
                        return true;
                    }
                }
            }

            public static abstract class RolePlayer extends Thing {

                final Set<Label> roleTypes;
                Set<TypeVertex> resolvedRoleTypes;

                private RolePlayer(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                                   Encoding.Direction.Edge direction, Set<Label> roleTypes) {
                    super(from, to, order, direction, ROLEPLAYER);
                    this.roleTypes = roleTypes;
                }

                Set<TypeVertex> resolvedRoleTypes(TypeGraph graph) {
                    // TODO: a duplicate of this code exists in PlannerEdge.Native.Thing.RolePlayer,
                    //       which is another indicator that we should:
                    // TODO: Merge PlannerVertex, PlannerEdge, ProcedureVertex, and ProcedureEdge into some
                    //       Vertex and Edge data structure (in `//traversal/fragment`) that aggregate their
                    //       'planner' and 'procedure' logic for each class following the variable //pattern data structure.
                    if (resolvedRoleTypes == null) {
                        resolvedRoleTypes = iterate(roleTypes).map(graph::getType)
                                .flatMap(rt -> tree(rt, r -> r.ins().edge(SUB).from())).toSet();
                    }
                    return resolvedRoleTypes;
                }

                public abstract Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchEdge(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                                                   Traversal.Parameters params);

                public abstract boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                  Vertex<?, ?> toVertex, Traversal.Parameters params,
                                                  GraphIterator.Scopes.Scoped withinScope);

                @Override
                public Seekable<? extends Vertex<?, ?>, Order.Asc> branch(GraphManager graphMgr,
                                                                          Vertex<?, ?> fromVertex,
                                                                          Traversal.Parameters params
                ) {
                    throw TypeDBException.of(ILLEGAL_OPERATION);
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                         Vertex<?, ?> toVertex, Traversal.Parameters params) {
                    throw TypeDBException.of(ILLEGAL_OPERATION);
                }

                @Override
                public boolean isRolePlayer() {
                    return true;
                }

                @Override
                public RolePlayer asRolePlayer() {
                    return this;
                }

                public Identifier.Variable scope() {
                    if (direction().isForward()) return from.id().asVariable();
                    else return to.id().asVariable();
                }

                @Override
                public String toString() {
                    return super.toString() + String.format(" { roleTypes: %s }", roleTypes);
                }

                static class Forward extends RolePlayer {

                    Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Set<Label> roleTypes) {
                        super(from, to, order, FORWARD, roleTypes);
                    }

                    @Override
                    public boolean onlyStartsFromRelation() {
                        return true;
                    }

                    @Override
                    public Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchEdge(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                                              Traversal.Parameters params) {
                        assert fromVertex.isThing();
                        ThingVertex rel = fromVertex.asThing();
                        Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iter;
                        boolean filteredIID = false, filteredTypes = false;

                        if (!roleTypes.isEmpty()) {
                            FunctionalIterator<TypeVertex> resolveRoleTypesIter = iterate(resolvedRoleTypes(graphMgr.schema()));
                            if (to.props().hasIID()) {
                                assert to.id().isVariable();
                                filteredIID = true;
                                ThingVertex player = graphMgr.data().getReadable(params.getIID(to.id().asVariable()));
                                if (player == null) return emptySorted();
                                // TODO we should use the exact type of the current vertex to restrict the set of role types possible
                                iter = resolveRoleTypesIter.mergeMap(
                                        ASC,
                                        rt -> rel.outs()
                                                .edge(ROLEPLAYER, rt, player.iid().prefix(), player.iid().type())
                                                .toAndOptimised()
                                ).filter(e -> e.key().equals(player));
                            } else if (!to.props().types().isEmpty()) {
                                filteredTypes = true;
                                iter = resolveRoleTypesIter.mergeMap(
                                        ASC,
                                        rt -> iterate(to.props().types())
                                                .map(l -> graphMgr.schema().getType(l)).noNulls()
                                                .mergeMap(
                                                        ASC,
                                                        t -> rel.outs()
                                                                .edge(ROLEPLAYER, rt, PrefixIID.of(t.encoding().instance()), t.iid())
                                                                .toAndOptimised()
                                                )
                                );
                            } else {
                                iter = resolveRoleTypesIter.mergeMap(ASC, rt -> rel.outs().edge(ROLEPLAYER, rt).toAndOptimised());
                            }
                        } else {
                            throw TypeDBException.of(ILLEGAL_STATE);
                        }

                        if (!filteredIID && to.props().hasIID()) iter = to.filterIIDOnPlayerAndRole(iter, params);
                        if (!filteredTypes && !to.props().types().isEmpty()) iter = to.filterTypesOnEdge(iter);
                        if (!to.props().predicates().isEmpty()) iter = to.filterPredicatesOnEdge(iter, params);
                        return iter;
                    }

                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params, GraphIterator.Scopes.Scoped scoped) {
                        ThingVertex rel = fromVertex.asThing();
                        ThingVertex player = toVertex.asThing();
                        Optional<KeyValue<ThingVertex, ThingVertex>> valid;
                        if (!roleTypes.isEmpty()) {
                            valid = iterate(resolvedRoleTypes(graphMgr.schema())).mergeMap(
                                    ASC,
                                    rt -> rel.outs()
                                            .edge(ROLEPLAYER, rt, player.iid().prefix(), player.iid().type())
                                            .toAndOptimised()
                                            .filter(kv -> kv.key().equals(player) && !scoped.contains(kv.value()))
                            ).first();
                        } else {
                            throw TypeDBException.of(ILLEGAL_STATE);
                        }
                        valid.ifPresent(kv -> scoped.push(kv.value(), order()));
                        return valid.isPresent();
                    }
                }

                static class Backward extends RolePlayer {

                    Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Set<Label> roleTypes) {
                        super(from, to, order, BACKWARD, roleTypes);
                    }

                    @Override
                    public Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> branchEdge(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                                              Traversal.Parameters params) {
                        assert fromVertex.isThing() && to.props().predicates().isEmpty();
                        ThingVertex player = fromVertex.asThing();
                        Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iter;
                        boolean filteredIID = false, filteredTypes = false;

                        if (!roleTypes.isEmpty()) {
                            FunctionalIterator<TypeVertex> resolveRoleTypesIter = iterate(resolvedRoleTypes(graphMgr.schema()));
                            if (to.props().hasIID()) {
                                assert to.id().isVariable();
                                filteredIID = true;
                                ThingVertex relation = graphMgr.data().getReadable(params.getIID(to.id().asVariable()));
                                if (relation == null) return emptySorted();
                                iter = resolveRoleTypesIter.mergeMap(
                                        ASC,
                                        rt -> player.ins()
                                                .edge(ROLEPLAYER, rt, relation.iid().prefix(), relation.iid().type())
                                                .fromAndOptimised().filter(r -> r.key().equals(relation))
                                );
                            } else if (!to.props().types().isEmpty()) {
                                filteredTypes = true;
                                iter = resolveRoleTypesIter.mergeMap(
                                        ASC,
                                        rt -> iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                                                .mergeMap(ASC, t -> player.ins()
                                                        .edge(ROLEPLAYER, rt, PrefixIID.of(t.encoding().instance()), t.iid())
                                                        .fromAndOptimised())
                                );
                            } else {
                                iter = resolveRoleTypesIter.mergeMap(
                                        ASC,
                                        rt -> player.ins().edge(ROLEPLAYER, rt).fromAndOptimised()
                                );
                            }
                        } else {
                            throw TypeDBException.of(ILLEGAL_STATE);
                        }

                        if (!filteredIID && to.props().hasIID()) iter = to.filterIIDOnPlayerAndRole(iter, params);
                        if (!filteredTypes && !to.props().types().isEmpty()) iter = to.filterTypesOnEdge(iter);
                        return iter;
                    }

                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params, GraphIterator.Scopes.Scoped scoped) {
                        ThingVertex player = fromVertex.asThing();
                        ThingVertex rel = toVertex.asThing();
                        Optional<KeyValue<ThingVertex, ThingVertex>> valid;
                        if (!roleTypes.isEmpty()) {
                            valid = iterate(resolvedRoleTypes(graphMgr.schema())).flatMap(
                                    rt -> player.ins().edge(ROLEPLAYER, rt, rel.iid().prefix(), rel.iid().type())
                                            .fromAndOptimised()
                                            .filter(kv -> kv.key().equals(rel) && !scoped.contains(kv.value())))
                                    .first();
                        } else {
                            throw TypeDBException.of(ILLEGAL_STATE);
                        }
                        valid.ifPresent(kv -> scoped.push(kv.value(), order()));
                        return valid.isPresent();
                    }

                    @Override
                    public boolean onlyEndsAtRelation() {
                        return true;
                    }
                }
            }
        }
    }
}
