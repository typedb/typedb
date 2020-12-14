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

package grakn.core.traversal.procedure;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.graph.GraphManager;
import grakn.core.graph.edge.ThingEdge;
import grakn.core.graph.iid.PrefixIID;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.graph.TraversalEdge;
import grakn.core.traversal.planner.PlannerEdge;

import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.iterator.Iterators.empty;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.loop;
import static grakn.core.common.iterator.Iterators.single;
import static grakn.core.common.iterator.Iterators.tree;
import static grakn.core.graph.util.Encoding.Direction.Edge.BACKWARD;
import static grakn.core.graph.util.Encoding.Direction.Edge.FORWARD;
import static grakn.core.graph.util.Encoding.Edge.Thing.HAS;
import static grakn.core.graph.util.Encoding.Edge.Thing.PLAYING;
import static grakn.core.graph.util.Encoding.Edge.Thing.RELATING;
import static grakn.core.graph.util.Encoding.Edge.Thing.ROLEPLAYER;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
import static grakn.core.graph.util.Encoding.Edge.Type.PLAYS;
import static grakn.core.graph.util.Encoding.Edge.Type.RELATES;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;
import static grakn.core.graph.util.Encoding.Prefix.VERTEX_ATTRIBUTE;
import static grakn.core.graph.util.Encoding.Prefix.VERTEX_ROLE;
import static grakn.core.graph.util.Encoding.Vertex.Thing.RELATION;
import static grakn.core.traversal.common.Predicate.Operator.Equality.EQ;

public abstract class ProcedureEdge<
        VERTEX_FROM extends ProcedureVertex<?, ?>,
        VERTEX_TO extends ProcedureVertex<?, ?>> extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    private final int order;
    private final Encoding.Direction.Edge direction;

    private ProcedureEdge(VERTEX_FROM from, VERTEX_TO to, int order, Encoding.Direction.Edge direction) {
        super(from, to);
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
            PlannerEdge.Native.Directional<?, ?> edge = plannerEdge.asNative();
            return Native.of(from, to, edge);
        } else {
            throw GraknException.of(UNRECOGNISED_VALUE);
        }
    }

    public abstract ResourceIterator<? extends Vertex<?, ?>> branchTo(GraphManager graphMgr, Vertex<?, ?> fromVertex,
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
        return order() > to().branchEdge().order();
    }

    public boolean isRolePlayer() { return false; }

    public Native.Thing.RolePlayer asRolePlayer() {
        throw GraknException.of(ILLEGAL_CAST, className(getClass()), className(Native.Thing.RolePlayer.class));
    }

    @Override
    public String toString() {
        return String.format("%s: (%s %s %s)", order, from.id(), direction.isForward() ? "-->" : "<--", to.id());
    }

    static class Equal extends ProcedureEdge<ProcedureVertex<?, ?>, ProcedureVertex<?, ?>> {

        private Equal(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to,
                      int order, Encoding.Direction.Edge direction) {
            super(from, to, order, direction);
        }

        @Override
        public ResourceIterator<? extends Vertex<?, ?>> branchTo(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                 Traversal.Parameters params) {
            return single(fromVertex);
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                 Vertex<?, ?> toVertex, Traversal.Parameters params) {
            assert fromVertex != null && toVertex != null;
            return fromVertex.equals(toVertex);
        }
    }

    static class Predicate extends ProcedureEdge<ProcedureVertex.Thing, ProcedureVertex.Thing> {

        private final grakn.core.traversal.common.Predicate.Variable predicate;

        private Predicate(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                          Encoding.Direction.Edge direction, grakn.core.traversal.common.Predicate.Variable predicate) {
            super(from, to, order, direction);
            this.predicate = direction.isForward() ? predicate : predicate.reflection();
        }

        @Override
        public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                GraphManager graphMgr, Vertex<?, ?> fromVertex,
                Traversal.Parameters params) {
            assert fromVertex.isThing() && fromVertex.asThing().isAttribute();
            ResourceIterator<? extends AttributeVertex<?>> toIter;

            if (to.props().hasIID()) {
                toIter = to.iterateAndFilterFromIID(graphMgr, params)
                        .filter(ThingVertex::isAttribute).map(ThingVertex::asAttribute);
            } else if (!to.props().types().isEmpty()) {
                toIter = to.iterateAndFilterFromTypes(graphMgr, params)
                        .filter(ThingVertex::isAttribute).map(ThingVertex::asAttribute);
            } else {
                assert !to.isStartingVertex();
                toIter = iterate(fromVertex.asThing().asAttribute().valueType().comparables())
                        .flatMap(vt -> graphMgr.schema().attributeTypes(vt))
                        .flatMap(at -> graphMgr.data().get(at)).map(ThingVertex::asAttribute);
                if (!to.props().predicates().isEmpty()) {
                    toIter = to.filterPredicates(toIter, params);
                }
            }

            return toIter.filter(toVertex -> predicate.apply(fromVertex.asThing().asAttribute(), toVertex));
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                 Traversal.Parameters params) {
            assert fromVertex.isThing() && fromVertex.asThing().isAttribute() &&
                    toVertex.isThing() && toVertex.asThing().isAttribute();
            return predicate.apply(fromVertex.asThing().asAttribute(), toVertex.asThing().asAttribute());
        }
    }

    static abstract class Native<
            VERTEX_NATIVE_FROM extends ProcedureVertex<?, ?>,
            VERTEX_NATIVE_TO extends ProcedureVertex<?, ?>
            > extends ProcedureEdge<VERTEX_NATIVE_FROM, VERTEX_NATIVE_TO> {

        private Native(VERTEX_NATIVE_FROM from, VERTEX_NATIVE_TO to, int order, Encoding.Direction.Edge direction) {
            super(from, to, order, direction);
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
                throw GraknException.of(UNRECOGNISED_VALUE);
            }
        }

        static abstract class Isa<
                VERTEX_ISA_FROM extends ProcedureVertex<?, ?>,
                VERTEX_ISA_TO extends ProcedureVertex<?, ?>> extends Native<VERTEX_ISA_FROM, VERTEX_ISA_TO> {

            final boolean isTransitive;

            private Isa(VERTEX_ISA_FROM from, VERTEX_ISA_TO to, int order,
                        Encoding.Direction.Edge direction, boolean isTransitive) {
                super(from, to, order, direction);
                this.isTransitive = isTransitive;
            }

            ResourceIterator<TypeVertex> isaTypes(ThingVertex fromVertex) {
                ResourceIterator<TypeVertex> iterator = single(fromVertex.type());
                if (isTransitive) {
                    Encoding.Vertex.Type encoding = fromVertex.type().encoding();
                    iterator = loop(
                            fromVertex.type(),
                            Objects::nonNull,
                            v -> v.outs().edge(SUB).to().filter(s -> s.encoding().equals(encoding)).firstOrNull()
                    );
                }
                return iterator;
            }

            static class Forward extends Isa<ProcedureVertex.Thing, ProcedureVertex.Type> {

                private Forward(ProcedureVertex.Thing thing, ProcedureVertex.Type type, int order, boolean isTransitive) {
                    super(thing, type, order, FORWARD, isTransitive);
                }

                @Override
                public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex,
                        Traversal.Parameters params) {
                    assert fromVertex.isThing();
                    Set<Label> fromTypes = from.props().types();
                    ResourceIterator<TypeVertex> iter = isaTypes(fromVertex.asThing());
                    if (!fromTypes.isEmpty()) iter = iter.filter(t -> fromTypes.contains(t.properLabel()));
                    return to.filter(iter);
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                         Traversal.Parameters params) {
                    assert fromVertex.isThing() && toVertex.isType();
                    return isaTypes(fromVertex.asThing()).filter(s -> s.equals(toVertex)).hasNext();
                }
            }

            static class Backward extends Isa<ProcedureVertex.Type, ProcedureVertex.Thing> {

                private Backward(ProcedureVertex.Type type, ProcedureVertex.Thing thing, int order, boolean isTransitive) {
                    super(type, thing, order, BACKWARD, isTransitive);
                }

                @Override
                public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                        GraphManager graphMgr, Vertex<?, ?> fromVertex,
                        Traversal.Parameters params) {
                    TypeVertex type = fromVertex.asType();
                    Set<Label> toTypes = to.props().types();
                    ResourceIterator<TypeVertex> typeIter;
                    if (!isTransitive) typeIter = single(type);
                    else typeIter = tree(type, v -> v.ins().edge(SUB).from());
                    if (!toTypes.isEmpty()) typeIter = typeIter.filter(t -> toTypes.contains(t.properLabel()));

                    ResourceIterator<? extends ThingVertex> iter = typeIter.flatMap(t -> graphMgr.data().get(t));
                    if (to.props().hasIID()) iter = to.filterIID(iter, params);
                    if (!to.props().predicates().isEmpty()) iter = to.filterPredicates(iter, params);
                    return iter;
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                         Traversal.Parameters params) {
                    assert fromVertex.isType() && toVertex.isThing();
                    return isaTypes(toVertex.asThing()).filter(s -> s.equals(fromVertex)).hasNext();
                }
            }
        }

        static abstract class Type extends Native<ProcedureVertex.Type, ProcedureVertex.Type> {

            final boolean isTransitive;

            private Type(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                         Encoding.Direction.Edge direction, boolean isTransitive) {
                super(from, to, order, direction);
                this.isTransitive = isTransitive;
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
                    throw GraknException.of(UNRECOGNISED_VALUE);
                }
            }

            static abstract class Sub extends Type {

                private Sub(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                            Encoding.Direction.Edge direction, boolean isTransitive) {
                    super(from, to, order, direction, isTransitive);
                }

                ResourceIterator<TypeVertex> superTypes(TypeVertex type) {
                    ResourceIterator<TypeVertex> iterator;
                    if (!isTransitive) iterator = type.outs().edge(SUB).to();
                    else {
                        iterator = loop(
                                type, Objects::nonNull,
                                v -> v.outs().edge(SUB).to().filter(s -> s.encoding().equals(type.encoding())).firstOrNull()
                        ).filter(t -> !t.equals(type));
                    }

                    return iterator;
                }

                static class Forward extends Sub {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, FORWARD, isTransitive);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        ResourceIterator<TypeVertex> iterator = superTypes(fromVertex.asType());
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return superTypes(fromVertex.asType()).filter(v -> v.equals(toVertex.asType())).hasNext();
                    }
                }

                static class Backward extends Sub {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, BACKWARD, isTransitive);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        ResourceIterator<TypeVertex> iter;
                        TypeVertex type = fromVertex.asType();
                        if (isTransitive) iter = type.ins().edge(SUB).from();
                        else iter = tree(type, t -> t.ins().edge(SUB).from()).filter(t -> t.equals(type));
                        return to.filter(iter);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return superTypes(toVertex.asType()).filter(v -> v.equals(fromVertex.asType())).hasNext();
                    }
                }
            }

            static abstract class Owns extends Type {

                final boolean isKey;

                private Owns(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                             Encoding.Direction.Edge direction, boolean isKey) {
                    super(from, to, order, direction, false);
                    this.isKey = isKey;
                }

                static class Forward extends Owns {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isKey) {
                        super(from, to, order, FORWARD, isKey);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        final ResourceIterator<TypeVertex> iterator;
                        if (isKey) iterator = fromVertex.asType().outs().edge(OWNS_KEY).to();
                        else iterator = link(list(fromVertex.asType().outs().edge(OWNS).to(),
                                                  fromVertex.asType().outs().edge(OWNS_KEY).to()));
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        boolean ownsKey = fromVertex.asType().outs().edge(OWNS_KEY, toVertex.asType()) != null;
                        if (isKey) return ownsKey;
                        else return ownsKey || fromVertex.asType().outs().edge(OWNS, toVertex.asType()) != null;
                    }
                }

                static class Backward extends Owns {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isKey) {
                        super(from, to, order, BACKWARD, isKey);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        final ResourceIterator<TypeVertex> iterator;
                        if (isKey) iterator = fromVertex.asType().ins().edge(OWNS_KEY).from();
                        else iterator = link(list(fromVertex.asType().ins().edge(OWNS).from(),
                                                  fromVertex.asType().ins().edge(OWNS_KEY).from()));
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        boolean isOwnedKey = fromVertex.asType().ins().edge(OWNS_KEY, toVertex.asType()) != null;
                        if (isKey) return isOwnedKey;
                        else return isOwnedKey || fromVertex.asType().ins().edge(OWNS, toVertex.asType()) != null;
                    }
                }
            }

            static abstract class Plays extends Type {

                private Plays(ProcedureVertex.Type from, ProcedureVertex.Type to, int order,
                              Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, false);
                }

                static class Forward extends Plays {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        return to.filter(fromVertex.asType().outs().edge(PLAYS).to());
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asType().outs().edge(PLAYS, toVertex.asType()) != null;
                    }
                }

                static class Backward extends Plays {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        return to.filter(fromVertex.asType().ins().edge(PLAYS).from());
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asType().ins().edge(PLAYS, toVertex.asType()) != null;
                    }
                }
            }

            static abstract class Relates extends Type {

                private Relates(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, false);
                }

                static class Forward extends Relates {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        return to.filter(fromVertex.asType().outs().edge(RELATES).to());
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asType().outs().edge(RELATES, toVertex.asType()) != null;
                    }
                }

                static class Backward extends Relates {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        return to.filter(fromVertex.asType().ins().edge(RELATES).from());
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asType().ins().edge(PLAYS, toVertex.asType()) != null;
                    }
                }
            }
        }

        static abstract class Thing extends Native<ProcedureVertex.Thing, ProcedureVertex.Thing> {

            private Thing(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                          Encoding.Direction.Edge direction) {
                super(from, to, order, direction);
            }

            static Native.Thing of(ProcedureVertex.Thing from, ProcedureVertex.Thing to,
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
                    if (isForward)
                        return new RolePlayer.Forward(from, to, orderNumber, edge.asRolePlayer().roleTypes());
                    else return new RolePlayer.Backward(from, to, orderNumber, edge.asRolePlayer().roleTypes());
                } else {
                    throw GraknException.of(UNRECOGNISED_VALUE);
                }
            }

            ResourceIterator<? extends ThingVertex> backwardBranchToIID(
                    GraphManager graphMgr, ThingVertex fromVertex,
                    Encoding.Edge.Thing encoding, VertexIID.Thing toIID) {
                ThingVertex toVertex = graphMgr.data().get(toIID);
                if (toVertex != null && fromVertex.ins().edge(encoding, toVertex) != null) return single(toVertex);
                else return empty();
            }

            ResourceIterator<? extends Vertex<?, ?>> forwardBranchToRole(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                         Encoding.Edge.Thing encoding) {
                assert !to.props().hasIID() && to.props().predicates().isEmpty();
                ResourceIterator<ThingVertex> iter;
                ThingVertex relation = fromVertex.asThing();
                Set<Label> toTypes = to.props().types();
                if (!toTypes.isEmpty()) {
                    iter = iterate(toTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                            .flatMap(t -> relation.outs().edge(encoding, PrefixIID.of(VERTEX_ROLE), t.iid()).to());
                } else {
                    iter = relation.outs().edge(encoding).to();
                }
                return iter;
            }

            static abstract class Has extends Thing {

                private Has(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                            Encoding.Direction.Edge direction) {
                    super(from, to, order, direction);
                }

                static class Forward extends Has {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        ResourceIterator<? extends AttributeVertex<?>> iter;
                        grakn.core.traversal.common.Predicate.Value<?> eq = null;
                        ThingVertex owner = fromVertex.asThing();
                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            VertexIID.Thing iid = params.getIID(to.id().asVariable());
                            AttributeVertex<?> att;
                            if (!iid.isAttribute()) att = null;
                            else att = graphMgr.data().get(iid.asAttribute());
                            if (att != null && owner.outs().edge(HAS, att) != null) iter = single(att);
                            else return empty();
                        } else if (!to.props().types().isEmpty()) {
                            if ((eq = iterate(to.props().predicates())
                                    .filter(p -> p.operator().equals(EQ)).firstOrNull()) != null) {
                                iter = to.iteratorOfAttributes(graphMgr, params, eq)
                                        .filter(a -> owner.outs().edge(HAS, a) != null);
                            } else {
                                iter = iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                                        .flatMap(t -> owner.outs().edge(HAS, PrefixIID.of(VERTEX_ATTRIBUTE), t.iid()).to())
                                        .map(ThingVertex::asAttribute);
                            }
                        } else {
                            iter = owner.outs().edge(HAS).to().map(ThingVertex::asAttribute);
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

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        assert fromVertex.isThing() && fromVertex.asThing().isAttribute();
                        ResourceIterator<? extends ThingVertex> iter;
                        AttributeVertex<?> att = fromVertex.asThing().asAttribute();

                        if (to.props().hasIID()) {
                            iter = backwardBranchToIID(graphMgr, att, HAS, params.getIID(to.id().asVariable()));
                        } else if (!to.props().types().isEmpty()) {
                            iter = iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                                    .flatMap(t -> att.ins().edge(HAS, PrefixIID.of(t.encoding().instance()), t.iid()).from());
                        } else {
                            iter = att.ins().edge(HAS).from();
                        }

                        if (to.props().predicates().isEmpty()) return iter;
                        else return to.filterPredicates(iter, params);
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
                    super(from, to, order, direction);
                }

                static class Forward extends Playing {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        return forwardBranchToRole(graphMgr, fromVertex, PLAYING);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().outs().edge(PLAYING, toVertex.asThing()) != null;
                    }
                }

                static class Backward extends Playing {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        assert fromVertex.isThing();
                        ThingVertex role = fromVertex.asThing();
                        Set<Label> toTypes = to.props().types();
                        ResourceIterator<? extends ThingVertex> iter;

                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            iter = backwardBranchToIID(graphMgr, role, PLAYING, params.getIID(to.id().asVariable()));
                        } else if (!toTypes.isEmpty()) {
                            iter = iterate(toTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                    .flatMap(t -> role.ins().edge(PLAYING, PrefixIID.of(t.encoding().instance()), t.iid()).from());
                        } else {
                            iter = role.ins().edge(PLAYING).from();
                        }

                        if (to.props().predicates().isEmpty()) return iter;
                        else return to.filterPredicates(iter, params);
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
                    super(from, to, order, direction);
                }

                static class Forward extends Relating {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        return forwardBranchToRole(graphMgr, fromVertex, RELATING);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters params) {
                        return fromVertex.asThing().outs().edge(RELATING, toVertex.asThing()) != null;
                    }
                }

                static class Backward extends Relating {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchTo(
                            GraphManager graphMgr, Vertex<?, ?> fromVertex,
                            Traversal.Parameters params) {
                        assert fromVertex.isThing() && to.props().predicates().isEmpty();
                        ThingVertex role = fromVertex.asThing();
                        Set<Label> toTypes = to.props().types();
                        ResourceIterator<? extends ThingVertex> iter;

                        if (to.props().hasIID()) {
                            assert to.id().isVariable();
                            iter = backwardBranchToIID(graphMgr, role, RELATING, params.getIID(to.id().asVariable()));
                        } else if (!toTypes.isEmpty()) {
                            iter = iterate(toTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                    .flatMap(t -> role.ins().edge(RELATING, PrefixIID.of(RELATION), t.iid()).from());
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
                }
            }

            public static abstract class RolePlayer extends Thing {

                final Set<Label> roleTypes;

                private RolePlayer(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                                   Encoding.Direction.Edge direction, Set<Label> roleTypes) {
                    super(from, to, order, direction);
                    this.roleTypes = roleTypes;
                }

                public abstract ResourceIterator<ThingEdge> branchEdge(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                       Traversal.Parameters params);

                public abstract boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                  Vertex<?, ?> toVertex, Traversal.Parameters params,
                                                  Set<ThingVertex> withinScope);

                @Override
                public ResourceIterator<? extends Vertex<?, ?>> branchTo(GraphManager graphMgr,
                                                                         Vertex<?, ?> fromVertex,
                                                                         Traversal.Parameters params) {
                    throw GraknException.of(ILLEGAL_OPERATION);
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                         Vertex<?, ?> toVertex, Traversal.Parameters params) {
                    throw GraknException.of(ILLEGAL_OPERATION);
                }

                @Override
                public boolean isRolePlayer() { return true; }

                @Override
                public RolePlayer asRolePlayer() { return this; }

                public Identifier.Variable scope() {
                    if (direction().isForward()) return from.id().asVariable();
                    else return to.id().asVariable();
                }

                static class Forward extends RolePlayer {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                                    Set<Label> roleTypes) {
                        super(from, to, order, FORWARD, roleTypes);
                    }

                    @Override
                    public ResourceIterator<ThingEdge> branchEdge(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                  Traversal.Parameters params) {
                        assert fromVertex.isThing();
                        ThingVertex rel = fromVertex.asThing();
                        ResourceIterator<ThingEdge> iter;
                        boolean filteredIID = false, filteredTypes = false;

                        if (!roleTypes.isEmpty()) {
                            if (to.props().hasIID()) {
                                assert to.id().isVariable();
                                filteredIID = true;
                                ThingVertex player = graphMgr.data().get(params.getIID(to.id().asVariable()));
                                if (player == null) return empty();
                                iter = iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).noNulls().flatMap(
                                        rt -> rel.outs().edge(ROLEPLAYER, rt.iid(), player.iid().prefix(), player.iid().type()).get()
                                ).filter(e -> e.to().equals(player));
                            } else if (!to.props().types().isEmpty()) {
                                filteredTypes = true;
                                iter = iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                        .flatMap(rt -> iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                                                .flatMap(t -> rel.outs().edge(ROLEPLAYER, rt.iid(), PrefixIID.of(t.encoding().instance()), t.iid()).get()));
                            } else {
                                iter = iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                        .flatMap(rt -> rel.outs().edge(ROLEPLAYER, rt.iid()).get());
                            }
                        } else {
                            iter = rel.outs().edge(ROLEPLAYER).get();
                        }

                        if (!filteredIID && to.props().hasIID()) iter = to.filterIIDOnEdge(iter, params, true);
                        if (!filteredTypes && !to.props().types().isEmpty()) iter = to.filterTypesOnEdge(iter, true);
                        if (!to.props().predicates().isEmpty()) iter = to.filterPredicatesOnEdge(iter, params, true);
                        return iter;
                    }

                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                             Vertex<?, ?> toVertex, Traversal.Parameters params, Set<ThingVertex> withinScope) {
                        ThingVertex rel = fromVertex.asThing();
                        ThingVertex player = toVertex.asThing();
                        if (!roleTypes.isEmpty()) {
                            return iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).anyMatch(
                                    rt -> rel.outs().edge(ROLEPLAYER, rt.iid(), player.iid().prefix(), player.iid().type()).get()
                                            .anyMatch(e -> e.to().equals(player) && !withinScope.contains(e.optimised().get())));
                        } else {
                            return rel.outs().edge(ROLEPLAYER).get().anyMatch(
                                    e -> e.to().equals(player) && !withinScope.contains(e.optimised().get())
                            );
                        }
                    }
                }

                static class Backward extends RolePlayer {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                                     Set<Label> roleTypes) {
                        super(from, to, order, BACKWARD, roleTypes);
                    }

                    @Override
                    public ResourceIterator<ThingEdge> branchEdge(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                  Traversal.Parameters params) {
                        assert fromVertex.isThing() && to.props().predicates().isEmpty();
                        ThingVertex player = fromVertex.asThing();
                        ResourceIterator<ThingEdge> iter;
                        boolean filteredIID = false, filteredTypes = false;

                        if (!roleTypes.isEmpty()) {
                            if (to.props().hasIID()) {
                                assert to.id().isVariable();
                                filteredIID = true;
                                ThingVertex relation = graphMgr.data().get(params.getIID(to.id().asVariable()));
                                if (relation == null) return empty();
                                iter = iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).noNulls().flatMap(
                                        rt -> player.ins().edge(ROLEPLAYER, rt.iid(), relation.iid().prefix(), relation.iid().type())
                                                .get().filter(r -> r.from().equals(relation)));
                            } else if (!to.props().types().isEmpty()) {
                                filteredTypes = true;
                                iter = iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                        .flatMap(rt -> iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).noNulls()
                                                .flatMap(t -> player.ins().edge(ROLEPLAYER, rt.iid(), PrefixIID.of(t.encoding().instance()), t.iid()).get()));
                            } else {
                                iter = iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).noNulls()
                                        .flatMap(rt -> player.ins().edge(ROLEPLAYER, rt.iid()).get());
                            }
                        } else {
                            iter = player.ins().edge(ROLEPLAYER).get();
                        }

                        if (!filteredIID && to.props().hasIID()) iter = to.filterIIDOnEdge(iter, params, false);
                        if (!filteredTypes && !to.props().types().isEmpty()) iter = to.filterTypesOnEdge(iter, false);
                        return iter;
                    }

                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                             Vertex<?, ?> toVertex, Traversal.Parameters params, Set<ThingVertex> withinScope) {
                        ThingVertex player = fromVertex.asThing();
                        ThingVertex rel = toVertex.asThing();
                        if (!roleTypes.isEmpty()) {
                            return iterate(roleTypes).map(l -> graphMgr.schema().getType(l)).anyMatch(
                                    rt -> player.ins().edge(ROLEPLAYER, rt.iid(), rel.iid().prefix(), rel.iid().type()).get()
                                            .anyMatch(e -> e.from().equals(rel) && !withinScope.contains(e.optimised().get())));
                        } else {
                            return player.ins().edge(ROLEPLAYER).get().anyMatch(
                                    e -> e.from().equals(rel) && !withinScope.contains(e.optimised().get())
                            );
                        }
                    }
                }
            }
        }
    }
}
