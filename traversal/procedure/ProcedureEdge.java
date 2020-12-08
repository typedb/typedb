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
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.graph.TraversalEdge;
import grakn.core.traversal.planner.PlannerEdge;

import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.single;
import static grakn.core.graph.util.Encoding.Direction.Edge.BACKWARD;
import static grakn.core.graph.util.Encoding.Direction.Edge.FORWARD;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
import static grakn.core.graph.util.Encoding.Edge.Type.PLAYS;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;
import static grakn.core.traversal.procedure.ProcedureVertex.Thing.filterAttributes;
import static java.util.Collections.emptyIterator;

public abstract class ProcedureEdge<VERTEX_FROM extends ProcedureVertex<?, ?>, VERTEX_TO extends ProcedureVertex<?, ?>>
        extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

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

    public abstract ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                        Traversal.Parameters parameters);

    public abstract boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                      Traversal.Parameters parameters);

    public int order() {
        return order;
    }

    public Encoding.Direction.Edge direction() {
        return direction;
    }

    public boolean isClosureEdge() {
        return order() > to().branchEdge().order();
    }

    static class Equal extends ProcedureEdge<ProcedureVertex<?, ?>, ProcedureVertex<?, ?>> {

        private Equal(ProcedureVertex<?, ?> from, ProcedureVertex<?, ?> to, int order, Encoding.Direction.Edge direction) {
            super(from, to, order, direction);
        }

        @Override
        public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                   Traversal.Parameters parameters) {
            return single(fromVertex);
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                 Traversal.Parameters parameters) {
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
        public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                   Traversal.Parameters parameters) {
            assert fromVertex.isThing() && fromVertex.asThing().isAttribute();
            ResourceIterator<AttributeVertex<?>> toIter;

            if (to.props().hasIID()) {
                toIter = to.iterateAndFilterFromIID(graphMgr, parameters)
                        .filter(ThingVertex::isAttribute).map(ThingVertex::asAttribute);
            } else if (!to.props().types().isEmpty()) {
                toIter = to.iterateAndFilterFromTypes(graphMgr, parameters)
                        .filter(ThingVertex::isAttribute).map(ThingVertex::asAttribute);
            } else {
                assert !to.isStartingVertex();
                toIter = iterate(fromVertex.asThing().asAttribute().valueType().comparables())
                        .flatMap(vt -> graphMgr.schema().attributeTypes(vt))
                        .flatMap(at -> graphMgr.data().get(at)).map(ThingVertex::asAttribute);
                if (!to.props().predicates().isEmpty()) {
                    toIter = to.filterPredicates(toIter, parameters).map(ThingVertex::asAttribute);
                }
            }

            return toIter.filter(toVertex -> predicate.apply(fromVertex.asThing().asAttribute(), toVertex));
        }

        @Override
        public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                 Traversal.Parameters parameters) {
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

        static abstract class Isa<VERTEX_ISA_FROM extends ProcedureVertex<?, ?>, VERTEX_ISA_TO extends ProcedureVertex<?, ?>>
                extends Native<VERTEX_ISA_FROM, VERTEX_ISA_TO> {

            final boolean isTransitive;

            private Isa(VERTEX_ISA_FROM from, VERTEX_ISA_TO to, int order, Encoding.Direction.Edge direction, boolean isTransitive) {
                super(from, to, order, direction);
                this.isTransitive = isTransitive;
            }

            ResourceIterator<TypeVertex> isaTypes(GraphManager graphMgr, ThingVertex fromVertex) {
                ResourceIterator<TypeVertex> iterator = single(fromVertex.asThing().type());
                if (isTransitive)
                    iterator = link(list(iterator, graphMgr.schema().superTypes(fromVertex.asThing().type())));
                return iterator;
            }

            static class Forward extends Isa<ProcedureVertex.Thing, ProcedureVertex.Type> {

                private Forward(ProcedureVertex.Thing thing, ProcedureVertex.Type type, int order, boolean isTransitive) {
                    super(thing, type, order, FORWARD, isTransitive);
                }

                @Override
                public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                           Traversal.Parameters parameters) {
                    assert fromVertex.isThing();
                    ResourceIterator<TypeVertex> iterator = isaTypes(graphMgr, fromVertex.asThing());
                    return to.filter(iterator);
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                         Traversal.Parameters parameters) {
                    assert fromVertex.isThing() && toVertex.isType();
                    return isaTypes(graphMgr, fromVertex.asThing()).filter(s -> s.equals(toVertex)).hasNext();
                }
            }

            static class Backward extends Isa<ProcedureVertex.Type, ProcedureVertex.Thing> {

                private Backward(ProcedureVertex.Type type, ProcedureVertex.Thing thing, int order, boolean isTransitive) {
                    super(type, thing, order, BACKWARD, isTransitive);
                }

                @Override
                public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                           Traversal.Parameters parameters) {
                    ResourceIterator<TypeVertex> typeIterator = single(fromVertex.asType());
                    if (isTransitive)
                        typeIterator = link(list(typeIterator, graphMgr.schema().subTypes(fromVertex.asType(), true)));
                    if (!to.props().types().isEmpty())
                        typeIterator = typeIterator.filter(t -> to.props().types().contains(t));
                    ResourceIterator<? extends ThingVertex> iterator = typeIterator.flatMap(t -> graphMgr.data().get(t));
                    if (to.props().hasIID()) iterator = to.filterIID(iterator, parameters);
                    if (!to.props().predicates().isEmpty())
                        iterator = to.filterPredicates(filterAttributes(iterator), parameters);
                    return iterator;
                }

                @Override
                public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                         Traversal.Parameters parameters) {
                    assert fromVertex.isType() && toVertex.isThing();
                    return isaTypes(graphMgr, toVertex.asThing()).filter(s -> s.equals(fromVertex)).hasNext();
                }
            }
        }

        static abstract class Type extends Native<ProcedureVertex.Type, ProcedureVertex.Type> {

            final boolean isTransitive;

            private Type(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, Encoding.Direction.Edge direction, boolean isTransitive) {
                super(from, to, order, direction);
                this.isTransitive = isTransitive;
            }

            static Native.Type of(ProcedureVertex.Type from, ProcedureVertex.Type to, PlannerEdge.Native.Type.Directional edge) {
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

                private Sub(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, Encoding.Direction.Edge direction, boolean isTransitive) {
                    super(from, to, order, direction, isTransitive);
                }

                ResourceIterator<TypeVertex> superTypes(GraphManager graphMgr, Vertex<?, ?> fromVertex) {
                    ResourceIterator<TypeVertex> iterator;
                    if (!isTransitive) iterator = fromVertex.asType().outs().edge(SUB).to();
                    else iterator = graphMgr.schema().superTypes(fromVertex.asType());
                    return iterator;
                }

                static class Forward extends Sub {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, FORWARD, isTransitive);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                               Traversal.Parameters parameters) {
                        ResourceIterator<TypeVertex> iterator = superTypes(graphMgr, fromVertex);
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters parameters) {
                        return superTypes(graphMgr, fromVertex).filter(v -> v.equals(toVertex.asType())).hasNext();
                    }
                }

                static class Backward extends Sub {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, BACKWARD, isTransitive);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                               Traversal.Parameters parameters) {
                        ResourceIterator<TypeVertex> iterator = graphMgr.schema().subTypes(fromVertex.asType(), isTransitive);
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters parameters) {
                        return superTypes(graphMgr, toVertex).filter(v -> v.equals(fromVertex.asType())).hasNext();
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

                    private ResourceIterator<TypeVertex> ownedTypes(TypeVertex fromVertex) {
                        if (isKey) return fromVertex.outs().edge(OWNS_KEY).to();
                        else return link(list(fromVertex.outs().edge(OWNS).to(),
                                              fromVertex.outs().edge(OWNS_KEY).to()));
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                               Traversal.Parameters parameters) {
                        ResourceIterator<TypeVertex> iterator = ownedTypes(fromVertex.asType());
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters parameters) {
                        return ownedTypes(fromVertex.asType()).filter(t -> t.equals(toVertex.asType())).hasNext();
                    }
                }

                static class Backward extends Owns {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isKey) {
                        super(from, to, order, BACKWARD, isKey);
                    }

                    private ResourceIterator<TypeVertex> ownerTypes(TypeVertex fromVertex) {
                        if (isKey) return fromVertex.ins().edge(OWNS_KEY).from();
                        else return link(list(fromVertex.ins().edge(OWNS).from(),
                                              fromVertex.ins().edge(OWNS_KEY).from()));
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex,
                                                                               Traversal.Parameters parameters) {
                        ResourceIterator<TypeVertex> iterator = ownerTypes(fromVertex.asType());
                        return to.filter(iterator);
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex,
                                             Traversal.Parameters parameters) {
                        return ownerTypes(fromVertex.asType()).filter(t -> t.equals(toVertex.asType())).hasNext();
                    }
                }
            }

            static abstract class Plays extends Type {

                private Plays(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, Encoding.Direction.Edge direction) {
                    super(from, to, order, direction, false);
                }

                static class Forward extends Plays {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return fromVertex.asType().outs().edge(PLAYS).to();
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return fromVertex.asType().outs().edge(PLAYS).to().filter(t -> t.equals(toVertex.asType())).hasNext();
                    }
                }

                static class Backward extends Plays {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return fromVertex.asType().ins().edge(PLAYS).from();
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return fromVertex.asType().ins().edge(PLAYS).from().filter(t -> t.equals(toVertex.asType())).hasNext();
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
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }

                static class Backward extends Relates {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false;
                    }
                }
            }
        }

        static abstract class Thing extends Native<ProcedureVertex.Thing, ProcedureVertex.Thing> {

            private Thing(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Encoding.Direction.Edge direction) {
                super(from, to, order, direction);
            }

            static Native.Thing of(ProcedureVertex.Thing from, ProcedureVertex.Thing to, PlannerEdge.Native.Thing.Directional edge) {
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

            static abstract class Has extends Thing {

                private Has(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Encoding.Direction.Edge direction) {
                    super(from, to, order, direction);
                }

                static class Forward extends Has {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }

                static class Backward extends Has {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }
            }

            static abstract class Playing extends Thing {

                private Playing(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Encoding.Direction.Edge direction) {
                    super(from, to, order, direction);
                }

                static class Forward extends Playing {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }

                static class Backward extends Playing {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }
            }

            static abstract class Relating extends Thing {

                private Relating(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Encoding.Direction.Edge direction) {
                    super(from, to, order, direction);
                }

                static class Forward extends Relating {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, FORWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }

                static class Backward extends Relating {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }
            }

            static abstract class RolePlayer extends Thing {

                private final Set<Label> roleTypes;

                private RolePlayer(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Encoding.Direction.Edge direction, Set<Label> roleTypes) {
                    super(from, to, order, direction);
                    this.roleTypes = roleTypes;
                }

                static class Forward extends RolePlayer {

                    private Forward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Set<Label> roleTypes) {
                        super(from, to, order, FORWARD, roleTypes);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }

                static class Backward extends RolePlayer {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Set<Label> roleTypes) {
                        super(from, to, order, BACKWARD, roleTypes);
                    }

                    @Override
                    public ResourceIterator<? extends Vertex<?, ?>> branchFrom(GraphManager graphMgr, Vertex<?, ?> fromVertex, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }

                    @Override
                    public boolean isClosure(GraphManager graphMgr, Vertex<?, ?> fromVertex, Vertex<?, ?> toVertex, Traversal.Parameters parameters) {
                        return false; // TODO
                    }
                }
            }
        }
    }
}
