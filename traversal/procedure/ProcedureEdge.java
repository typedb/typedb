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
import grakn.core.graph.util.Encoding;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.graph.TraversalEdge;
import grakn.core.traversal.planner.PlannerEdge;
import graql.lang.common.GraqlToken;

import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.graph.util.Encoding.Direction.Edge.BACKWARD;
import static grakn.core.graph.util.Encoding.Direction.Edge.FORWARD;
import static java.util.Collections.emptyIterator;

abstract class ProcedureEdge<VERTEX_FROM extends ProcedureVertex<?>, VERTEX_TO extends ProcedureVertex<?>>
        extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    private final int order;
    private final Encoding.Direction.Edge direction;

    private ProcedureEdge(VERTEX_FROM from, VERTEX_TO to, int order, Encoding.Direction.Edge direction) {
        super(from, to);
        this.order = order;
        this.direction = direction;
    }

    public static ProcedureEdge<?, ?> of(ProcedureVertex<?> from, ProcedureVertex<?> to, PlannerEdge.Directional<?, ?> plannerEdge) {
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

    abstract ResourceIterator<VERTEX_TO> execute(VERTEX_FROM from, Traversal.Parameters parameters);

    public int order() {
        return order;
    }

    public Encoding.Direction.Edge direction() {
        return direction;
    }

    static class Equal extends ProcedureEdge<ProcedureVertex<?>, ProcedureVertex<?>> {

        private Equal(ProcedureVertex<?> from, ProcedureVertex<?> to, int order, Encoding.Direction.Edge direction) {
            super(from, to, order, direction);
        }

        @Override
        ResourceIterator<ProcedureVertex<?>> execute(ProcedureVertex<?> procedureVertex, Traversal.Parameters parameters) {
            return iterate(emptyIterator()); // TODO
        }
    }

    static class Predicate extends ProcedureEdge<ProcedureVertex.Thing, ProcedureVertex.Thing> {

        private final GraqlToken.Predicate.Equality predicate;

        private Predicate(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order,
                          Encoding.Direction.Edge direction, GraqlToken.Predicate.Equality predicate) {
            super(from, to, order, direction);
            this.predicate = predicate;
        }

        @Override
        ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing from, Traversal.Parameters parameters) {
            return iterate(emptyIterator()); // TODO
        }
    }

    static abstract class Native<VERTEX_NATIVE_FROM extends ProcedureVertex<?>, VERTEX_NATIVE_TO extends ProcedureVertex<?>>
            extends ProcedureEdge<VERTEX_NATIVE_FROM, VERTEX_NATIVE_TO> {

        private Native(VERTEX_NATIVE_FROM from, VERTEX_NATIVE_TO to, int order, Encoding.Direction.Edge direction) {
            super(from, to, order, direction);
        }

        static Native<?, ?> of(ProcedureVertex<?> from, ProcedureVertex<?> to, PlannerEdge.Native.Directional<?, ?> edge) {
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

        static abstract class Isa<VERTEX_ISA_FROM extends ProcedureVertex<?>, VERTEX_ISA_TO extends ProcedureVertex<?>>
                extends Native<VERTEX_ISA_FROM, VERTEX_ISA_TO> {

            private final boolean isTransitive;

            private Isa(VERTEX_ISA_FROM vertex_isa_from, VERTEX_ISA_TO vertex_isa_to, int order, Encoding.Direction.Edge direction, boolean isTransitive) {
                super(vertex_isa_from, vertex_isa_to, order, direction);
                this.isTransitive = isTransitive;
            }

            static class Forward extends Isa<ProcedureVertex.Thing, ProcedureVertex.Type> {

                private Forward(ProcedureVertex.Thing thing, ProcedureVertex.Type type, int order, boolean isTransitive) {
                    super(thing, type, order, FORWARD, isTransitive);
                }

                @Override
                ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                    return iterate(emptyIterator()); // TODO
                }
            }

            static class Backward extends Isa<ProcedureVertex.Type, ProcedureVertex.Thing> {

                private Backward(ProcedureVertex.Type type, ProcedureVertex.Thing thing, int order, boolean isTransitive) {
                    super(type, thing, order, BACKWARD, isTransitive);
                }

                @Override
                ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                    return iterate(emptyIterator()); // TODO
                }
            }
        }

        static abstract class Type extends Native<ProcedureVertex.Type, ProcedureVertex.Type> {

            private final boolean isTransitive;

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

                static class Forward extends Sub {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, FORWARD, isTransitive);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends Sub {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isTransitive) {
                        super(from, to, order, BACKWARD, isTransitive);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }
            }

            static abstract class Owns extends Type {

                private final boolean isKey;

                private Owns(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, Encoding.Direction.Edge direction, boolean isKey) {
                    super(from, to, order, direction, false);
                    this.isKey = isKey;
                }

                static class Forward extends Owns {

                    private Forward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isKey) {
                        super(from, to, order, FORWARD, isKey);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends Owns {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order, boolean isKey) {
                        super(from, to, order, BACKWARD, isKey);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
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
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends Plays {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
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
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends Relates {

                    private Backward(ProcedureVertex.Type from, ProcedureVertex.Type to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Type> execute(ProcedureVertex.Type type, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
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
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends Has {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
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
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends Playing {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
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
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends Relating {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order) {
                        super(from, to, order, BACKWARD);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
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
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }

                static class Backward extends RolePlayer {

                    private Backward(ProcedureVertex.Thing from, ProcedureVertex.Thing to, int order, Set<Label> roleTypes) {
                        super(from, to, order, BACKWARD, roleTypes);
                    }

                    @Override
                    ResourceIterator<ProcedureVertex.Thing> execute(ProcedureVertex.Thing thing, Traversal.Parameters parameters) {
                        return iterate(emptyIterator()); // TODO
                    }
                }
            }
        }
    }
}
