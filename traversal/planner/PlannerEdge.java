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

package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.optimiser.OptimiserConstraint;
import com.vaticle.typedb.core.common.optimiser.OptimiserVariable;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.traversal.graph.TraversalEdge;
import com.vaticle.typedb.core.traversal.predicate.PredicateOperator;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
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
import static com.vaticle.typedb.core.traversal.planner.GraphPlanner.INIT_ZERO;

public abstract class PlannerEdge<VERTEX_FROM extends PlannerVertex<?>, VERTEX_TO extends PlannerVertex<?>>
        extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    protected final GraphPlanner planner;
    protected Directional<VERTEX_FROM, VERTEX_TO> forward;
    protected Directional<VERTEX_TO, VERTEX_FROM> backward;

    PlannerEdge(VERTEX_FROM from, VERTEX_TO to, String symbol) {
        this(from, to, symbol, true);
    }

    PlannerEdge(VERTEX_FROM from, VERTEX_TO to, String symbol, boolean initialise) {
        super(from, to, symbol);
        this.planner = from.planner;
        assert Objects.equals(this.planner, to.planner);
        if (initialise) initialiseDirectionalEdges();
    }

    protected abstract void initialiseDirectionalEdges();

    static PlannerEdge<?, ?> of(PlannerVertex<?> from, PlannerVertex<?> to, StructureEdge<?, ?> edge) {
        if (edge.isEqual()) return new PlannerEdge.Equal(from, to);
        else if (edge.isPredicate()) return new Predicate(from.asThing(), to.asThing(), edge.asPredicate().predicate());
        else if (edge.isNative()) return PlannerEdge.Native.of(from, to, edge.asNative());
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    Directional<VERTEX_FROM, VERTEX_TO> forward() {
        return forward;
    }

    Directional<VERTEX_TO, VERTEX_FROM> backward() {
        return backward;
    }

    void initialiseVariables() {
        forward.opposite(backward);
        backward.opposite(forward);
        forward.initialiseVariables();
        backward.initialiseVariables();
    }

    void initialiseConstraints() {
        String conPrefix = "edge_con_" + this.toString() + "_";
        OptimiserConstraint conOneDirection = planner.optimiser().constraint(1, 1, conPrefix + "one_direction");
        conOneDirection.setCoefficient(forward.varIsSelected, 1);
        conOneDirection.setCoefficient(backward.varIsSelected, 1);

        forward.initialiseConstraints();
        backward.initialiseConstraints();
    }

    void updateObjective(GraphManager graphMgr) {
        forward.updateObjective(graphMgr);
        backward.updateObjective(graphMgr);
    }

    void recordCost() {
        forward.recordCost();
        backward.recordCost();
    }

    void resetInitialValue() {
        forward.resetInitialValue();
        backward.resetInitialValue();
    }

    @Override
    public String toString() {
        return String.format("(%s T[%s]H %s)", from.id(), symbol, to.id());
    }

    public static abstract class Directional<VERTEX_DIR_FROM extends PlannerVertex<?>, VERTEX_DIR_TO extends PlannerVertex<?>>
            extends TraversalEdge<VERTEX_DIR_FROM, VERTEX_DIR_TO> {

        OptimiserVariable.Boolean varIsSelected;
        OptimiserVariable.Boolean[] varOrderAssignment;
        private OptimiserVariable.Integer varOrderNumber;
        private final String varPrefix;
        private final String conPrefix;
        private final GraphPlanner planner;
        private final Encoding.Direction.Edge direction;
        private boolean hasInitialValue;
        private boolean isInitialisedVariables;
        private boolean isInitialisedConstraints;
        private double costNext;
        double costLastRecorded;

        private Directional<VERTEX_DIR_TO, VERTEX_DIR_FROM> opposite;

        Directional(VERTEX_DIR_FROM from, VERTEX_DIR_TO to, Encoding.Direction.Edge direction, String symbol) {
            super(from, to, symbol);
            this.planner = from.planner;
            this.direction = direction;
            this.costLastRecorded = 0.01; // non-zero value for safe division
            this.isInitialisedVariables = false;
            this.isInitialisedConstraints = false;
            this.varPrefix = "edge_var_" + this.toString() + "_";
            this.conPrefix = "edge_con_" + this.toString() + "_";
        }

        abstract void updateObjective(GraphManager graphMgr);

        public boolean isSelected() {
            return varIsSelected.solutionValue();
        }

        public int orderNumber() {
            return varOrderNumber.solutionValue();
        }

        public Encoding.Direction.Edge direction() {
            return direction;
        }

        public boolean isInitialisedVariables() {
            return isInitialisedVariables;
        }

        public boolean isInitialisedConstraints() {
            return isInitialisedConstraints;
        }

        void opposite(Directional<VERTEX_DIR_TO, VERTEX_DIR_FROM> opposite) {
            this.opposite = opposite;
        }

        void initialiseVariables() {
            varIsSelected = planner.optimiser().booleanVar(varPrefix + "is_selected");
            varOrderNumber = planner.optimiser().intVar(0, planner.edges().size(), varPrefix + "order_number");
            varOrderAssignment = new OptimiserVariable.Boolean[planner.edges().size()];
            for (int i = 0; i < planner.edges().size(); i++) {
                varOrderAssignment[i] = planner.optimiser().booleanVar(varPrefix + "order_assignment[" + i + "]");
            }
            isInitialisedVariables = true;
        }

        void initialiseConstraints() {
            assert from.isInitialisedVariables();
            assert to.isInitialisedVariables();
            initialiseConstraintsForOrderNumber();
            initialiseConstraintsForOrderSequence();
            isInitialisedConstraints = true;
        }

        private void initialiseConstraintsForOrderNumber() {
            OptimiserConstraint conOrderIfSelected = planner.optimiser().constraint(0, 0, conPrefix + "order_if_selected");
            conOrderIfSelected.setCoefficient(varIsSelected, -1);

            OptimiserConstraint conAssignOrderNumber = planner.optimiser().constraint(0, 0, conPrefix + "assign_order_number");
            conAssignOrderNumber.setCoefficient(varOrderNumber, -1);

            for (int i = 0; i < planner.edges().size(); i++) {
                conOrderIfSelected.setCoefficient(varOrderAssignment[i], 1);
                conAssignOrderNumber.setCoefficient(varOrderAssignment[i], i + 1);
            }
        }

        private void initialiseConstraintsForOrderSequence() {
            Set<Directional<?, ?>> previousEdges = iterate(from.ins()).filter(
                    edge -> !edge.isSelfClosure() && !edge.equals(this.opposite)
            ).toSet();
            int i = 0;
            for (Directional<?, ?> previousEdge : previousEdges) {
                String name = conPrefix + "order_sequence_" + i++;
                OptimiserConstraint conOrderSequence = planner.optimiser().constraint(0, planner.edges().size() + 1, name);
                conOrderSequence.setCoefficient(this.varOrderNumber, 1);
                conOrderSequence.setCoefficient(this.opposite.varIsSelected, planner.edges().size() + 1);
                conOrderSequence.setCoefficient(previousEdge.varOrderNumber, -1);
                conOrderSequence.setCoefficient(previousEdge.varIsSelected, -1);
            }
        }

        protected boolean isSelfClosure() {
            return from.equals(to);
        }

        protected void setObjectiveCoefficient(double cost) {
            assert !Double.isNaN(cost);
            if (cost < INIT_ZERO) cost = INIT_ZERO;
            int expMultiplier = planner.edges().size() - 1;
            for (int i = 0; i < planner.edges().size(); i++) {
                double exp = 1 + (expMultiplier-- * planner.costExponentUnit);
                double coeff = cost * Math.pow(planner.branchingFactor, exp);
                planner.optimiser().setObjectiveCoefficient(varOrderAssignment[i], coeff);
            }
            costNext = cost;
            planner.updateCostNext(costLastRecorded, costNext);
        }

        private void recordCost() {
            costLastRecorded = costNext;
        }

        private void resetInitialValue() {
            varIsSelected.clearInitial();
            varOrderNumber.clearInitial();
            iterate(varOrderAssignment).forEachRemaining(OptimiserVariable.Boolean::clearInitial);
            hasInitialValue = false;
        }

        void setInitialValue(int order) {
            assert order > 0;
            varOrderNumber.setInitial(order);
            varIsSelected.setInitial(true);
            for (int i = 0; i < varOrderAssignment.length; i++) {
                if (i == order - 1) varOrderAssignment[i].setInitial(true);
                else varOrderAssignment[i].setInitial(false);
            }
            hasInitialValue = true;
            opposite.setInitialUnselected();
        }

        public void setInitialUnselected() {
            varIsSelected.setInitial(false);
            varOrderNumber.setInitial(0); // irrelevant
            iterate(varOrderAssignment).forEachRemaining(var -> var.setInitial(false));
            hasInitialValue = true;
        }

        boolean hasInitialValue() {
            return hasInitialValue;
        }

        public boolean isEqual() {
            return false;
        }

        public boolean isPredicate() {
            return false;
        }

        public boolean isNative() {
            return false;
        }

        public Equal.Directional asEqual() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Equal.Directional.class));
        }

        public Predicate.Directional asPredicate() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Predicate.Directional.class));
        }

        public Native.Directional<?, ?> asNative() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Native.Directional.class));
        }

        @Override
        public String toString() {
            if (direction.isForward()) return String.format("(%s T[%s]H %s)", from.id(), symbol, to.id());
            else return String.format("(%s H[%s]T %s)", from.id(), symbol, to.id());
        }

    }

    public static class Equal extends PlannerEdge<PlannerVertex<?>, PlannerVertex<?>> {

        Equal(PlannerVertex<?> from, PlannerVertex<?> to) {
            super(from, to, TypeQLToken.Predicate.Equality.EQ.toString());
        }

        @Override
        protected void initialiseDirectionalEdges() {
            forward = new Directional(from, to, FORWARD);
            backward = new Directional(to, from, BACKWARD);
        }

        public static class Directional extends PlannerEdge.Directional<PlannerVertex<?>, PlannerVertex<?>> {

            Directional(PlannerVertex<?> from, PlannerVertex<?> to, Encoding.Direction.Edge direction) {
                super(from, to, direction, TypeQLToken.Predicate.Equality.EQ.toString());
            }

            @Override
            void updateObjective(GraphManager graphMgr) {
                setObjectiveCoefficient(0);
            }

            @Override
            public boolean isEqual() {
                return true;
            }

            @Override
            public Equal.Directional asEqual() {
                return this;
            }
        }
    }

    public static class Predicate extends PlannerEdge<PlannerVertex.Thing, PlannerVertex.Thing> {

        private final com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate;

        Predicate(PlannerVertex.Thing from, PlannerVertex.Thing to,
                  com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate) {
            super(from, to, predicate.toString(), false);
            this.predicate = predicate;
            initialiseDirectionalEdges();
        }

        @Override
        protected void initialiseDirectionalEdges() {
            forward = new Directional(from.asThing(), to.asThing(), FORWARD, predicate);
            backward = new Directional(to.asThing(), from.asThing(), BACKWARD, predicate.reflection());
        }

        public static class Directional extends PlannerEdge.Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

            private final com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate;

            Directional(PlannerVertex.Thing from, PlannerVertex.Thing to, Encoding.Direction.Edge direction,
                        com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate) {
                super(from, to, direction, predicate.toString());
                this.predicate = predicate;
            }

            public com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate() {
                return predicate;
            }

            @Override
            public boolean isPredicate() {
                return true;
            }

            @Override
            public Predicate.Directional asPredicate() {
                return this;
            }

            @Override
            void updateObjective(GraphManager graphMgr) {
                long cost;
                if (isSelfClosure() || to().props().hasIID()) {
                    cost = 1;
                } else if (predicate.operator().equals(PredicateOperator.Equality.EQ)) {
                    cost = to.props().types().size();
                } else {
                    cost = graphMgr.data().stats().thingVertexSum(to.props().types());
                }
                assert !Double.isNaN(cost);
                setObjectiveCoefficient(cost);
            }
        }
    }

    public static abstract class Native<VERTEX_NATIVE_FROM extends PlannerVertex<?>, VERTEX_NATIVE_TO extends PlannerVertex<?>>
            extends PlannerEdge<VERTEX_NATIVE_FROM, VERTEX_NATIVE_TO> {

        protected final Encoding.Edge encoding;

        Native(VERTEX_NATIVE_FROM from, VERTEX_NATIVE_TO to, Encoding.Edge encoding) {
            super(from, to, encoding.name());
            this.encoding = encoding;
        }

        static PlannerEdge.Native<?, ?> of(PlannerVertex<?> from, PlannerVertex<?> to, StructureEdge.Native<?, ?> structureEdge) {
            if (structureEdge.encoding().equals(ISA)) {
                return new Isa(from.asThing(), to.asType(), structureEdge.isTransitive());
            } else if (structureEdge.encoding().isType()) {
                return Type.of(from.asType(), to.asType(), structureEdge);
            } else if (structureEdge.encoding().isThing()) {
                return Thing.of(from.asThing(), to.asThing(), structureEdge);
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        public static abstract class Directional<VERTEX_NATIVE_DIR_FROM extends PlannerVertex<?>, VERTEX_NATIVE_DIR_TO extends PlannerVertex<?>>
                extends PlannerEdge.Directional<VERTEX_NATIVE_DIR_FROM, VERTEX_NATIVE_DIR_TO> {

            Directional(VERTEX_NATIVE_DIR_FROM from, VERTEX_NATIVE_DIR_TO to,
                        Encoding.Direction.Edge direction, Encoding.Edge encoding) {
                super(from, to, direction, encoding.name());
            }

            @Override
            public boolean isNative() {
                return true;
            }

            @Override
            public Native.Directional<?, ?> asNative() {
                return this;
            }

            public boolean isIsa() {
                return false;
            }

            public boolean isType() {
                return false;
            }

            public boolean isThing() {
                return false;
            }

            public Isa.Directional<?, ?> asIsa() {
                throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Isa.Directional.class));
            }

            public Type.Directional asType() {
                throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.Directional.class));
            }

            public Thing.Directional asThing() {
                throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.Directional.class));
            }
        }

        static class Isa extends Native<PlannerVertex.Thing, PlannerVertex.Type> {

            private final boolean isTransitive;

            Isa(PlannerVertex.Thing from, PlannerVertex.Type to, boolean isTransitive) {
                super(from, to, ISA);
                this.isTransitive = isTransitive;
            }

            @Override
            protected void initialiseDirectionalEdges() {
                forward = new Forward(from.asThing(), to.asType());
                backward = new Backward(to.asType(), from.asThing());
            }

            public abstract class Directional<VERTEX_ISA_FROM extends PlannerVertex<?>, VERTEX_ISA_TO extends PlannerVertex<?>>
                    extends Native.Directional<VERTEX_ISA_FROM, VERTEX_ISA_TO> {

                Directional(VERTEX_ISA_FROM from, VERTEX_ISA_TO to, Encoding.Direction.Edge direction) {
                    super(from, to, direction, ISA);
                }

                public boolean isTransitive() {
                    return isTransitive;
                }

                @Override
                public boolean isIsa() {
                    return true;
                }

                @Override
                public Isa.Directional<?, ?> asIsa() {
                    return this;
                }
            }

            private class Forward extends Isa.Directional<PlannerVertex.Thing, PlannerVertex.Type> {

                private Forward(PlannerVertex.Thing from, PlannerVertex.Type to) {
                    super(from, to, FORWARD);
                }

                @Override
                void updateObjective(GraphManager graphMgr) {
                    long cost;
                    if (!isTransitive) cost = 1;
                    else cost = graphMgr.schema().stats().subTypesDepth(to.props().labels());
                    assert !Double.isNaN(cost);
                    setObjectiveCoefficient(cost);
                }
            }

            private class Backward extends Isa.Directional<PlannerVertex.Type, PlannerVertex.Thing> {

                private Backward(PlannerVertex.Type from, PlannerVertex.Thing to) {
                    super(from, to, BACKWARD);
                }

                @Override
                void updateObjective(GraphManager graphMgr) {
                    long cost;
                    if (to().props().hasIID()) {
                        cost = 1;
                    } else {
                        Set<Label> toTypes = to.props().types();
                        // TODO: should this calculate the average rather than max?
                        if (!isTransitive) cost = graphMgr.data().stats().thingVertexMax(toTypes);
                        else cost = graphMgr.data().stats().thingVertexTransitiveMax(toTypes);
                    }
                    assert !Double.isNaN(cost);
                    setObjectiveCoefficient(cost);
                }
            }
        }

        public static abstract class Type extends Native<PlannerVertex.Type, PlannerVertex.Type> {

            protected final boolean isTransitive;

            Type(PlannerVertex.Type from, PlannerVertex.Type to, Encoding.Edge.Type encoding, boolean isTransitive) {
                super(from, to, encoding);
                this.isTransitive = isTransitive;
            }

            public Encoding.Edge.Type encoding() {
                return encoding.asType();
            }

            static Type of(PlannerVertex.Type from, PlannerVertex.Type to, StructureEdge.Native<?, ?> structureEdge) {
                Encoding.Edge.Type encoding = structureEdge.encoding().asType();
                switch (encoding) {
                    case SUB:
                        return new Type.Sub(from.asType(), to.asType(), structureEdge.isTransitive());
                    case OWNS:
                        return new Type.Owns(from.asType(), to.asType(), false);
                    case OWNS_KEY:
                        return new Type.Owns(from.asType(), to.asType(), true);
                    case PLAYS:
                        return new Type.Plays(from.asType(), to.asType());
                    case RELATES:
                        return new Type.Relates(from.asType(), to.asType());
                    default:
                        throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            public abstract class Directional extends Native.Directional<PlannerVertex.Type, PlannerVertex.Type> {

                Directional(PlannerVertex.Type from, PlannerVertex.Type to,
                            Encoding.Direction.Edge direction, Encoding.Edge encoding) {
                    super(from, to, direction, encoding);
                }

                public boolean isTransitive() {
                    return isTransitive;
                }

                @Override
                public boolean isType() {
                    return true;
                }

                @Override
                public Type.Directional asType() {
                    return this;
                }

                public boolean isSub() {
                    return false;
                }

                public boolean isOwns() {
                    return false;
                }

                public boolean isPlays() {
                    return false;
                }

                public boolean isRelates() {
                    return false;
                }

                public Type.Sub.Directional asSub() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.Sub.Directional.class));
                }

                public Type.Owns.Directional asOwns() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.Owns.Directional.class));
                }

                public Type.Plays.Directional asPlays() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.Plays.Directional.class));
                }

                public Type.Relates.Directional asRelates() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.Relates.Directional.class));
                }
            }

            static class Sub extends Type {

                Sub(PlannerVertex.Type from, PlannerVertex.Type to, boolean isTransitive) {
                    super(from, to, SUB, isTransitive);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asType(), to.asType());
                    backward = new Backward(to.asType(), from.asType());
                }

                private abstract class Directional extends Type.Directional {

                    Directional(PlannerVertex.Type from, PlannerVertex.Type to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, SUB);
                    }

                    @Override
                    public boolean isSub() {
                        return true;
                    }

                    @Override
                    public Type.Sub.Directional asSub() {
                        return this;
                    }
                }

                private class Forward extends Sub.Directional {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        long cost;
                        if (isSelfClosure() || !isTransitive) {
                            cost = 1;
                        } else if (!to.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().subTypesDepth(to.props().labels());
                        } else {
                            cost = graphMgr.schema().stats().subTypesDepth(graphMgr.schema().rootThingType());
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Sub.Directional {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        double cost;
                        if (isSelfClosure()) {
                            cost = 1;
                        } else if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().subTypesMean(from.props().labels(), isTransitive);
                        } else {
                            cost = graphMgr.schema().stats().subTypesMean(graphMgr.schema().thingTypes().stream(), isTransitive);
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }
            }

            static class Owns extends Type {

                private final boolean isKey;

                Owns(PlannerVertex.Type from, PlannerVertex.Type to, boolean isKey) {
                    super(from, to, isKey ? OWNS_KEY : OWNS, false);
                    this.isKey = isKey;
                }

                public boolean isKey() {
                    return isKey;
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asType(), to.asType());
                    backward = new Backward(to.asType(), from.asType());
                }

                public abstract class Directional extends Type.Directional {

                    Directional(PlannerVertex.Type from, PlannerVertex.Type to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, OWNS);
                    }

                    public boolean isKey() {
                        return isKey;
                    }

                    @Override
                    public boolean isOwns() {
                        return true;
                    }

                    @Override
                    public Type.Owns.Directional asOwns() {
                        return this;
                    }
                }

                private class Forward extends Owns.Directional {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        double cost;
                        if (isSelfClosure()) {
                            cost = 1;
                        } else if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().outOwnsMean(from.props().labels(), isKey);
                        } else {
                            // TODO: We can refine the branching factor by not strictly considering entity types only
                            cost = graphMgr.schema().stats().outOwnsMean(graphMgr.schema().entityTypes().stream(), isKey);
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Owns.Directional {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        // TODO: We can refine the branching factor by not strictly considering entity types only
                        double cost;
                        if (isSelfClosure()) {
                            cost = 1;
                        } else if (!to.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().subTypesSum(to.props().labels(), true);
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().inOwnsMean(from.props().labels(), isKey) *
                                    graphMgr.schema().stats().subTypesMean(graphMgr.schema().entityTypes().stream(), true);
                        } else {
                            cost = graphMgr.schema().stats().inOwnsMean(graphMgr.schema().attributeTypes().stream(), isKey) *
                                    graphMgr.schema().stats().subTypesMean(graphMgr.schema().entityTypes().stream(), true);
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }
            }

            static class Plays extends Type {

                Plays(PlannerVertex.Type from, PlannerVertex.Type to) {
                    super(from, to, PLAYS, false);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asType(), to.asType());
                    backward = new Backward(to.asType(), from.asType());
                }

                private abstract class Directional extends Type.Directional {

                    Directional(PlannerVertex.Type from, PlannerVertex.Type to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, PLAYS);
                    }

                    @Override
                    public boolean isPlays() {
                        return true;
                    }

                    @Override
                    public Type.Plays.Directional asPlays() {
                        return this;
                    }
                }

                private class Forward extends Plays.Directional {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().outPlaysMean(from.props().labels());
                        } else {
                            // TODO: We can refine the branching factor by not strictly considering entity types only
                            cost = graphMgr.schema().stats().outPlaysMean(graphMgr.schema().entityTypes().stream());
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Plays.Directional {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        // TODO: We can refine the branching factor by not strictly considering entity types only
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().subTypesSum(to.props().labels(), true);
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().inPlaysMean(from.props().labels()) *
                                    graphMgr.schema().stats().subTypesMean(graphMgr.schema().entityTypes().stream(), true);
                        } else {
                            cost = graphMgr.schema().stats().inPlaysMean(graphMgr.schema().attributeTypes().stream()) *
                                    graphMgr.schema().stats().subTypesMean(graphMgr.schema().entityTypes().stream(), true);
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }
            }

            static class Relates extends Type {

                Relates(PlannerVertex.Type from, PlannerVertex.Type to) {
                    super(from, to, RELATES, false);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asType(), to.asType());
                    backward = new Backward(to.asType(), from.asType());
                }

                private abstract class Directional extends Type.Directional {

                    Directional(PlannerVertex.Type from, PlannerVertex.Type to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, RELATES);
                    }

                    @Override
                    public boolean isRelates() {
                        return true;
                    }

                    @Override
                    public Type.Relates.Directional asRelates() {
                        return this;
                    }
                }

                private class Forward extends Relates.Directional {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().outRelates(from.props().labels());
                        } else {
                            cost = graphMgr.schema().stats().outRelates(graphMgr.schema().relationTypes().stream());
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Relates.Directional {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = graphMgr.schema().stats().subTypesMean(to.props().labels(), true);
                        } else if (!from.props().labels().isEmpty()) {
                            Stream<TypeVertex> relationTypes = from.props().labels().stream().map(l -> {
                                assert l.scope().isPresent();
                                return Label.of(l.scope().get());
                            }).map(l -> graphMgr.schema().getType(l));
                            cost = graphMgr.schema().stats().subTypesMean(relationTypes, true);
                        } else {
                            cost = graphMgr.schema().stats().subTypesMean(graphMgr.schema().relationTypes().stream(), true);
                        }
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }
            }
        }

        public static abstract class Thing extends Native<PlannerVertex.Thing, PlannerVertex.Thing> {

            Thing(PlannerVertex.Thing from, PlannerVertex.Thing to, Encoding.Edge.Thing encoding) {
                super(from, to, encoding);
            }

            public Encoding.Edge.Thing encoding() {
                return encoding.asThing();
            }

            static Thing of(PlannerVertex.Thing from, PlannerVertex.Thing to, StructureEdge.Native<?, ?> structureEdge) {
                Encoding.Edge.Thing encoding = structureEdge.encoding().asThing();
                if (encoding == HAS) {
                    return new Has(from, to);
                } else if (encoding == PLAYING) {
                    return new Playing(from, to);
                } else if (encoding == RELATING) {
                    return new Relating(from, to);
                } else if (encoding == ROLEPLAYER) {
                    return new RolePlayer(from, to, structureEdge.asRolePlayer().types());
                } else {
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
                }
            }

            public abstract static class Directional extends Native.Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                Directional(PlannerVertex.Thing from, PlannerVertex.Thing to,
                            Encoding.Direction.Edge direction, Encoding.Edge encoding) {
                    super(from, to, direction, encoding);
                }

                @Override
                public boolean isThing() {
                    return true;
                }

                @Override
                public Thing.Directional asThing() {
                    return this;
                }

                public boolean isHas() {
                    return false;
                }

                public boolean isPlaying() {
                    return false;
                }

                public boolean isRelating() {
                    return false;
                }

                public boolean isRolePlayer() {
                    return false;
                }

                public Thing.Has.Directional asHas() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.Has.Directional.class));
                }

                public Thing.Playing.Directional asPlaying() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.Playing.Directional.class));
                }

                public Thing.Relating.Directional asRelating() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.Relating.Directional.class));
                }

                public Thing.RolePlayer.Directional asRolePlayer() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.RolePlayer.Directional.class));
                }
            }

            static class Has extends Thing {

                Has(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                    super(from, to, HAS);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asThing(), to.asThing());
                    backward = new Backward(to.asThing(), from.asThing());
                }

                private abstract static class Directional extends Thing.Directional {

                    Directional(PlannerVertex.Thing from, PlannerVertex.Thing to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, HAS);
                    }

                    @Override
                    public boolean isHas() {
                        return true;
                    }

                    @Override
                    public Thing.Has.Directional asHas() {
                        return this;
                    }
                }

                private static class Forward extends Has.Directional {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        if (isSelfClosure() || to().props().hasIID()) {
                            setObjectiveCoefficient(1);
                            return;
                        }

                        Set<TypeVertex> ownerTypes = iterate(from.props().types()).map(l -> graphMgr.schema().getType(l)).toSet();
                        Set<TypeVertex> attTypes = iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).toSet();

                        double cost = 0.0;
                        for (TypeVertex owner : ownerTypes) {
                            double div = graphMgr.data().stats().thingVertexCount(owner);
                            if (div > 0) {
                                cost += graphMgr.data().stats().hasEdgeSum(owner, attTypes) / div;
                            }
                        }
                        assert !ownerTypes.isEmpty();
                        cost /= ownerTypes.size();
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private static class Backward extends Has.Directional {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        if (isSelfClosure() || to().props().hasIID()) {
                            setObjectiveCoefficient(1);
                            return;
                        }

                        Set<TypeVertex> attTypes = iterate(from.props().types()).map(l -> graphMgr.schema().getType(l)).toSet();
                        Set<TypeVertex> ownerTypes = iterate(to.props().types()).map(l -> graphMgr.schema().getType(l)).toSet();

                        double cost = 0.0;
                        for (TypeVertex attr : attTypes) {
                            double div = graphMgr.data().stats().thingVertexCount(attr);
                            if (div > 0) {
                                cost += graphMgr.data().stats().hasEdgeSum(ownerTypes, attr) / div;
                            }
                        }
                        assert !attTypes.isEmpty();
                        cost /= attTypes.size();
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }
            }

            static class Playing extends Thing {

                Playing(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                    super(from, to, PLAYING);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asThing(), to.asThing());
                    backward = new Backward(to.asThing(), from.asThing());
                }

                private abstract static class Directional extends Thing.Directional {

                    Directional(PlannerVertex.Thing from, PlannerVertex.Thing to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, PLAYING);
                    }

                    @Override
                    public boolean isPlaying() {
                        return true;
                    }

                    @Override
                    public Thing.Playing.Directional asPlaying() {
                        return this;
                    }
                }

                private static class Forward extends Playing.Directional {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        assert !to.props().hasIID();
                        double cost = 0.0;
                        double div = graphMgr.data().stats().thingVertexSum(from.props().types());
                        if (div > 0) cost = graphMgr.data().stats().thingVertexSum(to.props().types()) / div;
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private static class Backward extends Playing.Directional {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        setObjectiveCoefficient(1);
                    }
                }
            }

            static class Relating extends Thing {

                Relating(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                    super(from, to, RELATING);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asThing(), to.asThing());
                    backward = new Backward(to.asThing(), from.asThing());
                }

                private abstract static class Directional extends Thing.Directional {

                    Directional(PlannerVertex.Thing from, PlannerVertex.Thing to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, RELATING);
                    }

                    @Override
                    public boolean isRelating() {
                        return true;
                    }

                    @Override
                    public Thing.Relating.Directional asRelating() {
                        return this;
                    }
                }

                private static class Forward extends Relating.Directional {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        assert !to.props().hasIID();
                        double cost = 0;
                        for (Label roleType : to.props().types()) {
                            assert roleType.scope().isPresent();
                            double div = graphMgr.data().stats().thingVertexCount(Label.of(roleType.scope().get()));
                            if (div > 0) cost += graphMgr.data().stats().thingVertexCount(roleType) / div;
                        }
                        assert !to.props().types().isEmpty();
                        cost /= to.props().types().size();
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private static class Backward extends Relating.Directional {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        setObjectiveCoefficient(1);
                    }
                }
            }

            public static class RolePlayer extends Thing {

                private final Set<Label> roleTypes;

                RolePlayer(PlannerVertex.Thing from, PlannerVertex.Thing to, Set<Label> roleTypes) {
                    super(from, to, ROLEPLAYER);
                    assert !roleTypes.isEmpty();
                    this.roleTypes = roleTypes;
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asThing(), to.asThing());
                    backward = new Backward(to.asThing(), from.asThing());
                }

                public abstract class Directional extends Thing.Directional {

                    Directional(PlannerVertex.Thing from, PlannerVertex.Thing to, Encoding.Direction.Edge direction) {
                        super(from, to, direction, ROLEPLAYER);
                    }

                    @Override
                    public boolean isRolePlayer() {
                        return true;
                    }

                    @Override
                    public Thing.RolePlayer.Directional asRolePlayer() {
                        return this;
                    }

                    public Set<Label> roleTypes() {
                        return roleTypes;
                    }
                }

                private class Forward extends RolePlayer.Directional {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, FORWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        if (isSelfClosure() || to.props().hasIID()) {
                            setObjectiveCoefficient(1);
                            return;
                        }

                        double cost = 0;
                        Set<TypeVertex> roleTypeVertices = iterate(this.roleTypes()).map(graphMgr.schema()::getType).toSet();
                        for (TypeVertex roleType : roleTypeVertices) {
                            assert roleType.isRoleType() && roleType.properLabel().scope().isPresent();
                            double div = graphMgr.data().stats().thingVertexCount(Label.of(roleType.properLabel().scope().get()));
                            if (div > 0) cost += graphMgr.data().stats().thingVertexCount(roleType) / div;
                        }
                        assert !roleTypeVertices.isEmpty();
                        cost = cost / roleTypeVertices.size();
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends RolePlayer.Directional {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                        super(from, to, BACKWARD);
                    }

                    @Override
                    void updateObjective(GraphManager graphMgr) {
                        if (isSelfClosure() || to.props().hasIID()) {
                            setObjectiveCoefficient(1);
                            return;
                        }

                        double cost = 0;
                        double div = graphMgr.data().stats().thingVertexSum(from.props().types());
                        if (div > 0) cost = graphMgr.data().stats().thingVertexSum(roleTypes) / div;
                        assert !Double.isNaN(cost);
                        setObjectiveCoefficient(cost);
                    }
                }
            }
        }
    }
}
