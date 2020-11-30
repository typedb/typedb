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

package grakn.core.traversal.planner;

import com.google.ortools.linearsolver.MPConstraint;
import com.google.ortools.linearsolver.MPVariable;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.traversal.graph.TraversalEdge;
import grakn.core.traversal.structure.StructureEdge;
import graql.lang.common.GraqlToken;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.pair;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.graph.util.Encoding.Edge.ISA;
import static grakn.core.graph.util.Encoding.Edge.Thing.HAS;
import static grakn.core.graph.util.Encoding.Edge.Thing.PLAYING;
import static grakn.core.graph.util.Encoding.Edge.Thing.RELATING;
import static grakn.core.graph.util.Encoding.Edge.Thing.ROLEPLAYER;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
import static grakn.core.graph.util.Encoding.Edge.Type.PLAYS;
import static grakn.core.graph.util.Encoding.Edge.Type.RELATES;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;
import static grakn.core.traversal.planner.Planner.OBJECTIVE_VARIABLE_COST_MAX_CHANGE;
import static grakn.core.traversal.planner.Planner.OBJECTIVE_VARIABLE_TO_PLANNER_COST_MIN_CHANGE;
import static graql.lang.common.GraqlToken.Predicate.Equality.EQ;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("NonAtomicOperationOnVolatileField") // Because Planner.optimise() is synchronised
public abstract class PlannerEdge<VERTEX_FROM extends PlannerVertex<?>, VERTEX_TO extends PlannerVertex<?>>
        extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    protected final Planner planner;
    protected Directional<VERTEX_FROM, VERTEX_TO> forward;
    protected Directional<VERTEX_TO, VERTEX_FROM> backward;

    PlannerEdge(VERTEX_FROM from, VERTEX_TO to) {
        super(from, to);
        this.planner = from.planner;
        assert this.planner.equals(to.planner);
        initialiseDirectionalEdges();
    }

    protected abstract void initialiseDirectionalEdges();

    static PlannerEdge<?, ?> of(PlannerVertex<?> from, PlannerVertex<?> to, StructureEdge<?, ?> structureEdge) {
        if (structureEdge.isEqual()) return new PlannerEdge.Equal(from, to);
        else if (structureEdge.isPredicate())
            return new PlannerEdge.Predicate(from.asThing(), to.asThing(), structureEdge.asPredicate().predicate());
        else if (structureEdge.isNative()) return PlannerEdge.Native.of(from, to, structureEdge.asNative());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    Directional<VERTEX_FROM, VERTEX_TO> forward() {
        return forward;
    }

    Directional<VERTEX_TO, VERTEX_FROM> backward() {
        return backward;
    }

    void initialiseVariables() {
        forward.initialiseVariables();
        backward.initialiseVariables();
    }

    void initialiseConstraints() {
        String conPrefix = "edge::con::" + from + "::" + to + "::";
        MPConstraint conOneDirection = planner.solver().makeConstraint(1, 1, conPrefix + "one_direction");
        conOneDirection.setCoefficient(forward.varIsSelected, 1);
        conOneDirection.setCoefficient(backward.varIsSelected, 1);

        forward.initialiseConstraints();
        backward.initialiseConstraints();
    }

    void updateObjective(GraphManager graph) {
        forward.updateObjective(graph);
        backward.updateObjective(graph);
    }

    void recordCost() {
        forward.recordCost();
        backward.recordCost();
    }

    void recordValues() {
        forward.recordValues();
        backward.recordValues();
    }

    public static abstract class Directional<VERTEX_DIR_FROM extends PlannerVertex<?>, VERTEX_DIR_TO extends PlannerVertex<?>>
            extends TraversalEdge<VERTEX_DIR_FROM, VERTEX_DIR_TO> {

        MPVariable varIsSelected;
        MPVariable[] varOrderAssignment;
        private MPVariable varOrderNumber;
        private int valueIsSelected;
        private int valueOrderNumber;
        private final String varPrefix;
        private final String conPrefix;
        private final Planner planner;
        private final PlannerEdge<?, ?> parent;
        private final boolean isForward;
        private double costPrevious;
        private double costNext;
        private boolean isInitialisedVariables;
        private boolean isInitialisedConstraints;

        Directional(VERTEX_DIR_FROM from, VERTEX_DIR_TO to, PlannerEdge<?, ?> parent, boolean isForward) {
            super(from, to);
            this.parent = parent;
            this.planner = parent.planner;
            this.isForward = isForward;
            this.costPrevious = 0.01; // non-zero value for safe division
            this.isInitialisedVariables = false;
            this.isInitialisedConstraints = false;
            this.varPrefix = "edge::var::" + from + "::" + to + "::";
            this.conPrefix = "edge::con::" + from + "::" + to + "::";
        }

        abstract void updateObjective(GraphManager graph);

        public boolean isSelected() {
            return valueIsSelected == 1;
        }

        public int orderNumber() {
            return valueOrderNumber;
        }

        public boolean isInitialisedVariables() {
            return isInitialisedVariables;
        }

        public boolean isInitialisedConstraints() {
            return isInitialisedConstraints;
        }

        void initialiseVariables() {
            varIsSelected = planner.solver().makeIntVar(0, 1, varPrefix + "is_selected");
            varOrderNumber = planner.solver().makeIntVar(0, planner.edges().size(), varPrefix + "order_number");
            varOrderAssignment = new MPVariable[planner.edges().size()];
            for (int i = 0; i < planner.edges().size(); i++) {
                varOrderAssignment[i] = planner.solver().makeIntVar(0, 1, varPrefix + "order_assignment[" + i + "]");
            }
            isInitialisedVariables = true;
        }

        void initialiseConstraints() {
            assert from.isInitialisedVariables();
            assert to.isInitialisedConstraints();
            initialiseConstraintsForOrderNumber();
            initialiseConstraintsForVertexFlow();
            initialiseConstraintsForOrderSequence();
            isInitialisedConstraints = true;
        }

        private void initialiseConstraintsForOrderNumber() {
            MPConstraint conOrderIfSelected = planner.solver().makeConstraint(0, 0, conPrefix + "order_if_selected");
            conOrderIfSelected.setCoefficient(varIsSelected, -1);

            MPConstraint conAssignOrderNumber = planner.solver().makeConstraint(0, 0, conPrefix + "assign_order_number");
            conAssignOrderNumber.setCoefficient(varOrderNumber, -1);

            for (int i = 0; i < planner.edges().size(); i++) {
                conOrderIfSelected.setCoefficient(varOrderAssignment[i], 1);
                conAssignOrderNumber.setCoefficient(varOrderAssignment[i], i + 1);
            }
        }

        private void initialiseConstraintsForVertexFlow() {
            MPConstraint conOutFromVertex = planner.solver().makeConstraint(0, 1, conPrefix + "out_from_vertex");
            conOutFromVertex.setCoefficient(from.varHasOutgoingEdges, 1);
            conOutFromVertex.setCoefficient(varIsSelected, -1);

            MPConstraint conInToVertex = planner.solver().makeConstraint(0, 1, conPrefix + "in_to_vertex");
            conInToVertex.setCoefficient(to.varHasIncomingEdges, 1);
            conInToVertex.setCoefficient(varIsSelected, -1);
        }

        private void initialiseConstraintsForOrderSequence() {
            to.outs().stream().filter(e -> !e.parent.equals(this.parent)).forEach(subsequentEdge -> {
                MPConstraint conOrderSequence = planner.solver().makeConstraint(0, planner.edges().size() + 1, conPrefix + "order_sequence");
                conOrderSequence.setCoefficient(to.varIsEndingVertex, planner.edges().size());
                conOrderSequence.setCoefficient(subsequentEdge.varOrderNumber, 1);
                conOrderSequence.setCoefficient(this.varIsSelected, -1);
                conOrderSequence.setCoefficient(this.varOrderNumber, -1);
            });
        }

        protected void setObjectiveCoefficient(double cost) {
            int exp = planner.edges().size() - 1;
            for (int i = 0; i < planner.edges().size(); i++) {
                planner.objective().setCoefficient(varOrderAssignment[i], cost * Math.pow(planner.branchingFactor, exp--));
            }
            planner.totalCostNext += cost;
            assert costPrevious > 0;
            if (cost / costPrevious >= OBJECTIVE_VARIABLE_COST_MAX_CHANGE &&
                    cost / planner.totalCostPrevious >= OBJECTIVE_VARIABLE_TO_PLANNER_COST_MIN_CHANGE) {
                planner.setOutOfDate();
            }
            costNext = cost;
        }

        private void recordCost() {
            costPrevious = costNext;
        }

        private void recordValues() {
            valueIsSelected = (int) Math.round(varIsSelected.solutionValue());
            valueOrderNumber = (int) Math.round(varOrderNumber.solutionValue());
        }

        public boolean isForward() { return isForward; }

        public boolean isBackward() { return !isForward; }
    }

    static class Equal extends PlannerEdge<PlannerVertex<?>, PlannerVertex<?>> {

        Equal(PlannerVertex<?> from, PlannerVertex<?> to) {
            super(from, to);
        }

        @Override
        protected void initialiseDirectionalEdges() {
            forward = new Directional(from, to, this, true);
            backward = new Directional(to, from, this, false);
        }

        private class Directional extends PlannerEdge.Directional<PlannerVertex<?>, PlannerVertex<?>> {

            Directional(PlannerVertex<?> from, PlannerVertex<?> to, Equal parent, boolean isForward) {
                super(from, to, parent, isForward);
            }

            @Override
            void updateObjective(GraphManager graph) {
                setObjectiveCoefficient(0);
            }
        }
    }

    static class Predicate extends PlannerEdge<PlannerVertex.Thing, PlannerVertex.Thing> {

        private final GraqlToken.Predicate.Equality predicate;

        Predicate(PlannerVertex.Thing from, PlannerVertex.Thing to, GraqlToken.Predicate.Equality predicate) {
            super(from, to);
            this.predicate = predicate;
        }

        @Override
        protected void initialiseDirectionalEdges() {
            forward = new Directional(from.asThing(), to.asThing(), this, true);
            backward = new Directional(to.asThing(), from.asThing(), this, false);
        }

        private class Directional extends PlannerEdge.Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

            Directional(PlannerVertex.Thing from, PlannerVertex.Thing to, Predicate parent, boolean isForward) {
                super(from, to, parent, isForward);
            }

            public GraqlToken.Predicate predicate() {
                return predicate;
            }

            void updateObjective(GraphManager graph) {
                long cost;
                if (predicate.equals(EQ)) {
                    if (!to.props().types().isEmpty()) {
                        cost = to.props().types().size();
                    } else if (!from.props().types().isEmpty()) {
                        cost = graph.schema().stats().attTypesWithValTypeComparableTo(from.props().types());
                    } else {
                        cost = graph.schema().stats().attributeTypeCount();
                    }
                } else {
                    if (!to.props().types().isEmpty()) {
                        cost = graph.data().stats().thingVertexSum(to.props().types());
                    } else if (!from.props().types().isEmpty()) {
                        Stream<TypeVertex> types = from.props().types().stream().map(l -> graph.schema().getType(l));
                        cost = graph.data().stats().thingVertexSum(types);
                    } else {
                        cost = graph.data().stats().thingVertexTransitiveCount(graph.schema().rootAttributeType());
                    }
                }
                setObjectiveCoefficient(cost);
            }
        }
    }

    static abstract class Native<VERTEX_NATIVE_FROM extends PlannerVertex<?>, VERTEX_NATIVE_TO extends PlannerVertex<?>>
            extends PlannerEdge<VERTEX_NATIVE_FROM, VERTEX_NATIVE_TO> {

        Native(VERTEX_NATIVE_FROM from, VERTEX_NATIVE_TO to) {
            super(from, to);
        }

        static PlannerEdge.Native<?, ?> of(PlannerVertex<?> from, PlannerVertex<?> to, StructureEdge.Native<?, ?> structureEdge) {
            if (structureEdge.encoding().equals(ISA)) {
                return new Isa(from.asThing(), to.asType(), structureEdge.isTransitive());
            } else if (structureEdge.encoding().isType()) {
                return Type.of(from.asType(), to.asType(), structureEdge);
            } else if (structureEdge.encoding().isThing()) {
                return Thing.of(from.asThing(), to.asThing(), structureEdge);
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        static class Isa extends Native<PlannerVertex.Thing, PlannerVertex.Type> {

            private final boolean isTransitive;

            Isa(PlannerVertex.Thing from, PlannerVertex.Type to, boolean isTransitive) {
                super(from, to);
                this.isTransitive = isTransitive;
            }

            @Override
            protected void initialiseDirectionalEdges() {
                forward = new Forward(from.asThing(), to.asType(), this);
                backward = new Backward(to.asType(), from.asThing(), this);
            }

            private class Forward extends Directional<PlannerVertex.Thing, PlannerVertex.Type> {

                private Forward(PlannerVertex.Thing from, PlannerVertex.Type to, Isa parent) {
                    super(from, to, parent, true);
                }

                @Override
                void updateObjective(GraphManager graph) {
                    long cost;
                    if (!isTransitive) {
                        cost = 1;
                    } else if (!to.props().labels().isEmpty()) {
                        cost = graph.schema().stats().subTypesDepth(to.props().labels());
                    } else {
                        cost = graph.schema().stats().subTypesDepth(graph.schema().rootThingType());
                    }
                    setObjectiveCoefficient(cost);
                }
            }

            private class Backward extends Directional<PlannerVertex.Type, PlannerVertex.Thing> {

                private Backward(PlannerVertex.Type from, PlannerVertex.Thing to, Isa parent) {
                    super(from, to, parent, false);
                }

                @Override
                void updateObjective(GraphManager graph) {
                    long cost;
                    if (!to.props().types().isEmpty()) {
                        if (!isTransitive) cost = graph.data().stats().thingVertexMax(to.props().types());
                        else
                            cost = graph.data().stats().thingVertexTransitiveMax(to.props().types(), to.props().types());
                    } else if (!from.props().labels().isEmpty()) {
                        if (!isTransitive) cost = graph.data().stats().thingVertexMax(from.props().labels());
                        else
                            cost = graph.data().stats().thingVertexTransitiveMax(from.props().labels(), to.props().types());
                    } else {
                        if (!isTransitive) cost = graph.data().stats().thingVertexMax(graph.schema().thingTypes());
                        else cost = graph.data().stats().thingVertexTransitiveMax(graph.schema().thingTypes(), set());
                    }
                    setObjectiveCoefficient(cost);
                }
            }
        }

        static abstract class Type extends Native<PlannerVertex.Type, PlannerVertex.Type> {

            protected final Encoding.Edge.Type encoding;
            protected final boolean isTransitive;

            Type(PlannerVertex.Type from, PlannerVertex.Type to, Encoding.Edge.Type encoding, boolean isTransitive) {
                super(from, to);
                this.encoding = encoding;
                this.isTransitive = isTransitive;
            }

            public Encoding.Edge.Type encoding() {
                return encoding;
            }

            public boolean isTransitive() {
                return isTransitive;
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
                        throw GraknException.of(UNRECOGNISED_VALUE);
                }
            }

            static class Sub extends Type {

                Sub(PlannerVertex.Type from, PlannerVertex.Type to, boolean isTransitive) {
                    super(from, to, SUB, isTransitive);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asType(), to.asType(), this);
                    backward = new Backward(to.asType(), from.asType(), this);
                }

                private class Forward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        long cost;
                        if (!isTransitive) {
                            cost = 1;
                        } else if (!to.props().labels().isEmpty()) {
                            cost = graph.schema().stats().subTypesDepth(to.props().labels());
                        } else {
                            cost = graph.schema().stats().subTypesDepth(graph.schema().rootThingType());
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graph.schema().stats().subTypesMean(from.props().labels(), isTransitive);
                        } else {
                            cost = graph.schema().stats().subTypesMean(graph.schema().thingTypes(), isTransitive);
                        }
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
                    forward = new Forward(from.asType(), to.asType(), this);
                    backward = new Backward(to.asType(), from.asType(), this);
                }

                private class Forward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graph.schema().stats().outOwnsMean(from.props().labels(), isKey);
                        } else {
                            // TODO: We can refine the branching factor by not strictly considering entity types only
                            cost = graph.schema().stats().outOwnsMean(graph.schema().entityTypes(), isKey);
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        // TODO: We can refine the branching factor by not strictly considering entity types only
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = graph.schema().stats().subTypesSum(to.props().labels(), true);
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graph.schema().stats().inOwnsMean(from.props().labels(), isKey) *
                                    graph.schema().stats().subTypesMean(graph.schema().entityTypes(), true);
                        } else {
                            cost = graph.schema().stats().inOwnsMean(graph.schema().attributeTypes(), isKey) *
                                    graph.schema().stats().subTypesMean(graph.schema().entityTypes(), true);
                        }
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
                    forward = new Forward(from.asType(), to.asType(), this);
                    backward = new Backward(to.asType(), from.asType(), this);
                }

                private class Forward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graph.schema().stats().outPlaysMean(from.props().labels());
                        } else {
                            // TODO: We can refine the branching factor by not strictly considering entity types only
                            cost = graph.schema().stats().outPlaysMean(graph.schema().entityTypes());
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        // TODO: We can refine the branching factor by not strictly considering entity types only
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = graph.schema().stats().subTypesSum(to.props().labels(), true);
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graph.schema().stats().inPlaysMean(from.props().labels()) *
                                    graph.schema().stats().subTypesMean(graph.schema().entityTypes(), true);
                        } else {
                            cost = graph.schema().stats().inPlaysMean(graph.schema().attributeTypes()) *
                                    graph.schema().stats().subTypesMean(graph.schema().entityTypes(), true);
                        }
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
                    forward = new Forward(from.asType(), to.asType(), this);
                    backward = new Backward(to.asType(), from.asType(), this);
                }

                private class Forward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Forward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = to.props().labels().size();
                        } else if (!from.props().labels().isEmpty()) {
                            cost = graph.schema().stats().outRelates(from.props().labels());
                        } else {
                            cost = graph.schema().stats().outRelates(graph.schema().relationTypes());
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Type, PlannerVertex.Type> {

                    private Backward(PlannerVertex.Type from, PlannerVertex.Type to, Type parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!to.props().labels().isEmpty()) {
                            cost = graph.schema().stats().subTypesMean(to.props().labels(), true);
                        } else if (!from.props().labels().isEmpty()) {
                            Stream<TypeVertex> relationTypes = from.props().labels().stream().map(l -> {
                                assert l.scope().isPresent();
                                return Label.of(l.scope().get());
                            }).map(l -> graph.schema().getType(l));
                            cost = graph.schema().stats().subTypesMean(relationTypes, true);
                        } else {
                            cost = graph.schema().stats().subTypesMean(graph.schema().relationTypes(), true);
                        }
                        setObjectiveCoefficient(cost);
                    }
                }
            }
        }

        static abstract class Thing extends Native<PlannerVertex.Thing, PlannerVertex.Thing> {

            private final Encoding.Edge.Thing encoding;

            Thing(PlannerVertex.Thing from, PlannerVertex.Thing to, Encoding.Edge.Thing encoding) {
                super(from, to);
                this.encoding = encoding;
            }

            public Encoding.Edge.Thing encoding() {
                return encoding;
            }

            static Thing of(PlannerVertex.Thing from, PlannerVertex.Thing to, StructureEdge.Native<?, ?> structureEdge) {
                Encoding.Edge.Thing encoding = structureEdge.encoding().asThing();
                switch (encoding) {
                    case HAS:
                        return new Has(from, to);
                    case PLAYING:
                        return new Playing(from, to);
                    case RELATING:
                        return new Relating(from, to);
                    case ROLEPLAYER:
                        return new RolePlayer(from, to, structureEdge.asOptimised().types());
                    default:
                        throw GraknException.of(UNRECOGNISED_VALUE);
                }
            }

            static class Has extends Thing {

                Has(PlannerVertex.Thing from, PlannerVertex.Thing to) {
                    super(from, to, HAS);
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asThing(), to.asThing(), this);
                    backward = new Backward(to.asThing(), from.asThing(), this);
                }

                private class Forward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        Set<TypeVertex> ownerTypes = null;
                        Set<TypeVertex> attTypes = null;
                        Map<TypeVertex, Set<TypeVertex>> ownerToAttributeTypes = new HashMap<>();

                        if (!from.props().types().isEmpty()) {
                            ownerTypes = from.props().types().stream()
                                    .map(l -> graph.schema().getType(l)).collect(toSet());
                        }
                        if (!to.props().types().isEmpty()) {
                            attTypes = to.props().types().stream()
                                    .map(l -> graph.schema().getType(l)).collect(toSet());
                        }

                        if (ownerTypes != null && attTypes != null) {
                            for (final TypeVertex ownerType : ownerTypes)
                                ownerToAttributeTypes.put(ownerType, attTypes);
                        } else if (ownerTypes != null) {
                            ownerTypes.stream().map(o -> pair(o, graph.schema().ownedAttributeTypes(o))).forEach(
                                    pair -> ownerToAttributeTypes.put(pair.first(), pair.second())
                            );
                        } else if (attTypes != null) {
                            attTypes.stream().flatMap(a -> graph.schema().ownersOfAttributeType(a).stream().map(o -> pair(o, a)))
                                    .forEach(pair -> ownerToAttributeTypes.computeIfAbsent(pair.first(), o -> new HashSet<>())
                                            .add(pair.second()));
                        } else { // fromTypes == null && toTypes == null;
                            // TODO: We can refine this by not strictly considering entities being the only divisor
                            ownerToAttributeTypes.put(graph.schema().rootEntityType(), set(graph.schema().rootAttributeType()));
                        }

                        double cost = 0.0;
                        for (TypeVertex owner : ownerToAttributeTypes.keySet()) {
                            cost += (double) graph.data().stats().hasEdgeSum(owner, ownerToAttributeTypes.get(owner)) /
                                    graph.data().stats().thingVertexCount(owner);
                        }
                        cost /= ownerToAttributeTypes.size();
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        Set<TypeVertex> ownerTypes = null;
                        Set<TypeVertex> attTypes = null;
                        Map<TypeVertex, Set<TypeVertex>> attributeTypesToOwners = new HashMap<>();

                        if (!from.props().types().isEmpty()) {
                            attTypes = from.props().types().stream()
                                    .map(l -> graph.schema().getType(l)).collect(toSet());
                        }
                        if (!to.props().types().isEmpty()) {
                            ownerTypes = to.props().types().stream()
                                    .map(l -> graph.schema().getType(l)).collect(toSet());
                        }

                        if (ownerTypes != null && attTypes != null) {
                            for (final TypeVertex attType : attTypes) attributeTypesToOwners.put(attType, ownerTypes);
                        } else if (attTypes != null) {
                            attTypes.stream().map(a -> pair(a, graph.schema().ownersOfAttributeType(a))).forEach(
                                    pair -> attributeTypesToOwners.put(pair.first(), pair.second())
                            );
                        } else if (ownerTypes != null) {
                            ownerTypes.stream().flatMap(o -> graph.schema().ownedAttributeTypes(o).stream().map(a -> pair(a, o)))
                                    .forEach(pair -> attributeTypesToOwners.computeIfAbsent(pair.first(), a -> new HashSet<>())
                                            .add(pair.second()));
                        } else { // fromTypes == null && toTypes == null;
                            attributeTypesToOwners.put(graph.schema().rootAttributeType(), set(graph.schema().rootThingType()));
                        }

                        double cost = 0.0;
                        for (TypeVertex owner : attributeTypesToOwners.keySet()) {
                            cost += (double) graph.data().stats().hasEdgeSum(attributeTypesToOwners.get(owner), owner) /
                                    graph.data().stats().thingVertexCount(owner);
                        }
                        cost /= attributeTypesToOwners.size();
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
                    forward = new Forward(from.asThing(), to.asThing(), this);
                    backward = new Backward(to.asThing(), from.asThing(), this);
                }

                private class Forward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!to.props().types().isEmpty() && !from.props().types().isEmpty()) {
                            cost = (double) graph.data().stats().thingVertexSum(to.props().types()) /
                                    graph.data().stats().thingVertexSum(from.props().types());
                        } else {
                            // TODO: We can refine this by not strictly considering entities being the only divisor
                            cost = (double) graph.data().stats().thingVertexTransitiveCount(graph.schema().rootRoleType()) /
                                    graph.data().stats().thingVertexTransitiveCount(graph.schema().rootEntityType());
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
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
                    forward = new Forward(from.asThing(), to.asThing(), this);
                    backward = new Backward(to.asThing(), from.asThing(), this);
                }

                private class Forward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!to.props().types().isEmpty()) {
                            cost = 0;
                            for (final Label roleType : to.props().types()) {
                                assert roleType.scope().isPresent();
                                cost += (double) graph.data().stats().thingVertexCount(roleType) /
                                        graph.data().stats().thingVertexCount(Label.of(roleType.scope().get()));
                            }
                            cost = cost / to.props().types().size();
                        } else {
                            cost = (double) graph.data().stats().thingVertexTransitiveCount(graph.schema().rootRoleType()) /
                                    graph.data().stats().thingVertexTransitiveCount(graph.schema().rootRelationType());
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        setObjectiveCoefficient(1);
                    }
                }
            }

            static class RolePlayer extends Thing {

                private final Set<Label> roleTypes;

                RolePlayer(PlannerVertex.Thing from, PlannerVertex.Thing to, Set<Label> roleTypes) {
                    super(from, to, ROLEPLAYER);
                    this.roleTypes = roleTypes;
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from.asThing(), to.asThing(), this);
                    backward = new Backward(to.asThing(), from.asThing(), this);
                }

                private class Forward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Forward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!roleTypes.isEmpty()) {
                            cost = 0;
                            for (final Label roleType : roleTypes) {
                                assert roleType.scope().isPresent();
                                cost += (double) graph.data().stats().thingVertexCount(roleType) /
                                        graph.data().stats().thingVertexCount(Label.of(roleType.scope().get()));
                            }
                            cost = cost / roleTypes.size();
                        } else {
                            cost = (double) graph.data().stats().thingVertexTransitiveCount(graph.schema().rootRoleType()) /
                                    graph.data().stats().thingVertexTransitiveCount(graph.schema().rootRelationType());
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional<PlannerVertex.Thing, PlannerVertex.Thing> {

                    private Backward(PlannerVertex.Thing from, PlannerVertex.Thing to, Thing parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(GraphManager graph) {
                        double cost;
                        if (!roleTypes.isEmpty() && !from.props().types().isEmpty()) {
                            cost = (double) graph.data().stats().thingVertexSum(roleTypes) /
                                    graph.data().stats().thingVertexSum(from.props().types());
                        } else {
                            // TODO: We can refine this by not strictly considering entities being the only divisor
                            cost = (double) graph.data().stats().thingVertexTransitiveCount(graph.schema().rootRoleType()) /
                                    graph.data().stats().thingVertexTransitiveCount(graph.schema().rootEntityType());
                        }
                        setObjectiveCoefficient(cost);
                    }
                }
            }
        }
    }
}
