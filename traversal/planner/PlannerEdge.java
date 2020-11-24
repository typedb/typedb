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
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.util.Encoding;
import grakn.core.traversal.graph.TraversalEdge;
import grakn.core.traversal.structure.StructureEdge;
import graql.lang.common.GraqlToken;

import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.graph.util.Encoding.Edge.Thing.HAS;
import static grakn.core.graph.util.Encoding.Edge.Thing.PLAYING;
import static grakn.core.graph.util.Encoding.Edge.Thing.RELATING;
import static grakn.core.graph.util.Encoding.Edge.Thing.ROLEPLAYER;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
import static grakn.core.graph.util.Encoding.Edge.Type.PLAYS;
import static grakn.core.graph.util.Encoding.Edge.Type.RELATES;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;

public abstract class PlannerEdge extends TraversalEdge<PlannerVertex<?>> {

    protected final Planner planner;
    protected Directional forward;
    protected Directional backward;

    PlannerEdge(PlannerVertex<?> from, PlannerVertex<?> to) {
        super(from, to);
        this.planner = from.planner();
        assert this.planner.equals(to.planner());
        initialiseDirectionalEdges();
    }

    protected abstract void initialiseDirectionalEdges();

    static PlannerEdge of(PlannerVertex<?> from, PlannerVertex<?> to, StructureEdge structureEdge) {
        if (structureEdge.isEqual()) return new PlannerEdge.Equal(from, to);
        else if (structureEdge.isPredicate()) return new PlannerEdge.Predicate(from, to, structureEdge.asPredicate().predicate());
        else if (structureEdge.isNative()) return PlannerEdge.Native.of(from, to, structureEdge.asNative());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    Directional forward() {
        return forward;
    }

    Directional backward() {
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

    void updateObjective(SchemaGraph graph) {
        forward.updateObjective(graph);
        backward.updateObjective(graph);
    }

    void recordValues() {
        forward.recordValues();
        backward.recordValues();
    }

    public abstract class Directional extends TraversalEdge<PlannerVertex<?>> {

        private static final double OBJ_COEFF_BASE = 2.0;

        MPVariable varIsSelected;
        MPVariable[] varOrderAssignment;
        private MPVariable varOrderNumber;
        private int valueIsSelected;
        private int valueOrderNumber;
        private final String varPrefix;
        private final String conPrefix;
        private final PlannerEdge parent;
        private final boolean isForward;
        private boolean isInitialisedVariables;
        private boolean isInitialisedConstraints;

        Directional(PlannerVertex<?> from, PlannerVertex<?> to, PlannerEdge parent, boolean isForward) {
            super(from, to);
            this.parent = parent;
            this.isForward = isForward;
            this.isInitialisedVariables = false;
            this.isInitialisedConstraints = false;
            this.varPrefix = "edge::var::" + from + "::" + to + "::";
            this.conPrefix = "edge::con::" + from + "::" + to + "::";
        }

        abstract void updateObjective(SchemaGraph graph);

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
                planner.objective().setCoefficient(varOrderAssignment[i], cost * Math.pow(OBJ_COEFF_BASE, exp--));
            }
        }

        private void recordValues() {
            valueIsSelected = (int) Math.round(varIsSelected.solutionValue());
            valueOrderNumber = (int) Math.round(varOrderNumber.solutionValue());
        }

        public boolean isForward() { return isForward; }

        public boolean isBackward() { return !isForward; }
    }

    static class Equal extends PlannerEdge {

        Equal(PlannerVertex<?> from, PlannerVertex<?> to) {
            super(from, to);
        }

        @Override
        protected void initialiseDirectionalEdges() {
            forward = new Directional(from, to, this, true);
            backward = new Directional(to, from, this, false);
        }

        class Directional extends PlannerEdge.Directional {

            Directional(PlannerVertex<?> from, PlannerVertex<?> to, Equal parent, boolean isForward) {
                super(from, to, parent, isForward);
            }

            @Override
            void updateObjective(SchemaGraph graph) {
                setObjectiveCoefficient(0);
            }
        }
    }

    static class Predicate extends PlannerEdge {

        private final GraqlToken.Predicate.Equality predicate;

        Predicate(PlannerVertex<?> from, PlannerVertex<?> to, GraqlToken.Predicate.Equality predicate) {
            super(from, to);
            this.predicate = predicate;
        }

        @Override
        protected void initialiseDirectionalEdges() {
            forward = new Directional(from, to, this, true);
            backward = new Directional(to, from, this, false);
        }

        class Directional extends PlannerEdge.Directional {

            Directional(PlannerVertex<?> from, PlannerVertex<?> to, Predicate parent, boolean isForward) {
                super(from, to, parent, isForward);
            }

            public GraqlToken.Predicate predicate() {
                return predicate;
            }

            @Override
            void updateObjective(SchemaGraph graph) {
                if (predicate.equals(GraqlToken.Predicate.Equality.EQ)) {
                    if (!to.asThing().properties().types().isEmpty()) {
                        setObjectiveCoefficient(to.asThing().properties().types().size());
                    } else {
                        // TODO: we can narrow down the attribute type by its value type
                        setObjectiveCoefficient(graph.countAttributeTypes());
                    }
                } else if (!to.asThing().properties().types().isEmpty()) {
                    setObjectiveCoefficient(graph.countInstances(to.asThing().properties().types(), true));
                } else {
                    // TODO: we can narrow down the attribute type by its value type
                    setObjectiveCoefficient(graph.rootAttributeType().instancesCountTransitive());
                }
            }
        }
    }

    static abstract class Native extends PlannerEdge {

        Native(PlannerVertex<?> from, PlannerVertex<?> to) {
            super(from, to);
        }

        static PlannerEdge.Native of(PlannerVertex<?> from, PlannerVertex<?> to, StructureEdge.Native structureEdge) {
            if (structureEdge.encoding().isType()) {
                return new Type(from, to, structureEdge.encoding().asType(), structureEdge.isTransitive());
            } else if (structureEdge.encoding().isThing()) {
                return Thing.of(from, to, structureEdge);
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        static class Isa extends Native {

            private final boolean isTransitive;

            Isa(PlannerVertex<?> from, PlannerVertex<?> to, boolean isTransitive) {
                super(from, to);
                this.isTransitive = isTransitive;
            }

            @Override
            protected void initialiseDirectionalEdges() {
                forward = new Forward(from, to, this);
                backward = new Backward(to, from, this);
            }

            private class Forward extends Directional {

                private Forward(PlannerVertex<?> from, PlannerVertex<?> to, Isa parent) {
                    super(from, to, parent, true);
                }

                @Override
                void updateObjective(SchemaGraph graph) {
                    setObjectiveCoefficient(1);
                }
            }

            private class Backward extends Directional {

                private Backward(PlannerVertex<?> from, PlannerVertex<?> to, Isa parent) {
                    super(from, to, parent, false);
                }

                @Override
                void updateObjective(SchemaGraph graph) {
                    if (!to.asThing().properties().types().isEmpty()) {
                        setObjectiveCoefficient(graph.countInstances(to.asThing().properties().types(), isTransitive));
                    } else {
                        setObjectiveCoefficient(graph.rootThingType().instancesCountTransitive());
                    }
                }
            }
        }

        static class Type extends Native {

            private final Encoding.Edge.Type encoding;
            private final boolean isTransitive;

            Type(PlannerVertex<?> from, PlannerVertex<?> to, Encoding.Edge.Type encoding, boolean isTransitive) {
                super(from, to);
                this.encoding = encoding;
                this.isTransitive = isTransitive;
            }

            @Override
            protected void initialiseDirectionalEdges() {
                forward = new Forward(from, to, this);
                backward = new Backward(to, from, this);
            }

            private class Forward extends Directional {

                private Forward(PlannerVertex<?> from, PlannerVertex<?> to, Type parent) {
                    super(from, to, parent, true);
                }

                @Override
                void updateObjective(SchemaGraph graph) {
                    if (encoding == SUB) {
                        setObjectiveCoefficient(1);
                    } else if (!to.asType().properties().labels().isEmpty()) {
                        setObjectiveCoefficient(to.asType().properties().labels().size());
                    } else if (encoding == OWNS || encoding == OWNS_KEY) {
                        setObjectiveCoefficient(graph.countAttributeTypes());
                    } else if (encoding == PLAYS || encoding == RELATES) {
                        setObjectiveCoefficient(graph.countRoleTypes());
                    }
                }
            }

            private class Backward extends Directional {

                private Backward(PlannerVertex<?> from, PlannerVertex<?> to, Type parent) {
                    super(from, to, parent, false);
                }

                @Override
                void updateObjective(SchemaGraph graph) {
                    if (encoding == SUB && !to.asType().properties().labels().isEmpty()) {
                        setObjectiveCoefficient(graph.countSubTypes(to.asType().properties().labels(), isTransitive));
                    } else if (encoding == SUB && !from.asType().properties().labels().isEmpty()) {
                        setObjectiveCoefficient(graph.countSubTypes(from.asType().properties().labels(), isTransitive));
                    } else if (!to.asType().properties().labels().isEmpty()) {
                        setObjectiveCoefficient(to.asType().properties().labels().size());
                    } else if (encoding == RELATES) {
                        setObjectiveCoefficient(graph.countRelationTypes());
                    } else {
                        setObjectiveCoefficient(graph.countThingTypes());
                    }
                }
            }
        }

        static class Thing extends Native {

            private final Encoding.Edge.Thing encoding;

            Thing(PlannerVertex<?> from, PlannerVertex<?> to, Encoding.Edge.Thing encoding) {
                super(from, to);
                this.encoding = encoding;
            }

            static Thing of(PlannerVertex<?> from, PlannerVertex<?> to, StructureEdge.Native structureEdge) {
                Encoding.Edge.Thing encoding = structureEdge.encoding().asThing();
                if (encoding.isOptimisation()) {
                    return new RolePlayer(from, to, structureEdge.asOptimised().types());
                } else {
                    return new Thing(from, to, encoding);
                }
            }

            @Override
            protected void initialiseDirectionalEdges() {
                forward = new Forward(from, to, this);
                backward = new Backward(to, from, this);
            }

            private class Forward extends Directional {

                private Forward(PlannerVertex<?> from, PlannerVertex<?> to, Thing parent) {
                    super(from, to, parent, true);
                }

                @Override
                void updateObjective(SchemaGraph graph) {
                    if (!to.asThing().properties().types().isEmpty()) {
                        setObjectiveCoefficient(graph.countInstances(to.asThing().properties().types(), true));
                    } else if (encoding == HAS) {
                        setObjectiveCoefficient(graph.rootAttributeType().instancesCountTransitive());
                    } else if (encoding == PLAYING || encoding == RELATING) {
                        setObjectiveCoefficient(graph.rootRoleType().instancesCountTransitive());
                    } else {
                        setObjectiveCoefficient(graph.rootThingType().instancesCountTransitive());
                    }
                }
            }

            private class Backward extends Directional {

                private Backward(PlannerVertex<?> from, PlannerVertex<?> to, Thing parent) {
                    super(from, to, parent, false);
                }

                @Override
                void updateObjective(SchemaGraph graph) {
                    if (!to.asThing().properties().types().isEmpty()) {
                        setObjectiveCoefficient(graph.countInstances(to.asThing().properties().types(), true));
                    } else if (encoding == RELATING || encoding == ROLEPLAYER) {
                        setObjectiveCoefficient(graph.rootRelationType().instancesCountTransitive());
                    } else {
                        setObjectiveCoefficient(graph.rootThingType().instancesCountTransitive());
                    }
                }
            }

            static class RolePlayer extends Thing {

                private final Set<Label> roleTypes;

                RolePlayer(PlannerVertex<?> from, PlannerVertex<?> to, Set<Label> roleTypes) {
                    super(from, to, ROLEPLAYER);
                    this.roleTypes = roleTypes;
                }

                @Override
                protected void initialiseDirectionalEdges() {
                    forward = new Forward(from, to, this);
                    backward = new Backward(to, from, this);
                }

                private class Forward extends Directional {

                    private Forward(PlannerVertex<?> from, PlannerVertex<?> to, Thing parent) {
                        super(from, to, parent, true);
                    }

                    @Override
                    void updateObjective(SchemaGraph graph) {
                        double cost;
                        if (!roleTypes.isEmpty()) {
                            cost = 0;
                            for (final Label roleType : roleTypes) {
                                assert roleType.scope().isPresent();
                                cost += (double) graph.getType(roleType).instancesCountTransitive() /
                                        graph.getType(roleType.scope().get()).instancesCountTransitive();
                            }
                            cost = cost / roleTypes.size();
                        } else {
                            cost = (double) graph.rootRoleType().instancesCountTransitive() /
                                    graph.rootRelationType().instancesCountTransitive();
                        }
                        setObjectiveCoefficient(cost);
                    }
                }

                private class Backward extends Directional {

                    private Backward(PlannerVertex<?> from, PlannerVertex<?> to, Thing parent) {
                        super(from, to, parent, false);
                    }

                    @Override
                    void updateObjective(SchemaGraph graph) {
                        double cost;
                        if (!roleTypes.isEmpty() && !from.asThing().properties().types().isEmpty()) {
                            cost = (double) graph.countInstances(roleTypes, true) /
                                    graph.countInstances(from.asThing().properties().types(), true);
                        } else {
                            cost = (double) graph.rootRoleType().instancesCountTransitive() /
                                    graph.rootThingType().instancesCountTransitive();
                        }
                        setObjectiveCoefficient(cost);
                    }
                }
            }
        }
    }
}
