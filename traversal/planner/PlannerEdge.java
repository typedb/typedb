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
import grakn.core.graph.SchemaGraph;
import grakn.core.traversal.graph.TraversalEdge;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public class PlannerEdge extends TraversalEdge<PlannerVertex<?>> {

    private final Planner planner;
    private final Forward forward;
    private final Backward backward;

    PlannerEdge(TraversalEdge.Property property, PlannerVertex<?> from, PlannerVertex<?> to) {
        super(property, from, to);
        this.planner = from.planner();
        this.forward = new Forward(property, from, to, this);
        this.backward = new Backward(property, to, from, this);
        assert this.planner.equals(to.planner());
    }

    Forward forward() {
        return forward;
    }

    Backward backward() {
        return backward;
    }

    void initialiseVariables() {
        forward.initialiseVariables();
        backward.initialiseVariables();
    }

    void initialiseConstraints() {
        String conPrefix = "edge::con::" + from() + "::" + to() + "::";
        MPConstraint conOneDirection = planner.solver().makeConstraint(1, 1, conPrefix + "one_direction");
        conOneDirection.setCoefficient(forward.varIsSelected, 1);
        conOneDirection.setCoefficient(backward.varIsSelected, 1);

        forward.initialiseConstraints();
        backward.initialiseConstraints();
    }

    void updateCost(SchemaGraph schema) {
        // TODO
    }

    void recordValues() {
        forward.recordValues();
        backward.recordValues();
    }

    public abstract class Directional extends TraversalEdge<PlannerVertex<?>> {

        MPVariable varIsSelected;
        MPVariable[] varOrderAssignment;
        private MPVariable varOrderNumber;
        private int valueIsSelected;
        private int valueOrderNumber;
        private final String varPrefix;
        private final String conPrefix;
        private final PlannerEdge parent;
        private boolean isInitialisedVariables;
        private boolean isInitialisedConstraints;

        Directional(Property property, PlannerVertex<?> from, PlannerVertex<?> to, PlannerEdge parent) {
            super(property, from, to);
            this.parent = parent;
            this.isInitialisedVariables = false;
            this.isInitialisedConstraints = false;
            this.varPrefix = "edge::var::" + from() + "::" + to() + "::";
            this.conPrefix = "edge::con::" + from() + "::" + to() + "::";
        }

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
            assert from().isInitialisedVariables();
            assert to().isInitialisedConstraints();

            MPConstraint conOrderIfSelected = planner.solver().makeConstraint(0, 0, conPrefix + "order_if_selected");
            conOrderIfSelected.setCoefficient(varIsSelected, -1);

            MPConstraint conAssignOrderNumber = planner.solver().makeConstraint(0, 0, conPrefix + "assign_order_number");
            conAssignOrderNumber.setCoefficient(varOrderNumber, -1);

            for (int i = 0; i < planner.edges().size(); i++) {
                conOrderIfSelected.setCoefficient(varOrderAssignment[i], 1);
                conAssignOrderNumber.setCoefficient(varOrderAssignment[i], i + 1);
            }

            MPConstraint conOutFromVertex = planner.solver().makeConstraint(0, 1, conPrefix + "out_from_vertex");
            conOutFromVertex.setCoefficient(from().varHasOutgoingEdges, 1);
            conOutFromVertex.setCoefficient(varIsSelected, -1);

            MPConstraint conInToVertex = planner.solver().makeConstraint(0, 1, conPrefix + "in_to_vertex");
            conInToVertex.setCoefficient(to().varHasIncomingEdges, 1);
            conInToVertex.setCoefficient(varIsSelected, -1);

            to().outs().stream().filter(e -> !e.parent.equals(this.parent)).forEach(subsequentEdge -> {
                MPConstraint conOrderSequence = planner.solver().makeConstraint(0, planner.edges().size() + 1, conPrefix + "order_sequence");
                conOrderSequence.setCoefficient(to().varIsEndingVertex, planner.edges().size());
                conOrderSequence.setCoefficient(subsequentEdge.varOrderNumber, 1);
                conOrderSequence.setCoefficient(this.varIsSelected, -1);
                conOrderSequence.setCoefficient(this.varOrderNumber, -1);
            });

            isInitialisedConstraints = true;
        }

        void recordValues() {
            valueIsSelected = (int) Math.round(varIsSelected.solutionValue());
            valueOrderNumber = (int) Math.round(varOrderNumber.solutionValue());
        }

        public boolean isForward() { return false; }

        public boolean isBackward() { return false; }

        Forward asForward() {
            throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Forward.class)));
        }

        Backward asBackward() {
            throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Backward.class)));
        }
    }

    class Forward extends Directional {

        Forward(Property property, PlannerVertex<?> from, PlannerVertex<?> to, PlannerEdge parent) {
            super(property, from, to, parent);
        }

        @Override
        public boolean isForward() { return true; }

        @Override
        Forward asForward() { return this; }
    }

    class Backward extends Directional {

        Backward(Property property, PlannerVertex<?> from, PlannerVertex<?> to, PlannerEdge parent) {
            super(property, from, to, parent);
        }

        @Override
        public boolean isBackward() { return true; }

        @Override
        Backward asBackward() { return this; }
    }
}
