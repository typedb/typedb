/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.graph.TraversalVertex;

import javax.annotation.Nullable;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.traversal.common.Predicate.Operator.Equality.EQ;

public abstract class PlannerVertex<PROPERTIES extends TraversalVertex.Properties>
        extends TraversalVertex<PlannerEdge.Directional<?, ?>, PROPERTIES> {

    final GraphPlanner planner;

    private final String varPrefix = "vertex_var_" + id() + "_";
    private final String conPrefix = "vertex_con_" + id() + "_";
    private int valueIsStartingVertex;
    private int valueIsEndingVertex;
    private int valueHasIncomingEdges;
    private int valueHasOutgoingEdges;
    private boolean isInitialisedVariables;
    private boolean isInitialisedConstraints;
    private double costPrevious;
    private double costNext;
    MPVariable varIsStartingVertex;
    MPVariable varIsEndingVertex;
    MPVariable varHasIncomingEdges;
    MPVariable varHasOutgoingEdges;

    PlannerVertex(Identifier identifier, @Nullable GraphPlanner planner) {
        super(identifier);
        this.planner = planner;
        isInitialisedVariables = false;
        isInitialisedConstraints = false;
        costPrevious = 0.01; // non-zero value for safe division
    }

    abstract void updateObjective(GraphManager graph);

    public boolean isStartingVertex() {
        return valueIsStartingVertex == 1;
    }

    public boolean isEndingVertex() {
        return valueIsEndingVertex == 1;
    }

    public boolean hasIncomingEdges() {
        return valueHasIncomingEdges == 1;
    }

    public boolean hasOutgoingEdges() {
        return valueHasOutgoingEdges == 1;
    }

    public boolean isInitialisedVariables() {
        return isInitialisedVariables;
    }

    public boolean isInitialisedConstraints() {
        return isInitialisedConstraints;
    }

    void out(PlannerEdge<?, ?> edge) {
        assert edge.forward().from().equals(this);
        assert edge.backward().to().equals(this);
        out(edge.forward());
        in(edge.backward());
    }

    void in(PlannerEdge<?, ?> edge) {
        assert edge.forward().to().equals(this);
        assert edge.backward().from().equals(this);
        in(edge.forward());
        out(edge.backward());
    }

    void initialiseVariables() {
        assert planner != null;
        varIsStartingVertex = planner.solver().makeIntVar(0, 1, varPrefix + "is_starting_vertex");
        varIsEndingVertex = planner.solver().makeIntVar(0, 1, varPrefix + "is_ending_vertex");
        varHasIncomingEdges = planner.solver().makeIntVar(0, 1, varPrefix + "has_incoming_edges");
        varHasOutgoingEdges = planner.solver().makeIntVar(0, 1, varPrefix + "has_outgoing_edges");

        isInitialisedVariables = true;
    }

    void initialiseConstraints() {
        assert ins().stream().allMatch(PlannerEdge.Directional::isInitialisedVariables);
        assert outs().stream().allMatch(PlannerEdge.Directional::isInitialisedVariables);
        initialiseConstraintsForIncomingEdges();
        initialiseConstraintsForOutgoingEdges();
        initialiseConstraintsForVertexFlow();
        isInitialisedConstraints = true;
    }

    private void initialiseConstraintsForIncomingEdges() {
        assert !ins().isEmpty();
        MPConstraint conHasIncomingEdges = planner.solver().makeConstraint(0, ins().size() - 1, conPrefix + "has_incoming_edges");
        conHasIncomingEdges.setCoefficient(varHasIncomingEdges, ins().size());
        ins().forEach(edge -> conHasIncomingEdges.setCoefficient(edge.varIsSelected, -1));
    }

    private void initialiseConstraintsForOutgoingEdges() {
        assert !outs().isEmpty();
        MPConstraint conHasOutgoingEdges = planner.solver().makeConstraint(0, outs().size() - 1, conPrefix + "has_outgoing_edges");
        conHasOutgoingEdges.setCoefficient(varHasOutgoingEdges, outs().size());
        outs().forEach(edge -> conHasOutgoingEdges.setCoefficient(edge.varIsSelected, -1));
    }

    private void initialiseConstraintsForVertexFlow() {
        MPConstraint conStartOrIncoming = planner.solver().makeConstraint(1, 1, conPrefix + "starting_or_incoming");
        conStartOrIncoming.setCoefficient(varIsStartingVertex, 1);
        conStartOrIncoming.setCoefficient(varHasIncomingEdges, 1);

        MPConstraint conEndingOrOutgoing = planner.solver().makeConstraint(1, 1, conPrefix + "ending_or_outgoing");
        conEndingOrOutgoing.setCoefficient(varIsEndingVertex, 1);
        conEndingOrOutgoing.setCoefficient(varHasOutgoingEdges, 1);
    }

    protected void setObjectiveCoefficient(double cost) {
        assert !Double.isNaN(cost);
        double exp = planner.edges().size() * planner.costExponentUnit;
        double coeff = cost * Math.pow(planner.branchingFactor, exp);
        planner.objective().setCoefficient(varIsStartingVertex, coeff);
        costNext = cost;
        planner.updateCostNext(costPrevious, costNext);
    }

    void recordCost() {
        if (costNext == 0) costNext = 0.01;
        costPrevious = costNext;
    }

    void recordValues() {
        valueIsStartingVertex = (int) Math.round(varIsStartingVertex.solutionValue());
        valueIsEndingVertex = (int) Math.round(varIsEndingVertex.solutionValue());
        valueHasIncomingEdges = (int) Math.round(varHasIncomingEdges.solutionValue());
        valueHasOutgoingEdges = (int) Math.round(varHasOutgoingEdges.solutionValue());
        assert !(isStartingVertex() && isEndingVertex());
        assert (isStartingVertex() ^ hasIncomingEdges());
        assert (isEndingVertex() ^ hasOutgoingEdges());
    }

    public void setStartingVertex() {
        valueIsStartingVertex = 1;
        valueIsEndingVertex = 0;
    }

    public void setHasOutGoingEdges() {
        valueHasOutgoingEdges = 1;
    }

    public void setHasIncomingEdges() {
        valueHasIncomingEdges = 1;
    }

    public PlannerVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.class));
    }

    public PlannerVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.class));
    }

    @Override
    public String toString() {
        String string = super.toString();
        if (isStartingVertex()) string += " (start)";
        else if (isEndingVertex()) string += " (end)";
        return string;
    }

    public static class Thing extends PlannerVertex<Properties.Thing> {

        Thing(Identifier id) {
            this(id, null);
        }

        Thing(Identifier identifier, @Nullable GraphPlanner planner) {
            super(identifier, planner);
        }

        @Override
        protected Properties.Thing newProperties() {
            return new Properties.Thing();
        }

        @Override
        void updateObjective(GraphManager graph) {
            if (props().hasIID()) {
                setObjectiveCoefficient(1);
            } else if (!props().types().isEmpty()) {
                if (iterate(props().predicates()).anyMatch(p -> p.operator().equals(EQ))) {
                    setObjectiveCoefficient(props().types().size());
                } else {
                    setObjectiveCoefficient(graph.data().stats().thingVertexSum(props().types()));
                }
            } else if (!props().predicates().isEmpty()) {
                ResourceIterator<TypeVertex> attTypes = iterate(props().predicates())
                        .flatMap(p -> iterate(p.valueType().comparables()))
                        .flatMap(vt -> graph.schema().attributeTypes(vt));
                if (iterate(props().predicates()).anyMatch(p -> p.operator().equals(EQ))) {
                    setObjectiveCoefficient(attTypes.count());
                } else {
                    setObjectiveCoefficient(graph.data().stats().thingVertexSum(attTypes.stream()));
                }
            } else {
                setObjectiveCoefficient(graph.data().stats().thingVertexTransitiveCount(graph.schema().rootThingType()));
            }
        }

        @Override
        public boolean isThing() { return true; }

        @Override
        public PlannerVertex.Thing asThing() { return this; }
    }

    public static class Type extends PlannerVertex<Properties.Type> {

        Type(Identifier id) {
            this(id, null);
        }

        Type(Identifier identifier, @Nullable GraphPlanner planner) {
            super(identifier, planner);
        }

        @Override
        void updateObjective(GraphManager graph) {
            if (!props().labels().isEmpty()) {
                setObjectiveCoefficient(props().labels().size());
            } else if (props().isAbstract()) {
                setObjectiveCoefficient(graph.schema().stats().abstractThingTypeCount());
            } else if (!props().valueTypes().isEmpty()) {
                int count = 0;
                for (Encoding.ValueType valueType : props().valueTypes()) {
                    count += graph.schema().stats().attTypesWithValueType(valueType);
                }
                setObjectiveCoefficient(count);
            } else if (props().regex().isPresent()) {
                setObjectiveCoefficient(1);
            } else {
                setObjectiveCoefficient(graph.schema().stats().typeCount());
            }
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public PlannerVertex.Type asType() { return this; }
    }
}
