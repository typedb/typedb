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
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.optimiser.OptimiserConstraint;
import com.vaticle.typedb.core.common.optimiser.OptimiserVariable;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;

import javax.annotation.Nullable;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.traversal.planner.GraphPlanner.INIT_ZERO;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;

public abstract class PlannerVertex<PROPERTIES extends TraversalVertex.Properties>
        extends TraversalVertex<PlannerEdge.Directional<?, ?>, PROPERTIES> {

    final GraphPlanner planner;

    private final String varPrefix = "vertex_var_" + id() + "_";
    private final String conPrefix = "vertex_con_" + id() + "_";
    private boolean isInitialisedVariables;
    private boolean isInitialisedConstraints;
    private double costNext;
    double costLastRecorded;
    OptimiserVariable.Boolean varIsStartingVertex;
    OptimiserVariable.Boolean varIsEndingVertex;
    OptimiserVariable.Boolean varHasIncomingEdges;
    OptimiserVariable.Boolean varHasOutgoingEdges;

    PlannerVertex(Identifier identifier, @Nullable GraphPlanner planner) {
        super(identifier);
        this.planner = planner;
        isInitialisedVariables = false;
        isInitialisedConstraints = false;
        costLastRecorded = 0.01; // non-zero value for safe division
    }

    abstract void updateObjective(GraphManager graph);

    public boolean isStartingVertex() {
        return varIsStartingVertex.solutionValue();
    }

    public boolean isEndingVertex() {
        return varIsEndingVertex.solutionValue();
    }

    public boolean hasIncomingEdges() {
        return varHasIncomingEdges.solutionValue();
    }

    public boolean hasOutgoingEdges() {
        return varHasOutgoingEdges.solutionValue();
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
        varIsStartingVertex = planner.optimiser().booleanVar(varPrefix + "is_starting_vertex");
        varIsEndingVertex = planner.optimiser().booleanVar(varPrefix + "is_ending_vertex");
        varHasIncomingEdges = planner.optimiser().booleanVar(varPrefix + "has_incoming_edges");
        varHasOutgoingEdges = planner.optimiser().booleanVar(varPrefix + "has_outgoing_edges");

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
        OptimiserConstraint conHasIncomingEdges = planner.optimiser().constraint(0, ins().size() - 1, conPrefix + "has_incoming_edges");
        conHasIncomingEdges.setCoefficient(varHasIncomingEdges, ins().size());
        ins().forEach(edge -> conHasIncomingEdges.setCoefficient(edge.varIsSelected, -1));
    }

    private void initialiseConstraintsForOutgoingEdges() {
        assert !outs().isEmpty();
        OptimiserConstraint conHasOutgoingEdges = planner.optimiser().constraint(0, outs().size() - 1, conPrefix + "has_outgoing_edges");
        conHasOutgoingEdges.setCoefficient(varHasOutgoingEdges, outs().size());
        outs().forEach(edge -> conHasOutgoingEdges.setCoefficient(edge.varIsSelected, -1));
    }

    private void initialiseConstraintsForVertexFlow() {
        OptimiserConstraint conStartOrIncoming = planner.optimiser().constraint(1, 1, conPrefix + "starting_or_incoming");
        conStartOrIncoming.setCoefficient(varIsStartingVertex, 1);
        conStartOrIncoming.setCoefficient(varHasIncomingEdges, 1);

        OptimiserConstraint conEndingOrOutgoing = planner.optimiser().constraint(1, 1, conPrefix + "ending_or_outgoing");
        conEndingOrOutgoing.setCoefficient(varIsEndingVertex, 1);
        conEndingOrOutgoing.setCoefficient(varHasOutgoingEdges, 1);
    }

    protected void setObjectiveCoefficient(double cost) {
        assert !Double.isNaN(cost);
        if (cost < INIT_ZERO) cost = INIT_ZERO;
        double exp = planner.edges().size() * planner.costExponentUnit;
        double coeff = cost * Math.pow(planner.branchingFactor, exp);
        planner.optimiser().setObjectiveCoefficient(varIsStartingVertex, coeff);
        costNext = cost;
        planner.updateCostNext(costLastRecorded, costNext);
    }

    void recordCost() {
        costLastRecorded = costNext;
    }

    boolean validResults() {
        return !(isStartingVertex() && isEndingVertex()) && (isStartingVertex() ^ hasIncomingEdges())
                && (isEndingVertex() ^ hasOutgoingEdges());
    }

    void resetInitialValue() {
        varIsStartingVertex.clearInitial();
        varIsEndingVertex.clearInitial();
        varHasIncomingEdges.clearInitial();
        varHasOutgoingEdges.clearInitial();
    }

    void setStartingVertexInitial() {
        varIsStartingVertex.setInitial(true);
        varIsEndingVertex.setInitial(false);
        varHasIncomingEdges.setInitial(false);
    }

    void setEndingVertexInitial() {
        varIsEndingVertex.setInitial(true);
        varIsStartingVertex.setInitial(false);
        varHasOutgoingEdges.setInitial(false);
        assert varHasIncomingEdges.getInitial() == 1;
    }

    void setHasOutgoingEdgesInitial() {
        varHasOutgoingEdges.setInitial(true);
        varIsEndingVertex.setInitial(false);
    }

    void setHasIncomingEdgesInitial() {
        varHasIncomingEdges.setInitial(true);
        varIsStartingVertex.setInitial(false);
    }

    public PlannerVertex.Thing asThing() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.class));
    }

    public PlannerVertex.Type asType() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Type.class));
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
                FunctionalIterator<TypeVertex> attTypes = iterate(props().predicates())
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
        public boolean isThing() {
            return true;
        }

        @Override
        public PlannerVertex.Thing asThing() {
            return this;
        }
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
                setObjectiveCoefficient(graph.schema().stats().abstractTypeCount());
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
        public boolean isType() {
            return true;
        }

        @Override
        public PlannerVertex.Type asType() {
            return this;
        }
    }
}
