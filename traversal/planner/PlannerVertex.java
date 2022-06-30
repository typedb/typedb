/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;

import javax.annotation.Nullable;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.traversal.planner.GraphPlanner.INIT_ZERO;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;
import static java.lang.Math.log;
import static java.lang.Math.max;

public abstract class PlannerVertex<PROPERTIES extends TraversalVertex.Properties>
        extends TraversalVertex<PlannerEdge.Directional<?, ?>, PROPERTIES> {

    final GraphPlanner planner;

    private final String varPrefix = "vertex_var_" + id() + "_";
    private final String conPrefix = "vertex_con_" + id() + "_";
    private boolean didInitialiseVariables;
    private boolean didInitialiseConstraints;
    private boolean didInitialiseValues;

    double cost;
    double recordedCost;

    OptimiserVariable.Boolean varIsStartingVertex;
    OptimiserVariable.Boolean[] varOrderAssignment;
    OptimiserVariable.Integer varOrderNumber;

    PlannerVertex(Identifier identifier, @Nullable GraphPlanner planner) {
        super(identifier);
        this.planner = planner;
        didInitialiseVariables = false;
        didInitialiseConstraints = false;
        didInitialiseValues = false;
        recordedCost = 0.01; // non-zero value for safe division
    }

    abstract void computeCost(GraphManager graphMgr);

    public double cost() {
        return max(cost, INIT_ZERO);
    }

    public boolean isStartingVertex() {
        return varIsStartingVertex.value();
    }

    public boolean isEndingVertex() {
        return !hasOutgoingEdges();
    }

    public boolean hasIncomingEdges() {
        return !isStartingVertex();
    }

    public boolean hasOutgoingEdges() {
        return iterate(outs()).anyMatch(PlannerEdge.Directional::isSelected);
    }

    public int getOrder() {
        return varOrderNumber.value();
    }

    public boolean didInitialiseVariables() {
        return didInitialiseVariables;
    }

    public boolean didInitialiseConstraints() {
        return didInitialiseConstraints;
    }

    public boolean didInitialiseValues() {
        return didInitialiseValues;
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

    void loop(PlannerEdge<?, ?> edge) {
        assert edge.forward().from().equals(edge.forward().to());
        assert edge.backward().from().equals(edge.forward().to());
        loop(edge.forward());
        loop(edge.backward());
    }

    void initialiseVariables() {
        assert planner != null;
        varIsStartingVertex = planner.optimiser().booleanVar(varPrefix + "is_starting_vertex");
        varOrderNumber = planner.optimiser().intVar(0, planner.vertices().size() - 1, varPrefix + "order_number");
        varOrderAssignment = new OptimiserVariable.Boolean[planner.vertices().size()];
        for (int i = 0; i < planner.vertices().size(); i++) {
            varOrderAssignment[i] = planner.optimiser().booleanVar(varPrefix + "order_assignment[" + i + "]");
        }
        didInitialiseVariables = true;
    }

    void initialiseConstraints() {
        assert ins().stream().allMatch(PlannerEdge.Directional::didInitialiseVariables);
        assert outs().stream().allMatch(PlannerEdge.Directional::didInitialiseVariables);
        assert loops().stream().allMatch(PlannerEdge.Directional::didInitialiseVariables);

        OptimiserConstraint conIsOrdered = planner.optimiser().constraint(1, 1, conPrefix + "ordered");
        OptimiserConstraint conAssignOrderNumber = planner.optimiser().constraint(0, 0, conPrefix + "assign_order_number");
        conAssignOrderNumber.setCoefficient(varOrderNumber, -1);
        for (int i = 0; i < planner.vertices().size(); i++) {
            conIsOrdered.setCoefficient(varOrderAssignment[i], 1);
            conAssignOrderNumber.setCoefficient(varOrderAssignment[i], i);
        }

        int numIns = ins().size();
        OptimiserConstraint conIsStartingVertex = planner.optimiser().constraint(1, numIns, conPrefix + "is_starting_vertex");
        conIsStartingVertex.setCoefficient(varIsStartingVertex, numIns);
        for (PlannerEdge.Directional<?, ?> edge : ins()) conIsStartingVertex.setCoefficient(edge.varIsSelected, 1);

        didInitialiseConstraints = true;
    }

    protected void updateOptimiserCoefficients() {
        assert !Double.isNaN(recordedCost);
        planner.optimiser().setObjectiveCoefficient(varIsStartingVertex, log(1 + recordedCost));
    }

    void recordCost() {
        recordedCost = cost();
    }

    boolean validResults() {
        return !(isStartingVertex() && isEndingVertex()) && (isStartingVertex() ^ hasIncomingEdges())
                && (isEndingVertex() ^ hasOutgoingEdges());
    }

    void setOrderInitial(int order) {
        varOrderNumber.setInitial(order);
        for (int i = 0; i < planner.vertices().size(); i++) varOrderAssignment[i].setInitial(order == i);
    }

    void inferStartingVertexFromOrder() {
        assert varOrderNumber.hasInitial();
        double initialOrder = varOrderNumber.initial();
        varIsStartingVertex.setInitial(outs().stream().allMatch(e -> e.to().varOrderNumber.initial() > initialOrder));
        didInitialiseValues = true;
    }

    void setOutgoingEdgesInitialValues() {
        double initialOrder = varOrderNumber.initial();
        outs().forEach(e -> e.setInitialSelected(e.to().varOrderNumber.initial() > initialOrder));
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
        if (didInitialiseValues) {
            if (isStartingVertex()) string += " (start)";
            else if (isEndingVertex()) string += " (end)";
        }
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
        void computeCost(GraphManager graphMgr) {
            if (props().hasIID()) {
                cost = 1;
            } else {
                assert !props().types().isEmpty();
                if (iterate(props().predicates()).anyMatch(p -> p.operator().equals(EQ))) {
                    cost = props().types().size();
                } else {
                    cost = graphMgr.data().stats().thingVertexSum(props().types());
                }
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
        void computeCost(GraphManager graphMgr) {
            if (!props().labels().isEmpty()) {
                cost = props().labels().size();
            } else if (props().isAbstract()) {
                cost = graphMgr.schema().stats().abstractTypeCount();
            } else if (!props().valueTypes().isEmpty()) {
                int count = 0;
                for (Encoding.ValueType valueType : props().valueTypes()) {
                    count += graphMgr.schema().stats().attTypesWithValueType(valueType);
                }
                cost = count;
            } else if (props().regex().isPresent()) {
                cost = 1;
            } else {
                cost = graphMgr.schema().stats().typeCount();
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
