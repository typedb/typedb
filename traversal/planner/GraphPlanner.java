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
import com.vaticle.typedb.core.common.optimiser.Optimiser;
import com.vaticle.typedb.core.common.optimiser.OptimiserConstraint;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalEdge;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typedb.core.traversal.structure.StructureEdge;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNEXPECTED_PLANNING_ERROR;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.lang.Math.abs;
import static java.time.Duration.between;
import static java.util.Comparator.comparing;

public class GraphPlanner implements Planner {

    private static final Logger LOG = LoggerFactory.getLogger(GraphPlanner.class);

    static final long DEFAULT_TIME_LIMIT_MILLIS = 100;
    static final long HIGHER_TIME_LIMIT_MILLIS = 200;
    static final double OBJECTIVE_PLANNER_COST_MAX_CHANGE = 0.2;
    static final double OBJECTIVE_VARIABLE_COST_MAX_CHANGE = 2.0;
    static final double OBJECTIVE_VARIABLE_TO_PLANNER_COST_MIN_CHANGE = 0.02;
    static final double INIT_ZERO = 0.01;

    private final Optimiser optimiser;
    private final Map<Identifier, PlannerVertex<?>> vertices;
    private final Set<PlannerEdge<?, ?>> edges;
    private final AtomicBoolean isOptimising;
    private final ReadWriteLock firstOptimisingLock;

    protected volatile GraphProcedure procedure;
    private volatile boolean isUpToDate;
    private volatile long totalDuration;
    private volatile long snapshot;

    private volatile double totalCostLastRecorded;
    private double totalCost;

    private GraphPlanner() {
        optimiser = new Optimiser();
        vertices = new HashMap<>();
        edges = new HashSet<>();
        isOptimising = new AtomicBoolean(false);
        firstOptimisingLock = new StampedLock().asReadWriteLock();
        isUpToDate = false;
        totalDuration = 0L;
        totalCostLastRecorded = INIT_ZERO;
        totalCost = INIT_ZERO;
        snapshot = -1L;
    }

    static GraphPlanner create(Structure structure) {
        assert structure.vertices().size() > 1;
        GraphPlanner planner = new GraphPlanner();
        Set<StructureVertex<?>> registeredVertices = new HashSet<>();
        Set<StructureEdge<?, ?>> registeredEdges = new HashSet<>();
        structure.vertices().forEach(vertex -> planner.registerVertex(vertex, registeredVertices, registeredEdges));
        assert planner.vertices().size() > 1 && !planner.edges().isEmpty();
        planner.initialise();
        return planner;
    }

    private void registerVertex(StructureVertex<?> structureVertex, Set<StructureVertex<?>> registeredVertices,
                                Set<StructureEdge<?, ?>> registeredEdges) {
        if (registeredVertices.contains(structureVertex)) return;
        registeredVertices.add(structureVertex);
        List<StructureVertex<?>> adjacents = new ArrayList<>();
        PlannerVertex<?> vertex = vertex(structureVertex);
        if (vertex.isThing()) vertex.asThing().props(structureVertex.asThing().props());
        else vertex.asType().props(structureVertex.asType().props());
        structureVertex.outs().forEach(structureEdge -> {
            if (!registeredEdges.contains(structureEdge)) {
                registeredEdges.add(structureEdge);
                adjacents.add(structureEdge.to());
                registerEdge(structureEdge);
            }
        });
        structureVertex.ins().forEach(structureEdge -> {
            if (!registeredEdges.contains(structureEdge)) {
                registeredEdges.add(structureEdge);
                adjacents.add(structureEdge.from());
                registerEdge(structureEdge);
            }
        });
        structureVertex.loops().forEach(structureEdge -> {
            if (!registeredEdges.contains(structureEdge)) {
                registeredEdges.add(structureEdge);
                registerEdge(structureEdge);
            }
        });
        adjacents.forEach(v -> registerVertex(v, registeredVertices, registeredEdges));
    }

    private void registerEdge(StructureEdge<?, ?> structureEdge) {
        PlannerVertex<?> from = vertex(structureEdge.from());
        PlannerVertex<?> to = vertex(structureEdge.to());
        PlannerEdge<?, ?> edge = PlannerEdge.of(from, to, structureEdge);
        edges.add(edge);
        if (from.equals(to)) from.loop(edge);
        else {
            from.out(edge);
            to.in(edge);
        }
    }

    private PlannerVertex<?> vertex(StructureVertex<?> structureVertex) {
        if (structureVertex.isThing()) return thingVertex(structureVertex.asThing());
        else return typeVertex(structureVertex.asType());
    }

    private PlannerVertex.Thing thingVertex(StructureVertex.Thing structureVertex) {
        return vertices.computeIfAbsent(
                structureVertex.id(), i -> new PlannerVertex.Thing(i, this)
        ).asThing();
    }

    private PlannerVertex.Type typeVertex(StructureVertex.Type structureVertex) {
        return vertices.computeIfAbsent(
                structureVertex.id(), i -> new PlannerVertex.Type(i, this)
        ).asType();
    }

    private void initialise() {
        createVariables();
        createConstraints();
    }

    private void createVariables() {
        vertices.values().forEach(PlannerVertex::createVariables);
        edges.forEach(PlannerEdge::createVariables);
    }

    private void createConstraints() {
        String conPrefix = "planner_con_";
        for (int i = 0; i < vertices.size(); i++) {
            OptimiserConstraint conOneVertexAtOrderI = optimiser.constraint(1, 1, conPrefix + "one_vertex_at_order_" + i);
            for (PlannerVertex<?> vertex : vertices.values()) {
                conOneVertexAtOrderI.setCoefficient(vertex.varOrderAssignment[i], 1);
            }
        }
        vertices.values().forEach(PlannerVertex::createConstraints);
        edges.forEach(PlannerEdge::createConstraints);
    }

    @Override
    public GraphProcedure procedure() {
        assert procedure != null;
        return procedure;
    }

    @Override
    public boolean isGraph() {
        return true;
    }

    @Override
    public GraphPlanner asGraph() {
        return this;
    }

    public Collection<PlannerVertex<?>> vertices() {
        return vertices.values();
    }

    public Set<PlannerEdge<?, ?>> edges() {
        return edges;
    }

    void setOutOfDate() {
        isUpToDate = false;
    }

    private boolean isUpToDate() {
        return isUpToDate;
    }

    private boolean isPlanned() {
        return optimiser.isFeasible() || optimiser.isOptimal();
    }

    @Override
    public boolean isOptimal() {
        return optimiser.isOptimal();
    }

    private boolean isError() {
        return optimiser.isError();
    }

    Optimiser optimiser() {
        return optimiser;
    }

    void mayOptimise(GraphManager graphMgr, boolean singleUse) {
        if (procedure == null) {
            try {
                firstOptimisingLock.writeLock().lock();
                if (procedure == null) optimise(graphMgr, singleUse);
                assert procedure != null;
            } finally {
                firstOptimisingLock.writeLock().unlock();
            }
        } else if (isOptimising.compareAndSet(false, true)) {
            try {
                optimise(graphMgr, singleUse);
            } finally {
                isOptimising.set(false);
            }
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void optimise(GraphManager graphMgr, boolean singleUse) {
        updateTraversalCosts(graphMgr);
        if (isUpToDate() && isOptimal()) {
            if (LOG.isDebugEnabled()) LOG.debug("GraphPlanner still optimal and up-to-date");
            return;
        }
        updateOptimiserCoefficients();

        if (!isPlanned()) setInitialValues();

        // TODO: we should have a more clever logic to allocate extra time
        long allocatedDuration = singleUse ? HIGHER_TIME_LIMIT_MILLIS : DEFAULT_TIME_LIMIT_MILLIS;
        Instant start, endSolver, end;
        totalDuration += allocatedDuration;

        start = Instant.now();
        optimiser.optimise(totalDuration);
        endSolver = Instant.now();
        if (isError()) throwPlanningError();
        else assert isPlanned();

        createProcedure();
        end = Instant.now();

        isUpToDate = true;
        totalDuration -= allocatedDuration - between(start, endSolver).toMillis();
        printDebug(start, endSolver, end);
    }

    private void updateOptimiserCoefficients() {
        vertices.values().forEach(PlannerVertex::updateOptimiserCoefficients);
        edges.forEach(PlannerEdge::updateOptimiserCoefficients);

        if (LOG.isTraceEnabled()) LOG.trace(optimiser.toString());
    }

    private void updateTraversalCosts(GraphManager graphMgr) {
        if (snapshot < graphMgr.data().stats().snapshot()) {
            // TODO: we should not include the graph's uncommitted writes, but only the persisted counts in the costs
            snapshot = graphMgr.data().stats().snapshot();
            computeTotalCost(graphMgr);

            if (!isUpToDate) {
                totalCostLastRecorded = totalCost;
                vertices.values().forEach(PlannerVertex::recordCost);
                edges.forEach(PlannerEdge::recordCost);
            }
        }
    }

    private void computeTotalCost(GraphManager graphMgr) {
        vertices.values().forEach(v -> {
            v.computeCost(graphMgr);
            if (costChangeSignificant(v)) setOutOfDate();
        });
        edges.forEach(e -> {
            e.computeCost(graphMgr);
            if (costChangeSignificant(e)) setOutOfDate();
        });

        double vertexCost = iterate(vertices.values()).map(PlannerVertex::safeCost).reduce(0.0, Double::sum);
        double edgeCost = iterate(edges).map(e -> e.forward.safeCost() + e.backward.safeCost()).reduce(0.0, Double::sum);
        totalCost = vertexCost + edgeCost;
        if (totalCostChangeSignificant()) setOutOfDate();
    }

    private boolean costChangeSignificant(PlannerVertex<?> vertex) {
        return costChangeSignificant(vertex.costLastRecorded, vertex.safeCost());
    }

    private boolean costChangeSignificant(PlannerEdge<?, ?> edge) {
        return costChangeSignificant(edge.forward) || costChangeSignificant(edge.backward);
    }

    private boolean costChangeSignificant(PlannerEdge.Directional<?, ?> edge) {
        return costChangeSignificant(edge.costLastRecorded, edge.safeCost());
    }

    private boolean costChangeSignificant(double costPrevious, double costNext) {
        assert totalCostLastRecorded > 0;
        assert costPrevious > 0;
        assert costNext > 0;

        return (costNext / costPrevious >= OBJECTIVE_VARIABLE_COST_MAX_CHANGE ||
                costNext / costPrevious <= 1.0 / OBJECTIVE_VARIABLE_COST_MAX_CHANGE) &&
                abs(costNext - costPrevious) / totalCostLastRecorded >= OBJECTIVE_VARIABLE_TO_PLANNER_COST_MIN_CHANGE;
    }

    private boolean totalCostChangeSignificant() {
        assert totalCostLastRecorded > 0;
        return abs((totalCost / totalCostLastRecorded) - 1) >= OBJECTIVE_PLANNER_COST_MAX_CHANGE;
    }

    private void throwPlanningError() {
        LOG.error(toString());
        LOG.error("Optimisation status: {}", optimiser.status());
        LOG.error(optimiser.toString());
        throw TypeDBException.of(UNEXPECTED_PLANNING_ERROR);
    }

    private void printDebug(Instant start, Instant endSolver, Instant end) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Optimisation status         : {}", optimiser.status().name());
            LOG.debug("Objective function value    : {}", optimiser.objectiveValue());
            LOG.debug("Solver duration             : {} (ms)", between(start, endSolver).toMillis());
            LOG.debug("Procedure creation duration : {} (ms)", between(endSolver, end).toMillis());
            LOG.debug("Total duration ------------ : {} (ms)", between(start, end).toMillis());
        }
    }

    private void createProcedure() {
        assert iterate(vertices.values()).allMatch(PlannerVertex::validResults);
        procedure = GraphProcedure.create(this);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Graph Planner: {");
        List<PlannerEdge<?, ?>> plannerEdges = new ArrayList<>(edges);
        plannerEdges.sort(Comparator.comparing(TraversalEdge::toString));
        List<PlannerVertex<?>> plannerVertices = new ArrayList<>(vertices.values());
        plannerVertices.sort(Comparator.comparing(v -> v.id().toString()));

        str.append("\n\tvertices:");
        for (PlannerVertex<?> v : plannerVertices) {
            str.append("\n\t\t").append(v);
        }
        str.append("\n\tedges:");
        for (PlannerEdge<?, ?> e : plannerEdges) {
            str.append("\n\t\t").append(e);
        }
        str.append("\n}");
        return str.toString();
    }

    private void setInitialValues() {
        Set<PlannerVertex<?>> unorderedVertices = new HashSet<>(vertices.values());
        int vertexOrder = 0;
        while (!unorderedVertices.isEmpty()) {
            PlannerVertex<?> vertex = unorderedVertices.stream().min(comparing(
                    v -> v.ins().stream().filter(e -> !unorderedVertices.contains(e.from()))
                            .mapToDouble(PlannerEdge.Directional::safeCost).min().orElse(v.safeCost())
            )).get();
            unorderedVertices.remove(vertex);
            vertex.setOrderInitial(vertexOrder++);
        }
        assert vertexOrder == vertices.size();

        vertices.values().forEach(PlannerVertex::initialise);
        edges.forEach(PlannerEdge::initialise);
    }
}
