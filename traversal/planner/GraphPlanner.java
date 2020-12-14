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
import com.google.ortools.linearsolver.MPObjective;
import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPSolverParameters;
import grakn.core.common.concurrent.ManagedCountDownLatch;
import grakn.core.common.exception.GraknException;
import grakn.core.graph.GraphManager;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.procedure.GraphProcedure;
import grakn.core.traversal.structure.Structure;
import grakn.core.traversal.structure.StructureEdge;
import grakn.core.traversal.structure.StructureVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.ortools.linearsolver.MPSolver.ResultStatus.ABNORMAL;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.FEASIBLE;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.INFEASIBLE;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.OPTIMAL;
import static com.google.ortools.linearsolver.MPSolver.ResultStatus.UNBOUNDED;
import static com.google.ortools.linearsolver.MPSolverParameters.IncrementalityValues.INCREMENTALITY_ON;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.INCREMENTALITY;
import static com.google.ortools.linearsolver.MPSolverParameters.IntegerParam.PRESOLVE;
import static com.google.ortools.linearsolver.MPSolverParameters.PresolveValues.PRESOLVE_ON;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_PLANNING_ERROR;

public class GraphPlanner implements Planner {

    private static final Logger LOG = LoggerFactory.getLogger(GraphPlanner.class);

    static final long TIME_LIMIT_MILLIS = 100;
    static final double OBJECTIVE_PLANNER_COST_MAX_CHANGE = 0.2;
    static final double OBJECTIVE_VARIABLE_COST_MAX_CHANGE = 2.0;
    static final double OBJECTIVE_VARIABLE_TO_PLANNER_COST_MIN_CHANGE = 0.02;

    private final MPSolver solver;
    private final MPSolverParameters parameters;
    private final Map<Identifier, PlannerVertex<?>> vertices;
    private final Set<PlannerEdge<?, ?>> edges;
    private final AtomicBoolean isOptimising;
    private final ManagedCountDownLatch procedureLatch;

    protected volatile GraphProcedure procedure;
    private volatile MPSolver.ResultStatus resultStatus;
    private volatile boolean isUpToDate;
    private volatile long totalDuration;
    private volatile long snapshot;

    volatile double totalCostPrevious;
    double totalCostNext;
    double branchingFactor;

    private GraphPlanner() {
        solver = MPSolver.createSolver("SCIP");
        solver.objective().setMinimization();
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        vertices = new HashMap<>();
        edges = new HashSet<>();
        procedureLatch = new ManagedCountDownLatch(1);
        isOptimising = new AtomicBoolean(false);
        resultStatus = MPSolver.ResultStatus.NOT_SOLVED;
        isUpToDate = false;
        totalDuration = 0L;
        totalCostPrevious = 0.01;
        totalCostNext = 0.01;
        branchingFactor = 0.01;
        snapshot = 0L;
    }

    static GraphPlanner create(Structure structure) {
        GraphPlanner planner = new GraphPlanner();
        Set<StructureVertex<?>> registeredVertices = new HashSet<>();
        Set<StructureEdge<?, ?>> registeredEdges = new HashSet<>();
        structure.vertices().forEach(vertex -> planner.registerVertex(vertex, registeredVertices, registeredEdges));
        assert !planner.vertices().isEmpty() && !planner.edges().isEmpty();
        planner.initialise();
        return planner;
    }

    @Override
    public GraphProcedure procedure() {
        if (procedure == null) {
            assert isOptimising.get();
            try {
                procedureLatch.await();
                assert procedure != null;
            } catch (InterruptedException e) {
                throw GraknException.of(e);
            }
        }
        return procedure;
    }

    @Override
    public boolean isGraph() { return true; }

    @Override
    public GraphPlanner asGraph() { return this; }

    public Collection<PlannerVertex<?>> vertices() {
        return vertices.values();
    }

    public Set<PlannerEdge<?, ?>> edges() {
        return edges;
    }

    void setOutOfDate() {
        this.isUpToDate = false;
    }

    private boolean isUpToDate() {
        return isUpToDate;
    }

    private boolean isPlanned() {
        return resultStatus == FEASIBLE || resultStatus == OPTIMAL;
    }

    private boolean isOptimal() {
        return resultStatus == OPTIMAL;
    }

    private boolean isError() {
        return resultStatus == INFEASIBLE || resultStatus == UNBOUNDED || resultStatus == ABNORMAL;
    }

    MPSolver solver() {
        return solver;
    }

    MPObjective objective() {
        return solver.objective();
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
        adjacents.forEach(v -> registerVertex(v, registeredVertices, registeredEdges));
    }

    private void registerEdge(StructureEdge<?, ?> structureEdge) {
        PlannerVertex<?> from = vertex(structureEdge.from());
        PlannerVertex<?> to = vertex(structureEdge.to());
        PlannerEdge<?, ?> edge = PlannerEdge.of(from, to, structureEdge);
        edges.add(edge);
        from.out(edge);
        to.in(edge);
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
        intialiseVariables();
        initialiseConstraintsForVariables();
        initialiseConstraintsForEdges();
    }

    private void intialiseVariables() {
        vertices.values().forEach(PlannerVertex::initialiseVariables);
        edges.forEach(PlannerEdge::initialiseVariables);
    }

    private void initialiseConstraintsForVariables() {
        String conPrefix = "planner::vertex::con::";
        boolean hasPotentialStartingVertex = false;
        vertices.values().forEach(PlannerVertex::initialiseConstraints);
        MPConstraint conOneStartingVertex = solver.makeConstraint(1, 1, conPrefix + "one_starting_vertex");
        for (PlannerVertex<?> vertex : vertices.values()) {
            if (vertex.isPotentialStartingVertex) {
                conOneStartingVertex.setCoefficient(vertex.varIsStartingVertex, 1);
                hasPotentialStartingVertex = true;
            }
        }
        if (!hasPotentialStartingVertex) {
            throw GraknException.of(ILLEGAL_STATE);
        }
    }

    private void initialiseConstraintsForEdges() {
        String conPrefix = "planner::edge::con::";
        edges.forEach(PlannerEdge::initialiseConstraints);
        for (int i = 0; i < edges.size(); i++) {
            MPConstraint conOneEdgeAtOrderI = solver.makeConstraint(1, 1, conPrefix + "one_edge_at_order_" + i);
            for (PlannerEdge<?, ?> edge : edges) {
                conOneEdgeAtOrderI.setCoefficient(edge.forward().varOrderAssignment[i], 1);
                conOneEdgeAtOrderI.setCoefficient(edge.backward().varOrderAssignment[i], 1);
            }
        }
    }

    private void updateObjective(GraphManager graph) {
        if (snapshot < graph.data().stats().snapshot()) {
            snapshot = graph.data().stats().snapshot();
            totalCostNext = 0.1;
            setBranchingFactor(graph);
            computeTotalCostNext(graph);

            assert !Double.isNaN(totalCostNext) && !Double.isNaN(totalCostPrevious) && totalCostPrevious > 0;
            if (totalCostNext / totalCostPrevious >= OBJECTIVE_PLANNER_COST_MAX_CHANGE) setOutOfDate();
            if (!isUpToDate) {
                totalCostPrevious = totalCostNext;
                vertices.values().forEach(PlannerVertex::recordCost);
                edges.forEach(PlannerEdge::recordCost);
            }
        }
        LOG.trace(solver.exportModelAsLpFormat());
    }

    void updateCostNext(double costPrevious, double costNext) {
        assert !Double.isNaN(totalCostNext);
        assert !Double.isNaN(totalCostPrevious);
        assert !Double.isNaN(costPrevious);
        assert !Double.isNaN(costNext);
        assert costPrevious > 0 && totalCostPrevious > 0;

        totalCostNext += costNext;
        assert !Double.isNaN(totalCostNext);

        if (costNext / costPrevious >= OBJECTIVE_VARIABLE_COST_MAX_CHANGE &&
                costNext / totalCostPrevious >= OBJECTIVE_VARIABLE_TO_PLANNER_COST_MIN_CHANGE) {
            setOutOfDate();
        }
    }

    private void setBranchingFactor(GraphManager graph) {
        // TODO: We can refine the branching factor by not strictly considering entities being the only divisor
        double entities = graph.data().stats().thingVertexTransitiveCount(graph.schema().rootEntityType());
        double roles = graph.data().stats().thingVertexTransitiveCount(graph.schema().rootRoleType());
        if (roles == 0) roles += 1;
        if (entities > 0) branchingFactor = roles / entities;
        assert !Double.isNaN(branchingFactor);
    }

    private void computeTotalCostNext(GraphManager graph) {
        vertices.values().forEach(v -> v.updateObjective(graph));
        edges.forEach(e -> e.updateObjective(graph));
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    void optimise(GraphManager graph) {
        if (isOptimising.compareAndSet(false, true)) {
            Instant s = Instant.now();
            updateObjective(graph);
            if (!isUpToDate() || !isOptimal()) {
                do {
                    totalDuration += TIME_LIMIT_MILLIS;
                    solver.setTimeLimit(totalDuration);
                    Instant start = Instant.now();
                    resultStatus = solver.solve(parameters);
                    Instant finish = Instant.now();
                    long timeElapsed = Duration.between(start, finish).toMillis();
                    totalDuration -= (TIME_LIMIT_MILLIS - timeElapsed);
                    if (isError()) throw GraknException.of(UNEXPECTED_PLANNING_ERROR);
                } while (!isPlanned());
                produceProcedure();
                isUpToDate = true;
            }
            isOptimising.set(false);
            Instant e = Instant.now();
            LOG.trace(String.format("[%s] optimisation duration: %s (ms)", toString(), Duration.between(s, e).toMillis()));
        }
    }

    private void produceProcedure() {
        vertices.values().forEach(PlannerVertex::recordValues);
        edges.forEach(PlannerEdge::recordValues);
        procedure = GraphProcedure.create(this);
        if (procedureLatch.getCount() > 0) procedureLatch.countDown();
    }
}
