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

import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPSolverParameters;
import grakn.core.common.concurrent.ManagedCountDownLatch;
import grakn.core.common.exception.GraknException;
import grakn.core.graph.SchemaGraph;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.procedure.Procedure;
import grakn.core.traversal.structure.Structure;
import grakn.core.traversal.structure.StructureEdge;
import grakn.core.traversal.structure.StructureVertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import static grakn.core.common.exception.ErrorMessage.Internal.UNEXPECTED_PLANNING_ERROR;

public class Planner {

    private static final long TIME_LIMIT_MILLIS = 100;

    private final MPSolver solver;
    private final MPSolverParameters parameters;
    private final Map<Identifier, PlannerVertex<?>> vertices;
    private final Set<PlannerEdge> edges;
    private final AtomicBoolean isOptimising;
    private final ManagedCountDownLatch procedureLatch;

    private volatile Procedure procedure;

    private MPSolver.ResultStatus resultStatus;
    private boolean isUpToDate;
    private long totalDuration;
    private long snapshot;

    private Planner() {
        solver = MPSolver.createSolver("SCIP");
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        vertices = new HashMap<>();
        edges = new HashSet<>();
        procedureLatch = new ManagedCountDownLatch(1);
        isOptimising = new AtomicBoolean(false);
        resultStatus = MPSolver.ResultStatus.NOT_SOLVED;
        totalDuration = 0L;
    }

    public static Planner create(Structure structure) {
        Planner planner = new Planner();
        Set<StructureVertex<?>> registeredVertices = new HashSet<>();
        Set<StructureEdge> registeredEdges = new HashSet<>();
        structure.vertices().forEach(vertex -> planner.register(vertex, registeredVertices, registeredEdges));
        planner.initialise();
        return planner;
    }

    private void register(StructureVertex<?> structureVertex, Set<StructureVertex<?>> registeredVertices,
                          Set<StructureEdge> registeredEdges) {
        if (registeredVertices.contains(structureVertex)) return;
        registeredVertices.add(structureVertex);
        List<StructureVertex<?>> adjacents = new LinkedList<>();
        PlannerVertex<?> vertex = vertex(structureVertex);
        if (vertex.isThing()) structureVertex.asThing().properties().forEach(p -> vertex.asThing().property(p));
        else structureVertex.asType().properties().forEach(p -> vertex.asType().property(p));
        structureVertex.outs().forEach(structureEdge -> {
            if (!registeredEdges.contains(structureEdge)) {
                registeredEdges.add(structureEdge);
                adjacents.add(structureEdge.to());
                PlannerVertex<?> to = vertex(structureEdge.to());
                PlannerEdge edge = new PlannerEdge(structureEdge.property(), vertex, to);
                vertex.out(edge);
                to.in(edge);
            }
        });
        structureVertex.ins().forEach(structureEdge -> {
            if (!registeredEdges.contains(structureEdge)) {
                registeredEdges.add(structureEdge);
                adjacents.add(structureEdge.from());
                PlannerVertex<?> from = vertex(structureEdge.from());
                PlannerEdge edge = new PlannerEdge(structureEdge.property(), from, vertex);
                vertex.in(edge);
                from.out(edge);
            }
        });
        adjacents.forEach(v -> register(v, registeredVertices, registeredEdges));
    }

    private PlannerVertex<?> vertex(StructureVertex<?> structureVertex) {
        if (structureVertex.isThing()) return thingVertex(structureVertex.asThing());
        else return typeVertex(structureVertex.asType());
    }

    private PlannerVertex.Thing thingVertex(StructureVertex.Thing structureVertex) {
        return vertices.computeIfAbsent(
                structureVertex.identifier(), i -> new PlannerVertex.Thing(this, i)
        ).asThing();
    }

    private PlannerVertex.Type typeVertex(StructureVertex.Type structureVertex) {
        return vertices.computeIfAbsent(
                structureVertex.identifier(), i -> new PlannerVertex.Type(this, i)
        ).asType();
    }

    private void initialise() {
        vertices.values().forEach(PlannerVertex::initialiseVariables);
        edges.forEach(PlannerEdge::initialiseVariables);
        vertices.values().forEach(PlannerVertex::initialiseConstraints);
        edges.forEach(PlannerEdge::initialiseConstraints);
    }

    MPSolver solver() {
        return solver;
    }

    private void setUpToDate(boolean isUpToDate) {
        this.isUpToDate = isUpToDate;
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

    public Procedure procedure() {
        try {
            procedureLatch.await();
        } catch (InterruptedException e) {
            throw GraknException.of(e);
        }
        assert procedure != null;
        return procedure;
    }

    public void optimise(SchemaGraph schema) {
        if (isOptimising.compareAndSet(false, true)) {
            updateCost(schema);
            if (!isUpToDate() || !isOptimal()) {
                do {
                    totalDuration += TIME_LIMIT_MILLIS;
                    solver.setTimeLimit(totalDuration);
                    resultStatus = solver.solve(parameters);
                    if (isError()) throw GraknException.of(UNEXPECTED_PLANNING_ERROR);
                } while (!isPlanned());
                exportProcedure();
                isUpToDate = true;
            }
            isOptimising.set(false);
        }
    }

    private void updateCost(SchemaGraph schema) {
        if (snapshot < schema.snapshot()) {
            snapshot = schema.snapshot();

            // TODO: update the cost of every traversal vertex and edge
        }
    }

    private void exportProcedure() {
        Procedure newProcedure = new Procedure();

        // TODO: extract Traversal Procedure from the MPVariables of Traversal Planner

        procedure = newProcedure;
        if (procedureLatch.getCount() > 0) procedureLatch.countDown();
    }
}
