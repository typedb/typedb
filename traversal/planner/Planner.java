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
import grakn.core.common.concurrent.ManagedBlockingQueue;
import grakn.core.common.exception.GraknException;
import grakn.core.graph.SchemaGraph;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.procedure.Procedure;
import grakn.core.traversal.structure.Structure;
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
    private final Map<Identifier, PlannerVertex> vertices;
    private final Set<PlannerEdge> edges;
    private final ManagedBlockingQueue<Procedure> procedureHolder;
    private final AtomicBoolean isOptimising;
    private MPSolver.ResultStatus resultStatus;
    private Procedure procedure;
    private boolean isUpToDate;
    private long totalDuration;
    private long snapshot;

    public Planner() {
        solver = MPSolver.createSolver("SCIP");
        parameters = new MPSolverParameters();
        parameters.setIntegerParam(PRESOLVE, PRESOLVE_ON.swigValue());
        parameters.setIntegerParam(INCREMENTALITY, INCREMENTALITY_ON.swigValue());
        vertices = new HashMap<>();
        edges = new HashSet<>();
        procedureHolder = new ManagedBlockingQueue<>(1);
        isOptimising = new AtomicBoolean(false);
        resultStatus = MPSolver.ResultStatus.NOT_SOLVED;
        totalDuration = 0L;
    }

    public static Planner create(Structure structure) {
        Planner planner = new Planner();
        Set<StructureVertex> registered = new HashSet<>();
        structure.vertices().forEach(vertex -> planner.register(vertex, registered));
        planner.initialise();
        return planner;
    }

    private void register(StructureVertex structureVertex, Set<StructureVertex> registered) {
        if (registered.contains(structureVertex)) return;
        List<StructureVertex> adjacents = new LinkedList<>();
        PlannerVertex vertex = vertex(structureVertex);
        if (vertex.isThing()) vertex.asThing().properties(structureVertex.asThing().properties());
        else vertex.asType().properties(structureVertex.asType().properties());
        structureVertex.outs().forEach(structureEdge -> {
            adjacents.add(structureEdge.to());
            PlannerVertex to = vertex(structureEdge.to());
            PlannerEdge edge = new PlannerEdge(structureEdge.property(), vertex, to);
            vertex.out(edge);
            to.in(edge);
        });
        structureVertex.ins().forEach(structureEdge -> {
            adjacents.add(structureEdge.from());
            PlannerVertex from = vertex(structureEdge.from());
            PlannerEdge edge = new PlannerEdge(structureEdge.property(), vertex, from);
            vertex.in(edge);
            from.out(edge);
        });
        registered.add(structureVertex);
        adjacents.forEach(v -> register(v, registered));
    }

    private PlannerVertex vertex(StructureVertex structureVertex) {
        if (structureVertex.isThing()) return thingVertex(structureVertex.asThing());
        else return typeVertex(structureVertex.asType());
    }

    private PlannerVertex.Thing thingVertex(StructureVertex.Thing structureVertex) {
        return vertices.computeIfAbsent(structureVertex.identifier(), i -> new PlannerVertex.Thing(this, i)).asThing();
    }

    private PlannerVertex.Type typeVertex(StructureVertex.Type structureVertex) {
        return vertices.computeIfAbsent(structureVertex.identifier(), i -> new PlannerVertex.Type(this, i)).asType();
    }

    private void initialise() {
        vertices.values().forEach(PlannerVertex::initalise);
        edges.forEach(PlannerEdge::initialise);
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
        if (procedure == null) {
            try {
                procedure = procedureHolder.take();
            } catch (InterruptedException e) {
                throw GraknException.of(e);
            }
        }
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
        Procedure newPlan = new Procedure();

        // TODO: extract Traversal Procedure from the MPVariables of Traversal Planner

        procedureHolder.clear();
        try {
            procedureHolder.put(newPlan);
        } catch (InterruptedException e) {
            throw GraknException.of(e);
        }
        procedure = null;
    }
}
