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

import com.google.ortools.linearsolver.MPVariable;

import java.util.LinkedHashSet;
import java.util.List;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class GraphInitialiser {

    private final GraphPlanner planner;
    private final LinkedHashSet<PlannerVertex<?>> queue;
    private final MPVariable[] variables;
    private final double[] initialValues;
    private int edgeCount;

    public GraphInitialiser(GraphPlanner planner) {
        this.planner = planner;
        queue = new LinkedHashSet<>();
        edgeCount = 0;

        int count = countVariables();
        variables = new MPVariable[count];
        initialValues = new double[count];
    }

    private int countVariables() {
        int vertexVars = 4 * planner.vertices().size();
        int edgeVars = (2 + planner.edges().size()) * planner.edges().size() * 2;
        return vertexVars + edgeVars;
    }

    public static GraphInitialiser create(GraphPlanner planner) {
        assert planner.vertices().size() > 1 && !planner.edges().isEmpty();
        return new GraphInitialiser(planner);
    }

    public void execute() {
        resetInitialValues();
        PlannerVertex<?> start = planner.vertices().stream().min(comparing(v -> v.costLastRecorded)).get();
        start.setStartingVertexInitial();
        queue.add(start);
        while (!queue.isEmpty()) {
            PlannerVertex<?> vertex = queue.iterator().next();
            List<PlannerEdge.Directional<?, ?>> outgoing = vertex.outs().stream()
                    .filter(e -> !e.hasInitialValue() && !(e.isSelfClosure() && e.direction().isBackward()))
                    .sorted(comparing(e -> e.costLastRecorded)).collect(toList());
            if (!outgoing.isEmpty()) {
                vertex.setHasOutgoingEdgesInitial();
                outgoing.forEach(e -> {
                    e.setInitialValue(++edgeCount);
                    e.to().setHasIncomingEdgesInitial();
                    queue.add(e.to());
                });
            } else {
                vertex.setEndingVertexInitial();
            }
            queue.remove(vertex);
        }

        int index = 0;
        for (PlannerVertex<?> v : planner.vertices()) index = v.recordInitial(variables, initialValues, index);
        for (PlannerEdge<?, ?> e : planner.edges()) index = e.recordInitial(variables, initialValues, index);
        assert index == variables.length && index == initialValues.length;

        planner.solver().setHint(variables, initialValues);
    }

    private void resetInitialValues() {
        planner.vertices().forEach(PlannerVertex::resetInitialValue);
        planner.edges().forEach(PlannerEdge::resetInitialValue);
    }
}
