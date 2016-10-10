/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Instance;
import io.mindmaps.exception.InvalidConceptTypeException;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.internal.analytics.Analytics;
import io.mindmaps.util.ErrorMessage;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

class ComputeQueryImpl implements ComputeQuery {

    private final Optional<MindmapsGraph> graph;
    private Set<String> typeIds;
    private final String computeMethod;

    ComputeQueryImpl(Optional<MindmapsGraph> graph, String computeMethod, Set<String> typeIds) {
        this.graph = graph;
        this.computeMethod = computeMethod;
        this.typeIds = typeIds;
    }

    @Override
    public Object execute() {
        MindmapsGraph theGraph = graph.orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage()));

        String keyspace = theGraph.getKeyspace();

        Analytics analytics;

        try {
            switch (computeMethod) {
                case "count": {
                    analytics = getAnalytics(keyspace, false);
                    return analytics.count();
                }
                case "degrees": {
                    analytics = getAnalytics(keyspace, false);
                    return analytics.degrees();
                }
                case "degreesAndPersist": {
                    analytics = getAnalytics(keyspace, false);
                    analytics.degreesAndPersist();
                    return "Degrees have been persisted.";
                }
                case "max": {
                    analytics = getAnalytics(keyspace, true);
                    return analytics.max();
                }
                case "mean": {
                    analytics = getAnalytics(keyspace, true);
                    return analytics.mean();
                }
                case "min": {
                    analytics = getAnalytics(keyspace, true);
                    return analytics.min();
                }
                case "std": {
                    analytics = getAnalytics(keyspace, true);
                    return analytics.std();
                }
                case "sum": {
                    analytics = getAnalytics(keyspace, true);
                    return analytics.sum();
                }
                default: {
                    throw new RuntimeException(ErrorMessage.NO_ANALYTICS_METHOD.getMessage(computeMethod));
                }
            }
        } catch (InvalidConceptTypeException e) {
            throw new IllegalArgumentException(ErrorMessage.MUST_BE_RESOURCE_TYPE.getMessage(typeIds),e);
        }

    }

    private Analytics getAnalytics(String keyspace, boolean isStatistics) {
        if (isStatistics) {
            return new Analytics(keyspace, new HashSet<>(), typeIds);
        } else {
            return new Analytics(keyspace, typeIds, new HashSet<>());
        }
    }

    @Override
    public Stream<String> resultsString() {
        Object computeResult = execute();
        if (computeResult instanceof Map) {
            Map<Instance, ?> map = (Map<Instance, ?>) computeResult;
            return map.entrySet().stream().map(e -> e.getKey().getId() + "\t" + e.getValue());
        } else if (computeResult instanceof Optional) {
            return ((Optional) computeResult).isPresent() ? Stream.of(((Optional) computeResult).get().toString()) : Stream.of("There are no instances of this resource type.");
        } else {
            return Stream.of(computeResult.toString());
        }
    }

    @Override
    public boolean isReadOnly() {
        // A compute query may modify the graph based on which method is being used
        return false;
    }

    @Override
    public String toString() {
        String subtypes;
        if (typeIds.isEmpty()) {
            subtypes = "";
        } else {
            subtypes = " in " + typeIds.stream().collect(joining(", "));
        }
        return "compute " + computeMethod + subtypes + ";";
    }

    @Override
    public ComputeQuery withGraph(MindmapsGraph graph) {
        return new ComputeQueryImpl(Optional.of(graph), computeMethod, typeIds);
    }
}
