/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknGraph;
import ai.grakn.exception.InvalidConceptTypeException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.analytics.Analytics;
import ai.grakn.util.ErrorMessage;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

class ComputeQueryImpl implements ComputeQuery {

    private final Optional<GraknGraph> graph;
    private final Set<String> subTypeIds;
    private final Set<String> statisticsResourceTypeIds;
    private final String computeMethod;
    private final String from;
    private final String to;

    ComputeQueryImpl(Optional<GraknGraph> graph, String computeMethod, Set<String> subTypeIds, Set<String> statisticsResourceTypeIds, String from, String to) {
        this.graph = graph;
        this.computeMethod = computeMethod;
        this.subTypeIds = subTypeIds;
        this.statisticsResourceTypeIds = statisticsResourceTypeIds;
        this.from = from;
        this.to = to;
    }

    @Override
    public Object execute() {
        GraknGraph theGraph = graph.orElseThrow(() -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage()));

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
                case "connectedComponents": {
                    analytics = getAnalytics(keyspace, false);
                    return analytics.connectedComponents();
                }
                case "connectedComponentsSize": {
                    analytics = getAnalytics(keyspace, false);
                    return analytics.connectedComponentsSize();
                }
                case "connectedComponentsAndPersist": {
                    analytics = getAnalytics(keyspace, false);
                    return analytics.connectedComponentsAndPersist();
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
                case "median": {
                    analytics = getAnalytics(keyspace, true);
                    return analytics.median();
                }
                case "path": {
                    analytics = getAnalytics(keyspace, true);
                    return analytics.shortestPath(from, to);
                }
                default: {
                    throw new IllegalArgumentException(ErrorMessage.NO_ANALYTICS_METHOD.getMessage(computeMethod));
                }
            }
        } catch (InvalidConceptTypeException e) {
            throw new IllegalArgumentException(ErrorMessage.MUST_BE_RESOURCE_TYPE.getMessage(subTypeIds), e);
        }

    }

    private Analytics getAnalytics(String keyspace, boolean isStatistics) {
        return new Analytics(keyspace, subTypeIds, statisticsResourceTypeIds);
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        Object computeResult = execute();
        if (computeResult instanceof Map) {
            if (((Map) computeResult).isEmpty())
                return Stream.of("There are no instances of the selected type(s).");
            if (((Map) computeResult).values().iterator().next() instanceof Set) {
                Map<Serializable, Set<String>> map = (Map<Serializable, Set<String>>) computeResult;
                return map.entrySet().stream().map(entry -> {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (String s : entry.getValue()) {
                        stringBuilder.append(entry.getKey()).append("\t").append(s).append("\n");
                    }
                    return stringBuilder.toString();
                });
            }
        }

        return Stream.of(printer.graqlString(computeResult));
    }

    @Override
    public boolean isReadOnly() {
        // A compute query may modify the graph based on which method is being used
        return false;
    }

    @Override
    public String toString() {
        String subtypes;
        if (subTypeIds.isEmpty()) {
            subtypes = "";
        } else {
            subtypes = " in " + subTypeIds.stream().collect(joining(", "));
        }
        return "compute " + computeMethod + subtypes + ";";
    }

    @Override
    public ComputeQuery withGraph(GraknGraph graph) {
        return new ComputeQueryImpl(Optional.of(graph), computeMethod, subTypeIds, statisticsResourceTypeIds, from, to);
    }

    @Override
    public ComputeQuery infer() {
        return this;
    }
}
