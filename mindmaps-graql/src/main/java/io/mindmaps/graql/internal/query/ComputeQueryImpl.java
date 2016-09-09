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
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.internal.analytics.Analytics;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

class ComputeQueryImpl implements ComputeQuery {

    private final MindmapsGraph graph;
    private Optional<Set<String>> typeIds;
    private final String computeMethod;

    ComputeQueryImpl(MindmapsGraph graph, String computeMethod) {
        this.graph = graph;
        this.computeMethod = computeMethod;
        this.typeIds = Optional.empty();
    }

    ComputeQueryImpl(MindmapsGraph graph, String computeMethod, Set<String> typeIds) {
        this.graph = graph;
        this.computeMethod = computeMethod;
        this.typeIds = Optional.of(typeIds);
    }

    @Override
    public Object execute() throws ExecutionException, InterruptedException {
        String keyspace = graph.getKeyspace();

        Analytics analytics = typeIds.map(ids -> {
            Set<Type> types = ids.stream().map(graph::getType).collect(toSet());
            return new Analytics(keyspace, types);
        }).orElseGet(() ->
            new Analytics(keyspace)
        );

        switch (computeMethod) {
            case "count": {
                return analytics.count();
            }
            case "degrees": {
                return analytics.degrees();
            }
            case "degreesAndPersist": {
                analytics.degreesAndPersist();
                return "Degrees have been persisted.";
            }
            default: {
                throw new RuntimeException(ErrorMessage.NO_ANALYTICS_METHOD.getMessage(computeMethod));
            }
        }
    }

    @Override
    public String toString() {
        String subtypes = typeIds.map(types -> " in " + types.stream().collect(joining(", "))).orElse("");
        return "compute " + computeMethod + subtypes;
    }

}
