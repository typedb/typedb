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

package io.mindmaps.graql.internal.parser;

import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.MatchQueryDefault;

import java.util.*;
import java.util.stream.Stream;

/**
 * A printer for a MatchQuery, that uses a map of Getters to decide what information to print
 */
public class MatchQueryPrinter {

    private MatchQueryDefault matchQuery;
    private final Map<String, List<Getter>> getters;

    /**
     * @param matchQuery the match query whose results should be printed
     */
    public MatchQueryPrinter(MatchQueryDefault matchQuery, Map<String, List<Getter>> getters) {
        this.matchQuery = matchQuery;
        this.getters = getters;
    }

    public void setMatchQuery(MatchQueryDefault matchQuery) {
        this.matchQuery = matchQuery;
    }

    /**
     * @return the MatchQuery that this printer will print
     */
    public MatchQueryDefault getMatchQuery() {
        return matchQuery;
    }

    /**
     * @return a stream of strings, where each string represents a result
     */
    public Stream<String> resultsString() {
        return matchQuery.stream().map(results -> {
            StringBuilder str = new StringBuilder();

            results.forEach((name, concept) -> {
                List<Getter> getterList = getGetters(name);
                Concept result = results.get(name);
                str.append("$").append(name);
                getterList.forEach(getter -> str.append(getter.resultString(result)));
                str.append("; ");
            });

            return str.toString();
        });
    }

    private List<Getter> getGetters(String name) {
        List<Getter> getterList = getters.computeIfAbsent(name, k -> new ArrayList<>());

        // Add default getters if none provided
        if (getterList.isEmpty()) {
            getterList.add(Getter.id());
            getterList.add(Getter.value());
            getterList.add(Getter.isa());
        }

        return getterList;
    }
}
