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

package ai.grakn.graql.admin;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Admin class for inspecting and manipulating a MatchQuery
 *
 * @author Felix Chapman
 */
public interface MatchQueryAdmin extends MatchQuery {

    /**
     * Get a list of results. This differs from {@code MatchQuery#execute} because the keys are instances of
     * {@code VarName}.
     * @return a list of results
     */
    List<Map<VarName, Concept>> results();

    /**
     * Get a stream of results. This differs from {@code MatchQuery#execute} because the keys are instances of
     * {@code VarName}.
     * @return a stream of results
     */
    Stream<Map<VarName, Concept>> streamWithVarNames();

    /**
     * Get a stream of answers. This differs from {@code MatchQuery#execute} because it returns a stream of
     * {@code Answer} objects containing the original map with {@code VarName} as key.
     * @return a stream of answers
     */
    Stream<Answer> streamWithAnswers();

    /**
     * @param graph the graph to use to get types from
     * @return all concept types referred to explicitly in the query
     */
    Set<Type> getTypes(GraknGraph graph);

    /**
     * @return all concept types referred to explicitly in the query
     */
    Set<Type> getTypes();

    /**
     * @return the pattern to match in the graph
     */
    Conjunction<PatternAdmin> getPattern();

    /**
     * @return the graph the query operates on, if one was provided
     */
    Optional<GraknGraph> getGraph();

    /**
     * @return all selected variable names in the query
     */
    Set<VarName> getSelectedNames();
}
