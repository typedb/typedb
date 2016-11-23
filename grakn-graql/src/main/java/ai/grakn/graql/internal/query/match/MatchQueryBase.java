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

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.GremlinQuery;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Base MatchQuery implementation that executes the gremlin traversal
 */
public class MatchQueryBase implements MatchQueryInternal {

    private final Conjunction<PatternAdmin> pattern;

    /**
     * @param pattern a pattern to match in the graph
     */
    public MatchQueryBase(Conjunction<PatternAdmin> pattern) {
        if (pattern.getPatterns().size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.MATCH_NO_PATTERNS.getMessage());
        }

        this.pattern = pattern;
    }

    @Override
    public Stream<Map<String, Concept>> stream(Optional<GraknGraph> optionalGraph, Optional<MatchOrder> order) {
        GraknGraph graph = optionalGraph.orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage())
        );

        for (VarAdmin var : pattern.getVars()) {
            var.getProperties().forEach(property -> ((VarPropertyInternal) property).checkValid(graph, var));
        }

        GraphTraversal<Vertex, Map<String, Vertex>> traversal = getQuery(graph, order).getTraversal();
        return traversal.toStream().map(vertices -> makeResults(graph, vertices)).sequential();
    }

    @Override
    public Set<Type> getTypes(GraknGraph graph) {
        GremlinQuery gremlinQuery = getQuery(graph, Optional.empty());
        return gremlinQuery.getConcepts().map(graph::getType).filter(t -> t != null).collect(toSet());
    }

    @Override
    public Set<Type> getTypes() {
        throw new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage());
    }

    @Override
    public ImmutableSet<String> getSelectedNames() {
        // Default selected names are all user defined variable names shared between disjunctions.
        // For example, in a query of the form
        // {..$x..$y..} or {..$x..}
        // $x will appear in the results, but not $y because it is not guaranteed to appear in all disjunctions

        // Get conjunctions within disjunction
        Set<Conjunction<VarAdmin>> conjunctions = pattern.getDisjunctiveNormalForm().getPatterns();

        // Get all selected names from each conjunction
        Stream<Set<String>> vars = conjunctions.stream().map(this::getDefinedNamesFromConjunction);

        // Get the intersection of all conjunctions to find any variables shared between them
        // This will fail if there are no conjunctions (so the query is empty)
        Set<String> names = vars.reduce(Sets::intersection).orElseThrow(
                () -> new RuntimeException(ErrorMessage.MATCH_NO_PATTERNS.getMessage())
        );
        
        return ImmutableSet.copyOf(names);
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        return pattern;
    }

    @Override
    public Optional<GraknGraph> getGraph() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "match " + pattern.getPatterns().stream().map(p -> p + ";").collect(joining(" "));
    }

    /**
     * @param conjunction a conjunction containing variables
     * @return all user-defined variable names in the given conjunction
     */
    private Set<String> getDefinedNamesFromConjunction(Conjunction<VarAdmin> conjunction) {
        return conjunction.getVars().stream()
                .flatMap(var -> var.getInnerVars().stream())
                .filter(VarAdmin::isUserDefinedName)
                .map(VarAdmin::getVarName)
                .collect(Collectors.toSet());
    }

    /**
     * @param graph the graph to execute the query on
     * @param order an optional ordering of the query
     * @return the query that will match the specified patterns
     */
    private GremlinQuery getQuery(GraknGraph graph, Optional<MatchOrder> order) {
        return new GremlinQuery(graph, this.pattern, getSelectedNames(), order);
    }

    /**
     * @param graph the graph to get results from
     * @param vertices a map of vertices where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<String, Concept> makeResults(GraknGraph graph, Map<String, Vertex> vertices) {
        return getSelectedNames().stream().collect(Collectors.toMap(
                name -> name,
                name -> graph.getConcept(vertices.get(name).id().toString())
        ));
    }
}

