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

package io.mindmaps.graql.internal.query.match;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.Query;
import io.mindmaps.graql.internal.validation.MatchQueryValidator;
import io.mindmaps.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.mindmaps.util.Schema.ConceptProperty.ITEM_IDENTIFIER;
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
    public Stream<Map<String, Concept>> stream(Optional<MindmapsGraph> optionalGraph, Optional<MatchOrder> order) {
        MindmapsGraph graph = optionalGraph.orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage())
        );

        new MatchQueryValidator(admin()).validate(graph);

        GraphTraversal<Vertex, Map<String, Vertex>> traversal = getQuery(graph, order).getTraversals();
        return traversal.toStream().map(vertices -> makeResults(graph, vertices)).sequential();
    }

    @Override
    public Set<Type> getTypes(MindmapsGraph graph) {
        Query query = getQuery(graph, Optional.empty());
        return query.getConcepts().map(graph::getType).filter(t -> t != null).collect(toSet());
    }

    @Override
    public Set<Type> getTypes() {
        throw new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage());
    }

    @Override
    public Set<String> getSelectedNames() {
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
        return vars.reduce(Sets::intersection).orElseThrow(
                () -> new RuntimeException(ErrorMessage.MATCH_NO_PATTERNS.getMessage())
        );
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        return pattern;
    }

    @Override
    public Optional<MindmapsGraph> getGraph() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "match " + pattern;
    }

    /**
     * @param conjunction a conjunction containing variables
     * @return all user-defined variable names in the given conjunction
     */
    private Set<String> getDefinedNamesFromConjunction(Conjunction<VarAdmin> conjunction) {
        return conjunction.getVars().stream()
                .flatMap(var -> var.getInnerVars().stream())
                .filter(VarAdmin::isUserDefinedName)
                .map(VarAdmin::getName)
                .collect(Collectors.toSet());
    }

    /**
     * @param graph the graph to execute the query on
     * @param order an optional ordering of the query
     * @return the query that will match the specified patterns
     */
    private Query getQuery(MindmapsGraph graph, Optional<MatchOrder> order) {
        return new Query(graph, this.pattern, getSelectedNames(), order);
    }

    /**
     * @param graph the graph to get results from
     * @param vertices a map of vertices where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<String, Concept> makeResults(MindmapsGraph graph, Map<String, Vertex> vertices) {
        return getSelectedNames().stream().collect(Collectors.toMap(
                name -> name,
                name -> graph.getConcept(vertices.get(name).value(ITEM_IDENTIFIER.name()))
        ));
    }
}

