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
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.gremlin.Query;
import io.mindmaps.graql.internal.validation.MatchQueryValidator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.mindmaps.constants.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;
import static java.util.stream.Collectors.toSet;

/**
 * Base MatchQuery implementation that executes the gremlin traversal
 */
public class MatchQueryBase implements MatchQuery.Admin {

    private final Pattern.Conjunction<Pattern.Admin> pattern;
    private final MindmapsTransaction transaction;
    private final Optional<MatchOrder> order;

    /**
     * @param transaction a transaction to execute the match query on
     * @param pattern a pattern to match in the graph
     */
    private MatchQueryBase(Pattern.Conjunction<Pattern.Admin> pattern, MindmapsTransaction transaction, Optional<MatchOrder> order) {
        if (pattern.getPatterns().size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.MATCH_NO_PATTERNS.getMessage());
        }

        this.pattern = pattern;
        this.transaction = transaction;
        this.order = order;

        if (transaction != null) validate();
    }

    public MatchQueryBase(Pattern.Conjunction<Pattern.Admin> pattern, MindmapsTransaction transaction) {
        this(pattern, transaction, Optional.empty());
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        GraphTraversal<Vertex, Map<String, Vertex>> traversal = getQuery().getTraversals();
        applyModifiers(traversal);
        return traversal.toStream().map(this::makeResults).sequential();
    }

    @Override
    public MatchQuery withTransaction(MindmapsTransaction transaction) {
        return new MatchQueryBase(pattern, transaction, order);
    }

    @Override
    public MatchQuery orderBy(String varName, boolean asc) {
        MatchOrder order = new MatchOrder(varName, Optional.empty(), asc);
        return new MatchQueryBase(pattern, transaction, Optional.of(order));
    }

    @Override
    public MatchQuery orderBy(String varName, String resourceType, boolean asc) {
        MatchOrder order = new MatchOrder(varName, Optional.of(resourceType), asc);
        return new MatchQueryBase(pattern, transaction, Optional.of(order));
    }

    @Override
    public Admin admin() {
        return this;
    }

    @Override
    public Set<Type> getTypes() {
        return getQuery().getConcepts().map(transaction::getType).filter(t -> t != null).collect(toSet());
    }

    @Override
    public Set<String> getSelectedNames() {
        // Default selected names are all user defined variable names shared between disjunctions.
        // For example, in a query of the form
        // {..$x..$y..} or {..$x..}
        // $x will appear in the results, but not $y because it is not guaranteed to appear in all disjunctions

        // Get conjunctions within disjunction
        Set<Pattern.Conjunction<Var.Admin>> conjunctions = pattern.getDisjunctiveNormalForm().getPatterns();

        // Get all selected names from each conjunction
        Stream<Set<String>> vars = conjunctions.stream().map(this::getDefinedNamesFromConjunction);

        // Get the intersection of all conjunctions to find any variables shared between them
        return vars.reduce(Sets::intersection).get();
    }

    @Override
    public Pattern.Conjunction<Pattern.Admin> getPattern() {
        return pattern;
    }

    @Override
    public Optional<MindmapsTransaction> getTransaction() {
        return Optional.ofNullable(transaction);
    }

    @Override
    public String toString() {
        String orderString = order.map(MatchOrder::toString).orElse("");
        return String.format("match %s %s", pattern, orderString).trim();
    }

    /**
     * @param conjunction a conjunction containing variables
     * @return all user-defined variable names in the given conjunction
     */
    private Set<String> getDefinedNamesFromConjunction(Pattern.Conjunction<Var.Admin> conjunction) {
        return conjunction.getVars().stream()
                .flatMap(var -> var.getInnerVars().stream())
                .filter(Var.Admin::isUserDefinedName)
                .map(Var.Admin::getName)
                .collect(Collectors.toSet());
    }

    /**
     * @return the query that will match the specified patterns
     */
    private Query getQuery() {
        return new Query(transaction, this.pattern);
    }

    /**
     * Apply query modifiers (limit, offset, distinct, order) to the graph traversal
     * @param traversal the graph traversal to apply modifiers to
     */
    private void applyModifiers(GraphTraversal<Vertex, Map<String, Vertex>> traversal) {
        order.ifPresent(o -> o.orderTraversal(transaction, traversal));

        Set<String> namesSet = getSelectedNames();
        String[] namesArray = namesSet.toArray(new String[namesSet.size()]);

        // Must provide three arguments in order to pass an array to .select
        // If ordering, select the variable to order by as well
        if (order.isPresent()) {
            traversal.select(order.get().getVar(), order.get().getVar(), namesArray);
        } else if (namesArray.length != 0) {
            traversal.select(namesArray[0], namesArray[0], namesArray);
        }
    }

    /**
     * @param vertices a map of vertices where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<String, Concept> makeResults(Map<String, Vertex> vertices) {
        return getSelectedNames().stream().collect(Collectors.toMap(
                name -> name,
                name -> transaction.getConcept(vertices.get(name).value(ITEM_IDENTIFIER.name()))
        ));
    }

    /**
     * Validate the MatchQuery, assuming a transaction has been specified
     */
    private void validate() {
        new MatchQueryValidator(admin()).validate(transaction);
    }
}

