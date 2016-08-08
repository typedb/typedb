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

import com.google.common.collect.ImmutableSet;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.graql.internal.AdminConverter;
import io.mindmaps.graql.internal.gremlin.Query;
import io.mindmaps.graql.internal.validation.ErrorMessage;
import io.mindmaps.graql.internal.validation.MatchQueryValidator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.mindmaps.core.implementation.DataType.ConceptPropertyUnique.ITEM_IDENTIFIER;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of MatchQuery for finding patterns in graphs
 */
public class MatchQueryImpl implements MatchQuery.Admin {

    private final Optional<ImmutableSet<String>> names;
    private final Pattern.Conjunction<Pattern.Admin> pattern;

    private final Optional<Long> limit;
    private final long offset;
    private final boolean distinct;

    private final Optional<MatchOrder> order;

    private final MindmapsTransaction transaction;

    /**
     * @param transaction a transaction to execute the match query on
     * @param pattern a pattern to match in the graph
     */
    private MatchQueryImpl(Pattern.Conjunction<Pattern.Admin> pattern, MindmapsTransaction transaction, Optional<ImmutableSet<String>> names, Optional<Long> limit, long offset, boolean distinct, Optional<MatchOrder> order) {
        if (pattern.getPatterns().size() == 0) {
            throw new IllegalArgumentException(ErrorMessage.MATCH_NO_PATTERNS.getMessage());
        }

        this.pattern = pattern;
        this.transaction = transaction;
        this.names = names;
        this.limit = limit;
        this.offset = offset;
        this.distinct = distinct;
        this.order = order;
    }

    public MatchQueryImpl(Pattern.Conjunction<Pattern.Admin> pattern, MindmapsTransaction transaction) {
        this(pattern, transaction, Optional.empty(), Optional.empty(), 0, false, Optional.empty());
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        if (transaction != null) validate();

        GraphTraversal<Vertex, Map<String, Vertex>> traversal = getQuery().getTraversals();
        applyModifiers(traversal);
        return traversal.toStream().map(this::makeResults).sequential();
    }

    @Override
    public MatchQuery select(Set<String> names) {
        if (names.isEmpty()) {
            throw new IllegalArgumentException(ErrorMessage.SELECT_NONE_SELECTED.getMessage());
        }

        return new MatchQueryImpl(pattern, transaction, Optional.of(ImmutableSet.copyOf(names)), limit, offset, distinct, order);
    }

    @Override
    public Streamable<Concept> get(String name) {
        return () -> stream().map(result -> result.get(name));
    }

    @Override
    public AskQuery ask() {
        return new AskQueryImpl(this);
    }

    @Override
    public InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableSet<Var.Admin> varAdmins = ImmutableSet.copyOf(AdminConverter.getVarAdmins(vars));
        return new InsertQueryImpl(varAdmins, Optional.of(this), Optional.ofNullable(transaction));
    }

    @Override
    public DeleteQuery delete(Collection<? extends Var> deleters) {
        return new DeleteQueryImpl(AdminConverter.getVarAdmins(deleters), this);
    }

    @Override
    public MatchQuery withTransaction(MindmapsTransaction transaction) {
        return new MatchQueryImpl(pattern, transaction, names, limit, offset, distinct, order);
    }

    @Override
    public MatchQuery limit(long limit) {
        return new MatchQueryImpl(pattern, transaction, names, Optional.of(limit), offset, distinct, order);
    }

    @Override
    public MatchQuery offset(long offset) {
        return new MatchQueryImpl(pattern, transaction, names, limit, offset, distinct, order);
    }

    @Override
    public MatchQuery distinct() {
        return new MatchQueryImpl(pattern, transaction, names, limit, offset, true, order);
    }

    @Override
    public MatchQuery orderBy(String varName, boolean asc) {
        MatchOrder order = new MatchOrder(varName, Optional.empty(), asc);
        return new MatchQueryImpl(pattern, transaction, names, limit, offset, distinct, Optional.of(order));
    }

    @Override
    public MatchQuery orderBy(String varName, String resourceType, boolean asc) {
        MatchOrder order = new MatchOrder(varName, Optional.of(resourceType), asc);
        return new MatchQueryImpl(pattern, transaction, names, limit, offset, distinct, Optional.of(order));
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
        return names.orElseGet(this::defaultSelectedNames);
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
        String selectString = "";

        if (names.isPresent()) {
            selectString = "select " + getSelectedNames().stream().map(s -> "$" + s).collect(Collectors.joining(", "));
        }

        String modifiers = "";
        modifiers += limit.map(l -> "limit " + l + " ").orElse("");
        if (offset != 0) modifiers += "offset " + Long.toString(offset) + " ";
        if (distinct) modifiers += "distinct ";
        modifiers += order.map(MatchOrder::toString).orElse("");

        return String.format("match %s %s%s", pattern, modifiers, selectString).trim();
    }

    private ImmutableSet<String> defaultSelectedNames() {
        return ImmutableSet.copyOf(pattern.getVars().stream()
                .flatMap(v -> v.getInnerVars().stream())
                .filter(Var.Admin::isUserDefinedName)
                .map(Var.Admin::getName)
                .collect(Collectors.toSet()));
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

        if (distinct) traversal.dedup();

        long top = limit.map(lim -> offset + lim).orElse(Long.MAX_VALUE);
        traversal.range(offset, top);
    }

    /**
     * @param vertices a map of vertices where the key is the variable name
     * @return a map of concepts where the key is the variable name
     */
    private Map<String, Concept> makeResults(Map<String, Vertex> vertices) {
        Map<String, Concept> map = new HashMap<>();
        for (String name : getSelectedNames()) {
            Vertex vertex = vertices.get(name);
            Concept concept = transaction.getConcept(vertex.value(ITEM_IDENTIFIER.name()));
            if(concept != null)
                map.put(name, concept);
        }
        return map;
        //TODO: Find out why lambda fails when Titan Indices don't return anything. I.e. when concept = null. This happens when Titan indices are corrupted.
        /*return names.stream().collect(Collectors.toMap(
                name -> name,
                name -> transaction.getConcept(vertices.get(name).value(ITEM_IDENTIFIER.name()))
        ));*/
    }

    /**
     * Validate the MatchQuery, assuming a transaction has been specified
     */
    private void validate() {
        new MatchQueryValidator(admin()).validate(transaction);
    }
}

