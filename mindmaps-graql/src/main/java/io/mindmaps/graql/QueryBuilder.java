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

package io.mindmaps.graql;

import com.google.common.collect.ImmutableSet;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.parser.MatchQueryPrinter;
import io.mindmaps.graql.internal.parser.QueryParser;
import io.mindmaps.graql.internal.query.Patterns;
import io.mindmaps.graql.internal.query.Queries;
import io.mindmaps.graql.internal.util.AdminConverter;

import java.io.InputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A starting point for creating queries.
 * <p>
 * A {@code QueryBuiler} is constructed with a {@code MindmapsGraph}. All operations are performed using this
 * graph. The user must explicitly commit or rollback changes after executing queries.
 * <p>
 * {@code QueryBuilder} also provides static methods for creating {@code Vars}.
 */
public class QueryBuilder {

    private final Optional<MindmapsGraph> graph;
    private final QueryParser queryParser;

    QueryBuilder() {
        this.graph = Optional.empty();
        queryParser = QueryParser.create(this);
    }

    QueryBuilder(MindmapsGraph graph) {
        this.graph = Optional.of(graph);
        queryParser = QueryParser.create(this);
    }

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    public MatchQuery match(Pattern... patterns) {
        return match(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    public MatchQuery match(Collection<? extends Pattern> patterns) {
        MatchQuery query = Queries.match(Patterns.conjunction(AdminConverter.getPatternAdmins(patterns)));
        return graph.map(query::withGraph).orElse(query);
    }

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    public InsertQuery insert(Var... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    public InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableSet<VarAdmin> varAdmins = ImmutableSet.copyOf(AdminConverter.getVarAdmins(vars));
        return Queries.insert(varAdmins, graph);
    }

    public ComputeQuery compute(String computeMethod) {
        return Queries.compute(graph, computeMethod);
    }

    public ComputeQuery compute(String computeMethod, Set<String> typeIds) {
        return Queries.compute(graph, computeMethod, typeIds);
    }

    /**
     * @param queryString a string representing a match query
     * @return the parsed match query
     */
    public MatchQueryPrinter parseMatch(String queryString) {
        return queryParser.parseMatchQuery(queryString);
    }

    /**
     * @param queryString a string representing an ask query
     * @return a parsed ask query
     */
    public AskQuery parseAsk(String queryString) {
        return queryParser.parseAskQuery(queryString);
    }

    /**
     * @param queryString a string representing an insert query
     * @return a parsed insert query
     */
    public InsertQuery parseInsert(String queryString) {
        return queryParser.parseInsertQuery(queryString);
    }

    /**
     * @param queryString a string representing a delete query
     * @return a parsed delete query
     */
    public DeleteQuery parseDelete(String queryString) {
        return queryParser.parseDeleteQuery(queryString);
    }

    /**
     * @param queryString a string representing an aggregate query
     * @return a parsed aggregate query
     */
    public AggregateQuery<?> parseAggregate(String queryString) {
        return queryParser.parseAggregateQuery(queryString);
    }

    /**
     * @param queryString a string representing a delete query
     * @return a parsed compute query
     */
    public ComputeQuery parseCompute(String queryString) {
        return queryParser.parseComputeQuery(queryString);
    }

    /**
     * @param inputStream a stream representing a list of patterns
     * @return a stream of patterns
     */
    public Stream<Pattern> parsePatterns(InputStream inputStream) {
        return queryParser.parsePatterns(inputStream);
    }

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    public Query<?> parse(String queryString) {
        return queryParser.parseQuery(queryString);
    }

    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        queryParser.registerAggregate(name, aggregateMethod);
    }
}