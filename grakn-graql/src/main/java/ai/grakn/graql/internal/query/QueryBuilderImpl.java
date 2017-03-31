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
 *
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknGraph;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.ComputeQueryBuilder;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.parser.QueryParser;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.analytics.ComputeQueryBuilderImpl;
import ai.grakn.graql.internal.query.match.MatchQueryBase;
import ai.grakn.graql.internal.template.TemplateParser;
import ai.grakn.graql.internal.util.AdminConverter;
import ai.grakn.graql.macro.Macro;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A starting point for creating queries.
 * <p>
 * A {@code QueryBuiler} is constructed with a {@code GraknGraph}. All operations are performed using this
 * graph. The user must explicitly commit or rollback changes after executing queries.
 * <p>
 * {@code QueryBuilderImpl} also provides static methods for creating {@code Vars}.
 *
 * @author Felix Chapman
 */
public class QueryBuilderImpl implements QueryBuilder {

    private final Optional<GraknGraph> graph;
    private final QueryParser queryParser;
    private final TemplateParser templateParser;
    private boolean infer = false;
    private boolean materialise = false;

    public QueryBuilderImpl() {
        this.graph = Optional.empty();
        queryParser = QueryParser.create(this);
        templateParser = TemplateParser.create();
    }

    public QueryBuilderImpl(GraknGraph graph) {
        this.graph = Optional.of(graph);
        queryParser = QueryParser.create(this);
        templateParser = TemplateParser.create();
    }

    @Override
    public QueryBuilder infer(boolean infer) {
        this.infer = infer;
        return this;
    }

    @Override
    public QueryBuilder materialise(boolean materialise) {
        this.materialise = materialise;
        return this;
    }

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    @Override
    public MatchQuery match(Pattern... patterns) {
        return match(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    @Override
    public MatchQuery match(Collection<? extends Pattern> patterns) {
        Conjunction<PatternAdmin> conjunction = Patterns.conjunction(Sets.newHashSet(AdminConverter.getPatternAdmins(patterns)));
        MatchQueryBase base = new MatchQueryBase(conjunction);
        MatchQuery query = infer ? base.infer(materialise).admin() : base;
        return graph.map(query::withGraph).orElse(query);
    }

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    @Override
    public InsertQuery insert(Var... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    @Override
    public InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableList<VarAdmin> varAdmins = ImmutableList.copyOf(AdminConverter.getVarAdmins(vars));
        return new InsertQueryImpl(varAdmins, Optional.empty(), graph);
    }

    /**
     * @return a compute query builder for building analytics query
     */
    @Override
    public ComputeQueryBuilder compute(){
        return new ComputeQueryBuilderImpl(graph);
    }

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    @Override
    public List<Pattern> parsePatterns(String patternsString) {
        return queryParser.parsePatterns(patternsString);
    }

    /**
     * @param patternString a string representing a pattern
     * @return a pattern
     */
    @Override
    public Pattern parsePattern(String patternString) {
        return queryParser.parsePattern(patternString);
    }

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    @Override
    public <T extends Query<?>> T parse(String queryString) {
        return queryParser.parseQuery(queryString);
    }

    @Override
    public <T extends Query<?>> List<T> parseList(String queryString) {
        return queryParser.parseList(queryString);
    }

    /**
     * @param template a string representing a templated graql query
     * @param data     data to use in template
     * @return a resolved graql query
     */
    @Override
    public <T extends Query<?>> List<T> parseTemplate(String template, Map<String, Object> data){
        return parseList(templateParser.parseTemplate(template, data));
    }

    @Override
    public void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod) {
        queryParser.registerAggregate(name, aggregateMethod);
    }

    @Override
    public void registerMacro(Macro macro){
        templateParser.registerMacro(macro.name(), macro);
    }
}