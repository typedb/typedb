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

package ai.grakn.graql;

import ai.grakn.graql.macro.Macro;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Starting point for creating queries
 *
 * @author Felix Chapman
 */
public interface QueryBuilder {

    /**
     * @param patterns an array of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    MatchQuery match(Pattern... patterns);

    /**
     * @param patterns a collection of patterns to match in the graph
     * @return a match query that will find matches of the given patterns
     */
    MatchQuery match(Collection<? extends Pattern> patterns);

    /**
     * @param vars an array of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    InsertQuery insert(Var... vars);

    /**
     * @param vars a collection of variables to insert into the graph
     * @return an insert query that will insert the given variables into the graph
     */
    InsertQuery insert(Collection<? extends Var> vars);

    /**
     * @return a compute query builder for building analytics query
     */
    ComputeQueryBuilder compute();

    /**
     * @param patternsString a string representing a list of patterns
     * @return a list of patterns
     */
    List<Pattern> parsePatterns(String patternsString);

    /**
     * @param patternString a string representing a pattern
     * @return a pattern
     */
    Pattern parsePattern(String patternString);

    /**
     * @param queryString a string representing a query
     * @return a query, the type will depend on the type of query.
     */
    <T extends Query<?>> T parse(String queryString);

    /**
     * @param queryString a string representing several queries
     * @return a list of queries
     */
    <T extends Query<?>> List<T> parseList(String queryString);

    /**
     * @param template a string representing a templated graql query
     * @param data data to use in template
     * @return a query, the type will depend on the type of template.
     */
    <T extends Query<?>> List<T> parseTemplate(String template, Map<String, Object> data);

    /**
     * Register an aggregate that can be used when parsing a Graql query
     * @param name the name of the aggregate
     * @param aggregateMethod a function that will produce an aggregate when passed a list of arguments
     */
    void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod);

    /**
     * Register a macro that can be used when parsing a Graql template
     * @param macro the macro to register
     */
    void registerMacro(Macro macro);

    /**
     * Enable or disable inference
     */
    QueryBuilder infer(boolean infer);

    /**
     * Enable or disable materialisation
     */
    QueryBuilder materialise(boolean materialise);
}
