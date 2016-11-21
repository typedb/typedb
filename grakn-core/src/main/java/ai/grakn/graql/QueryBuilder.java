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

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

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

    ComputeQuery compute(String computeMethod);

    ComputeQuery compute(String computeMethod, Set<String> subTypeIds, Set<String> statisticsResourceTypeIds);

    ComputeQuery compute(String computeMethod, String from, String to, Set<String> subTypeIds);

    /**
     * @param inputStream a stream representing a list of patterns
     * @return a stream of patterns
     */
    Stream<Pattern> parsePatterns(InputStream inputStream);

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
     * @param template a string representing a templated graql query
     * @param data data to use in template
     * @return a query, the type will depend on the type of template.
     */
    <T extends Query<?>> T  parseTemplate(String template, Map<String, Object> data);

    void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod);

    void registerMacro(Macro macro);
}
