/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql;

import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Class for parsing query strings into valid queries
 *
 * @author Felix Chapman
 */
public interface QueryParser {

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    @SuppressWarnings("unchecked")
    <T extends Query<?>> T parseQuery(String queryString);

    /**
     * @param reader a reader representing several queries
     * @return a list of queries
     */
    <T extends Query<?>> Stream<T> parseList(Reader reader);

    /**
     * @param queryString a string representing several queries
     * @return a list of queries
     */
    <T extends Query<?>> Stream<T> parseList(String queryString);

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
     * @param template a string representing a templated graql query
     * @param data     data to use in template
     * @return a resolved graql query
     */
    <T extends Query<?>> Stream<T> parseTemplate(String template, Map<String, Object> data);

    /**
     * Register an aggregate that can be used when parsing a Graql query
     * @param name the name of the aggregate
     * @param aggregateMethod a function that will produce an aggregate when passed a list of arguments
     */
    void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod);

    /**
     * Set whether the parser should set all {@link Var}s as user-defined. If it does, then every variable will
     * generate a user-defined name and be returned in results. For example:
     * <pre>
     *     match ($x, $y);
     * </pre>
     *
     * The previous query would normally return only two concepts per result for {@code $x} and {@code $y}. However,
     * if the flag is set it will also return a third variable with a random name representing the relation
     * {@code $123 ($x, $y)}.
     */
    void defineAllVars(boolean defineAllVars);
}
