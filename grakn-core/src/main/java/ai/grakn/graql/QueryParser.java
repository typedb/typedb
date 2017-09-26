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

package ai.grakn.graql;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Class for parsing query strings into valid queries
 *
 * @author Felix Chapman
 */
public interface QueryParser {

    void registerAggregate(String name, Function<List<Object>, Aggregate> aggregateMethod);

    /**
     * @param queryString a string representing a query
     * @return
     * a query, the type will depend on the type of query.
     */
    @SuppressWarnings("unchecked")
    <T extends Query<?>> T parseQuery(String queryString);

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
}
