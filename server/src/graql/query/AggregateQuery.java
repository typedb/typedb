/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query;

import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

public class AggregateQuery implements Query {

    private final GetQuery getQuery;
    private final Method method;
    private final Set<Variable> vars;

    public final static Map<Method, Integer> VARIABLES_MINIMUM = variablesMinimum();
    public final static Map<Method, Integer> VARIABLES_MAXIMUM = variablesMaximum();

    public enum Method {
        COUNT("count"),
        MAX("max"),
        MEAN("mean"),
        MEDIAN("median"),
        MIN("min"),
        STD("std"),
        SUM("sum");

        private final String method;

        Method(String method) {
            this.method = method;
        }

        @Override
        public String toString() {
            return this.method;
        }

        public static Method of(String value) {
            for (Method m : Method.values()) {
                if (m.method.equals(value)) {
                    return m;
                }
            }
            return null;
        }
    }

    public AggregateQuery(GetQuery getQuery, Method method, Set<Variable> vars) {
        if (getQuery == null) {
            throw new NullPointerException("GetQuery is null");
        }
        this.getQuery = getQuery;

        if (method == null) {
            throw new NullPointerException("Method is null");
        }
        this.method = method;

        if (vars == null) {
            throw new NullPointerException("Set<Variables> is null");
        } else if ((VARIABLES_MINIMUM.containsKey(method) && vars.size() < VARIABLES_MINIMUM.get(method)) ||
                (VARIABLES_MAXIMUM.containsKey(method) && vars.size() > VARIABLES_MAXIMUM.get(method))) {
            throw GraqlQueryException.incorrectAggregateArgumentNumber(method.toString(),
                                                                       VARIABLES_MINIMUM.get(method),
                                                                       VARIABLES_MAXIMUM.get(method),
                                                                       vars);
        }
        this.vars = vars;
    }

    private static Map<Method, Integer> variablesMinimum() {
        Map<Method, Integer> minimum = new HashMap<>();
        minimum.put(Method.MAX, 1);
        minimum.put(Method.MEAN, 1);
        minimum.put(Method.MEDIAN, 1);
        minimum.put(Method.MIN, 1);
        minimum.put(Method.STD, 1);
        minimum.put(Method.SUM, 1);

        return Collections.unmodifiableMap(minimum);
    }

    private static Map<Method, Integer> variablesMaximum() {
        Map<Method, Integer> maximum = new HashMap<>();
        maximum.put(Method.MAX, 1);
        maximum.put(Method.MEAN, 1);
        maximum.put(Method.MEDIAN, 1);
        maximum.put(Method.MIN, 1);
        maximum.put(Method.STD, 1);
        maximum.put(Method.SUM, 1);

        return Collections.unmodifiableMap(maximum);
    }

    public GetQuery getQuery() {
        return getQuery;
    }

    public Method method() {
        return method;
    }

    public Set<Variable> vars() {
        return vars;
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(getQuery()).append(Char.SPACE)
                .append(method).append(Char.SPACE)
                .append(vars.stream().map(Variable::toString).collect(joining(Char.COMMA_SPACE.toString())))
                .append(Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregateQuery that = (AggregateQuery) o;

        return (this.getQuery.equals(that.getQuery()) &&
                this.method.equals(that.method()) &&
                this.vars.equals(that.vars()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.getQuery.hashCode();
        h *= 1000003;
        h ^= this.method.hashCode();
        h *= 1000003;
        h ^= this.vars.hashCode();
        return h;
    }

}
