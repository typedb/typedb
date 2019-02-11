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

package grakn.core.graql.query.query;

import grakn.core.graql.query.query.builder.AggregateBuilder;
import grakn.core.graql.query.statement.Variable;
import graql.util.Token;

public class GraqlGroup extends GraqlQuery implements AggregateBuilder<GraqlGroup.Aggregate> {

    private final GraqlGet getQuery;
    private final Variable var;

    public GraqlGroup(GraqlGet getQuery, Variable var) {
        if (getQuery == null) {
            throw new NullPointerException("GetQuery is null");
        }
        this.getQuery = getQuery;

        if (var == null) {
            throw new NullPointerException("Variable is null");
        }
        this.var = var;
    }

    public GraqlGet getQuery() {
        return getQuery;
    }

    public Variable var() {
        return var;
    }

    @Override
    public Aggregate aggregate(GraqlAggregate.Method method, Variable var) {
        return new GraqlGroup.Aggregate(this, method, var);
    }

    @Override
    public String toString() {
        StringBuilder query = new StringBuilder();

        query.append(getQuery()).append(Token.Char.SPACE)
                .append(Token.Command.GROUP).append(Token.Char.SPACE)
                .append(var).append(Token.Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraqlGroup that = (GraqlGroup) o;

        return (this.getQuery.equals(that.getQuery()) &&
                this.var.equals(that.var()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.getQuery.hashCode();
        h *= 1000003;
        h ^= this.var.hashCode();
        return h;
    }

    public static class Aggregate extends GraqlGroup {

        private final GraqlAggregate.Method method;
        private final Variable aggregateVar;

        public Aggregate(GraqlGroup groupQuery, GraqlAggregate.Method aggregateMethod, Variable aggregateVar) {
            super(groupQuery.getQuery(), groupQuery.var());

            if (aggregateMethod == null) {
                throw new NullPointerException("Method is null");
            }
            this.method = aggregateMethod;

            if (aggregateVar == null && !method.equals(GraqlAggregate.Method.COUNT)) {
                throw new NullPointerException("Variable is null");
            } else if (aggregateVar != null && method.equals(GraqlAggregate.Method.COUNT)) {
                throw new IllegalArgumentException("Aggregate COUNT does not accept a Variable");
            } else if (aggregateVar != null && !groupQuery.getQuery().vars().contains(aggregateVar)) {
                throw new IllegalArgumentException("Aggregate variable should be contained in GET query");
            }
            this.aggregateVar = aggregateVar;
        }

        public GraqlAggregate.Method aggregateMethod() {
            return method;
        }

        public Variable aggregateVar() {
            return aggregateVar;
        }

        @Override
        public final String toString() {
            StringBuilder query = new StringBuilder();

            query.append(getQuery()).append(Token.Char.SPACE)
                    .append(Token.Command.GROUP).append(Token.Char.SPACE)
                    .append(var()).append(Token.Char.SEMICOLON).append(Token.Char.SPACE)
                    .append(method);

            if (aggregateVar != null) query.append(Token.Char.SPACE).append(aggregateVar);
            query.append(Token.Char.SEMICOLON);

            return query.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Aggregate that = (Aggregate) o;

            return (this.getQuery().equals(that.getQuery()) &&
                    this.var().equals(that.var()) &&
                    this.aggregateMethod().equals(that.aggregateMethod()) &&
                    this.aggregateVar() == null ?
                        that.aggregateVar() == null :
                        this.aggregateVar().equals(that.aggregateVar()));
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^= this.getQuery().hashCode();
            h *= 1000003;
            h ^= this.var().hashCode();
            h *= 1000003;
            h ^= this.aggregateMethod().hashCode();
            h *= 1000003;
            h ^= this.aggregateVar().hashCode();
            return h;
        }
    }
}
