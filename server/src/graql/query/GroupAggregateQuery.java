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

import grakn.core.graql.query.pattern.Variable;

public class GroupAggregateQuery extends GroupQuery {

    private final AggregateQuery.Method method;
    private final Variable aggregateVar;

    public GroupAggregateQuery(GroupQuery groupQuery, AggregateQuery.Method aggregateMethod, Variable aggregateVar) {
        super(groupQuery.getQuery(), groupQuery.var());

        if (aggregateMethod == null) {
            throw new NullPointerException("Method is null");
        }
        this.method = aggregateMethod;

        if (aggregateVar == null && !method.equals(AggregateQuery.Method.COUNT)) {
            throw new NullPointerException("Variable is null");
        } else if (aggregateVar != null && method.equals(AggregateQuery.Method.COUNT)) {
            throw new IllegalArgumentException("Aggregate COUNT does not accept a Variable");
        } else if (aggregateVar != null && !groupQuery.getQuery().vars().contains(aggregateVar)) {
            throw new IllegalArgumentException("Aggregate variable should be contained in GET query");
        }
        this.aggregateVar = aggregateVar;
    }

    public AggregateQuery.Method aggregateMethod() {
        return method;
    }

    public Variable aggregateVar() {
        return aggregateVar;
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(getQuery()).append(Char.SPACE)
                .append(Command.GROUP).append(Char.SPACE)
                .append(var()).append(Char.SEMICOLON).append(Char.SPACE)
                .append(method);

        if (aggregateVar != null) query.append(Char.SPACE).append(aggregateVar);
        query.append(Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupAggregateQuery that = (GroupAggregateQuery) o;

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
