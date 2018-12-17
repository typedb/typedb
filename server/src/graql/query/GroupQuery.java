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

public class GroupQuery implements Query {

    private final GetQuery getQuery;
    private final Variable var;

    public GroupQuery(GetQuery getQuery, Variable var) {
        if (getQuery == null) {
            throw new NullPointerException("GetQuery is null");
        }
        this.getQuery = getQuery;

        if (var == null) {
            throw new NullPointerException("Variable is null");
        }
        this.var = var;
    }

    public GetQuery getQuery() {
        return getQuery;
    }

    public Variable var() {
        return var;
    }

    @Override
    public String toString() {
        StringBuilder query = new StringBuilder();

        query.append(getQuery()).append(Char.SPACE)
                .append(Command.GROUP).append(Char.SPACE)
                .append(var).append(Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupQuery that = (GroupQuery) o;

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

}
