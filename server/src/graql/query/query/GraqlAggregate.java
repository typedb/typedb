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

import grakn.core.graql.query.statement.Variable;
import graql.lang.util.Token;

public class GraqlAggregate extends GraqlQuery {

    private final GraqlGet getQuery;
    private final Token.Statistics.Method method;
    private final Variable var;

    public GraqlAggregate(GraqlGet getQuery, Token.Statistics.Method method, Variable var) {
        if (getQuery == null) {
            throw new NullPointerException("GetQuery is null");
        }
        this.getQuery = getQuery;

        if (method == null) {
            throw new NullPointerException("Method is null");
        }
        this.method = method;

        if (var == null && !method.equals(Token.Statistics.Method.COUNT)) {
            throw new NullPointerException("Variable is null");
        } else if (var != null && method.equals(Token.Statistics.Method.COUNT)) {
            throw new IllegalArgumentException("Aggregate COUNT does not accept a Variable");
        } else if (var != null && !getQuery.vars().contains(var)) {
            throw new IllegalArgumentException("Aggregate variable should be contained in GET query");
        }

        this.var = var;
    }

    public GraqlGet getQuery() {
        return getQuery;
    }

    public Token.Statistics.Method method() {
        return method;
    }

    public Variable var() {
        return var;
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(getQuery()).append(Token.Char.SPACE).append(method);

        if (var != null) query.append(Token.Char.SPACE).append(var);
        query.append(Token.Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraqlAggregate that = (GraqlAggregate) o;

        return (this.getQuery.equals(that.getQuery()) &&
                this.method.equals(that.method()) &&
                this.var == null ?
                    that.var() == null :
                    this.var.equals(that.var()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.getQuery.hashCode();
        h *= 1000003;
        h ^= this.method.hashCode();
        h *= 1000003;
        h ^= this.var.hashCode();
        return h;
    }

}
