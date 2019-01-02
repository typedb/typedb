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
import grakn.core.graql.query.pattern.Statement;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A query for inserting data.
 * When built without a match clause the insert query will execute once, inserting all the variables provided.
 * When built from a match clause, the insert query will execute for each result of the match clause,
 * where variable names in the insert query are bound to the concept in the result of the match clause.
 */
public class InsertQuery implements Query {

    private final MatchClause match;
    private final List<Statement> statements;

    public InsertQuery(@Nullable MatchClause match, List<Statement> statements) {
        if (statements.isEmpty()) {
            throw GraqlQueryException.noPatterns();
        }

        this.match = match;
        this.statements = statements;
    }

    @Nullable
    @CheckReturnValue
    public MatchClause match() {
        return match;
    }

    @CheckReturnValue
    public List<Statement> statements() {
        return statements;
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        if (match() != null) query.append(match()).append(Char.NEW_LINE);

        query.append(Command.INSERT);
        if (statements.size()>1) query.append(Char.NEW_LINE);
        else query.append(Char.SPACE);

        query.append(statements().stream()
                             .map(Statement::toString)
                             .collect(Collectors.joining(Char.NEW_LINE.toString())));

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InsertQuery that = (InsertQuery) o;

        return (this.match == null ? that.match() == null : this.match.equals(that.match()) &&
                this.statements.equals(that.statements()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (match == null) ? 0 : this.match.hashCode();
        h *= 1000003;
        h ^= this.statements.hashCode();
        return h;
    }
}
