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

import grakn.core.graql.query.statement.Variable;
import graql.exception.GraqlException;

import javax.annotation.CheckReturnValue;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * A query for deleting concepts from a match clause clause.
 * The delete operation to perform is based on what Statement objects
 * are provided to it. If only variable names are provided, then the delete
 * query will delete the concept bound to each given variable name. If property
 * flags are provided, e.g. {@code var("x").has("name")} then only those
 * properties are deleted.
 */
public class GraqlDelete extends GraqlQuery {

    private final MatchClause match;
    private final LinkedHashSet<Variable> vars;

    public GraqlDelete(MatchClause match) {
        this(match, new LinkedHashSet<>());
    }

    public GraqlDelete(MatchClause match, LinkedHashSet<Variable> vars) {
        if (match == null) {
            throw new NullPointerException("Null match");
        }
        this.match = match;
        if (vars == null) {
            throw new NullPointerException("Null vars");
        }
        for (Variable var : vars) {
            if (!match.getSelectedNames().contains(var)) {
                throw GraqlException.varNotInQuery(var.toString());
            }
        }
        this.vars = vars;
    }

    @CheckReturnValue
    public MatchClause match() {
        return match;
    }

    @CheckReturnValue // TODO: Return LinkedHashSet
    public Set<Variable> vars() {
        if (vars.isEmpty()) return match.getPatterns().variables();
        return vars;
    }

    @Override @SuppressWarnings("Duplicates")
    public String toString() {
        StringBuilder query = new StringBuilder();

        query.append(match());
        if (match().getPatterns().getPatterns().size()>1) query.append(Token.Char.NEW_LINE);
        else query.append(Token.Char.SPACE);

        // It is important that we use vars (the property) and not vars() (the method)
        // vars (the property) stores the variables as the user defined
        // vars() (the method) returns match.vars() if vars (the property) is empty
        // we want to print vars (the property) as the user defined
        query.append(Token.Command.DELETE);
        if (!vars.isEmpty()) {
            query.append(Token.Char.SPACE).append(
                    vars.stream().map(Variable::toString)
                            .collect(joining(Token.Char.COMMA_SPACE.toString())).trim()
            );
        }
        query.append(Token.Char.SEMICOLON);

        return query.toString();
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraqlDelete that = (GraqlDelete) o;

        // It is important that we use vars() (the method) and not vars (the property)
        // vars (the property) stores the variables as the user defined
        // vars() (the method) returns match.vars() if vars (the property) is empty
        // we want to compare vars() (the method) which determines the final value
        return this.match().equals(that.match()) &&
                this.vars().equals(that.vars());
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        // It is important that we use vars() (the method) and not vars (the property)
        // For reasons explained in the equals() method above
        h ^= this.vars().hashCode();
        h *= 1000003;
        h ^= this.match().hashCode();
        return h;
    }
}
