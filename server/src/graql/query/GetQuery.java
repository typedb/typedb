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

import grakn.core.graql.query.pattern.statement.Variable;
import graql.exception.GraqlException;

import javax.annotation.CheckReturnValue;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * A query used for finding data in a knowledge base that matches the given patterns. The Get Query is a
 * pattern-matching query. The patterns are described in a declarative fashion, then the query will traverse
 * the knowledge base in an efficient fashion to find any matching answers.
 */
public class GetQuery implements Query {

    private final LinkedHashSet<Variable> vars;
    private final MatchClause match;

    public GetQuery(MatchClause match) {
        this(match, new LinkedHashSet<>());
    }

    public GetQuery(MatchClause match, LinkedHashSet<Variable> vars) {
        if (vars == null) {
            throw new NullPointerException("Null vars");
        }
        this.vars = vars;
        if (match == null) {
            throw new NullPointerException("Null match");
        }
        for (Variable var : vars) {
            if (!match.getSelectedNames().contains(var)) {
                throw GraqlException.varNotInQuery(var.toString());
            }
        }
        this.match = match;
    }

    public AggregateQuery count() {
        return new AggregateQuery(this, AggregateQuery.Method.COUNT, null);
    }

    public AggregateQuery max(String var) {
        return max(new Variable(var));
    }

    public AggregateQuery max(Variable var) {
        return new AggregateQuery(this, AggregateQuery.Method.MAX, var);
    }

    public AggregateQuery mean(String var) {
        return mean(new Variable(var));
    }

    public AggregateQuery mean(Variable var) {
        return new AggregateQuery(this, AggregateQuery.Method.MEAN, var);
    }

    public AggregateQuery median(String var) {
        return median(new Variable(var));
    }

    public AggregateQuery median(Variable var) {
        return new AggregateQuery(this, AggregateQuery.Method.MEDIAN, var);
    }

    public AggregateQuery min(String var) {
        return min(new Variable(var));
    }

    public AggregateQuery min(Variable var) {
        return new AggregateQuery(this, AggregateQuery.Method.MIN, var);
    }

    public AggregateQuery std(String var) {
        return std(new Variable(var));
    }

    public AggregateQuery std(Variable var) {
        return new AggregateQuery(this, AggregateQuery.Method.STD, var);
    }

    public AggregateQuery sum(String var) {
        return sum(new Variable(var));
    }

    public AggregateQuery sum(Variable var) {
        return new AggregateQuery(this, AggregateQuery.Method.SUM, var);
    }

    public GroupQuery group(String var) {
        return group(new Variable(var));
    }

    public GroupQuery group(Variable var) {
        return new GroupQuery(this, var);
    }

    @CheckReturnValue // TODO: Return LinkedHashSet
    public Set<Variable> vars() {
        if (vars.isEmpty()) return match.getPatterns().variables();
        else return vars;
    }

    @CheckReturnValue
    public MatchClause match() {
        return match;
    }

    @Override @SuppressWarnings("Duplicates")
    public String toString() {
        StringBuilder query = new StringBuilder();

        query.append(match());
        if (match().getPatterns().getPatterns().size()>1) query.append(Char.NEW_LINE);
        else query.append(Char.SPACE);

        // It is important that we use vars (the property) and not vars() (the method)
        // vars (the property) stores the variables as the user defined
        // vars() (the method) returns match.vars() if vars (the property) is empty
        // we want to print vars (the property) as the user defined
        query.append(Command.GET);
        if (!vars.isEmpty()) {
            query.append(Char.SPACE).append(
                    vars.stream().map(Variable::toString)
                            .collect(joining(Char.COMMA_SPACE.toString()))
            );
        }
        query.append(Char.SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetQuery that = (GetQuery) o;

        // It is important that we use vars() (the method) and not vars (the property)
        // vars (the property) stores the variables as the user defined
        // vars() (the method) returns match.vars() if vars (the property) is empty
        // we want to compare vars() (the method) which determines the final value
        return (this.vars().equals(that.vars()) &&
                this.match().equals(that.match()));
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
