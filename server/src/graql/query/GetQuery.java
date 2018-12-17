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
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Variable;

import javax.annotation.CheckReturnValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/**
 * A query used for finding data in a knowledge base that matches the given patterns. The Get Query is a
 * pattern-matching query. The patterns are described in a declarative fashion, then the query will traverse
 * the knowledge base in an efficient fashion to find any matching answers.
 */
public class GetQuery implements Query {

    private final Set<Variable> vars;
    private final MatchClause match;

    public GetQuery(Set<Variable> vars, MatchClause match) {
        if (vars == null) {
            throw new NullPointerException("Null vars");
        }
        this.vars = Collections.unmodifiableSet(vars);
        if (match == null) {
            throw new NullPointerException("Null match");
        }
        for (Variable var : vars) {
            if (!match.getSelectedNames().contains(var)) {
                throw GraqlQueryException.varNotInQuery(var);
            }
        }
        this.match = match;
    }

    @CheckReturnValue
    public AggregateQuery aggregate(AggregateQuery.Method method, String... vars) {
        return aggregate(method, Arrays.stream(vars).map(Pattern::var).collect(Collectors.toSet()));
    }

    @CheckReturnValue
    public AggregateQuery aggregate(AggregateQuery.Method method, Variable var, Variable... vars) {
        Set<Variable> varSet = new HashSet<>(vars.length + 1);
        varSet.add(var);
        varSet.addAll(Arrays.asList(vars));
        return aggregate(method, varSet);
    }

    @CheckReturnValue
    public AggregateQuery aggregate(AggregateQuery.Method method, Set<Variable> vars) {
        return new AggregateQuery(this, method, vars);
    }

    @CheckReturnValue
    public GroupQuery group(String var) {
        return group(Pattern.var(var));
    }

    @CheckReturnValue
    public GroupQuery group(Variable var) {
        return new GroupQuery(this, var);
    }

    @CheckReturnValue
    public GroupAggregateQuery group(String var, AggregateQuery.Method method, String... vars) {
        return group(Pattern.var(var), method, Arrays.stream(vars).map(Pattern::var).collect(Collectors.toSet()));
    }

    @CheckReturnValue
    public GroupAggregateQuery group(Variable var, AggregateQuery.Method method, Variable aggregateVar, Variable... aggreagetVars) {
        Set<Variable> varSet = new HashSet<>(aggreagetVars.length + 1);
        varSet.add(aggregateVar);
        varSet.addAll(Arrays.asList(aggreagetVars));
        return group(var, method, varSet);
    }

    @CheckReturnValue
    public GroupAggregateQuery group(Variable var, AggregateQuery.Method method, Set<Variable> aggregateVars) {
        return new GroupAggregateQuery(this, var, method, aggregateVars);
    }

    @CheckReturnValue
    public Set<Variable> vars() {
        return vars;
    }

    @CheckReturnValue
    public MatchClause match() {
        return match;
    }

    @Override
    public String toString() {
        StringBuilder query = new StringBuilder();

        query.append(match()).append(Char.SPACE).append(Command.GET);
        if (!vars().isEmpty()) {
            query.append(Char.SPACE).append(
                    vars().stream().map(Variable::toString)
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

        return (this.vars.equals(that.vars()) &&
                this.match.equals(that.match()));
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.vars.hashCode();
        h *= 1000003;
        h ^= this.match.hashCode();
        return h;
    }
}
