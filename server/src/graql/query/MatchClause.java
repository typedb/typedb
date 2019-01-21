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

import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import graql.exception.GraqlException;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A part of a query used for finding data in a knowledge base that matches the given patterns.
 * The match clause is the pattern-matching part of a query. The patterns are described in a declarative fashion,
 * then the match clause will traverse the knowledge base in an efficient fashion to find any matching answers.
 */
public class MatchClause {

    private final Conjunction<Pattern> pattern;

    public MatchClause(Conjunction<Pattern> pattern) {
        if (pattern.getPatterns().size() == 0) {
            throw GraqlException.noPatterns();
        }

        this.pattern = pattern;
    }

    @CheckReturnValue
    public final Conjunction<Pattern> getPatterns() {
        return pattern;
    }

    @CheckReturnValue
    public final Set<Variable> getSelectedNames() {
        return pattern.variables();
    }

    /**
     * Construct a get query with all all variables mentioned in the query
     */
    @CheckReturnValue
    public GetQuery get() {
        return new GetQuery(this);
    }

    /**
     * @param vars an array of variables to select
     * @return a Get Query that selects the given variables
     */
    @CheckReturnValue
    public GetQuery get(String var, String... vars) {
        LinkedHashSet<Variable> varSet = Stream
                .concat(Stream.of(var), Stream.of(vars))
                .map(Variable::new)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return get(varSet);
    }

    /**
     * @param vars an array of variables to select
     * @return a Get Query that selects the given variables
     */
    @CheckReturnValue
    public GetQuery get(Variable var, Variable... vars) {
        LinkedHashSet<Variable> varSet = new LinkedHashSet<>();
        varSet.add(var);
        varSet.addAll(Arrays.asList(vars));
        return get(varSet);
    }

    /**
     * @param vars a set of variables to select
     * @return a Get Query that selects the given variables
     */
    @CheckReturnValue
    public GetQuery get(List<Variable> vars) {
        return get(new LinkedHashSet<>(vars));
    }

    /**
     * @param vars a set of variables to select
     * @return a Get Query that selects the given variables
     */
    @CheckReturnValue
    public GetQuery get(LinkedHashSet<Variable> vars) {
        return new GetQuery(this, vars);
    }

    /**
     * @param vars an array of variables to insert for each result of this match clause
     * @return an insert query that will insert the given variables for each result of this match clause
     */
    @CheckReturnValue
    public final InsertQuery insert(Statement... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert for each result of this match clause
     * @return an insert query that will insert the given variables for each result of this match clause
     */
    @CheckReturnValue
    public final InsertQuery insert(Collection<? extends Statement> vars) {
        MatchClause match = this;
        return new InsertQuery(match, Collections.unmodifiableList(new ArrayList<>(vars)));
    }

    /**
     * Construct a delete query with all all variables mentioned in the query
     */
    @CheckReturnValue
    public DeleteQuery delete() {
        return new DeleteQuery(this);
    }

    /**
     * @param vars an array of variables to delete for each result of this match clause
     * @return a delete query that will delete the given variables for each result of this match clause
     */
    @CheckReturnValue
    public final DeleteQuery delete(String var, String... vars) {
        LinkedHashSet<Variable> varSet = Stream
                .concat(Stream.of(var), Stream.of(vars))
                .map(Variable::new)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return delete(varSet);
    }

    /**
     * @param vars an array of variables to delete for each result of this match clause
     * @return a delete query that will delete the given variables for each result of this match clause
     */
    @CheckReturnValue
    public final DeleteQuery delete(Variable var, Variable... vars) {
        LinkedHashSet<Variable> varSet = new LinkedHashSet<>();
        varSet.add(var);
        varSet.addAll(Arrays.asList(vars));
        return delete(varSet);
    }

    /**
     * @param vars a collection of variables to delete for each result of this match clause
     * @return a delete query that will delete the given variables for each result of this match clause
     */
    @CheckReturnValue
    public final DeleteQuery delete(List<Variable> vars) {
        return new DeleteQuery(this, new LinkedHashSet<>(vars));
    }

    /**
     * @param vars a collection of variables to delete for each result of this match clause
     * @return a delete query that will delete the given variables for each result of this match clause
     */
    @CheckReturnValue
    public final DeleteQuery delete(LinkedHashSet<Variable> vars) {
        return new DeleteQuery(this, vars);
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(Query.Command.MATCH);
        if (pattern.getPatterns().size()>1) query.append(Query.Char.NEW_LINE);
        else query.append(Query.Char.SPACE);

        query.append(pattern.getPatterns().stream()
                             .map(Object::toString)
                             .collect(Collectors.joining(Query.Char.NEW_LINE.toString())));

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchClause other = (MatchClause) o;
        return pattern.equals(other.pattern);
    }

    @Override
    public int hashCode() { // TODO: multiple with a large prime number
        return pattern.hashCode();
    }
}
