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

package graql.lang.query;

import graql.lang.query.builder.Filterable;
import graql.lang.statement.Variable;
import graql.lang.exception.GraqlException;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.LinkedHashSet;
import java.util.Optional;
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
public class GraqlDelete extends GraqlQuery implements Filterable {

    private final MatchClause match;
    private final LinkedHashSet<Variable> vars;
    private final Filterable.Sorting sorting;
    private final long offset;
    private final long limit;

    public GraqlDelete(MatchClause match) {
        this(match, new LinkedHashSet<>());
    }

    public GraqlDelete(MatchClause match, LinkedHashSet<Variable> vars) {
        this(match, vars, null, -1, -1);
    }

    public GraqlDelete(MatchClause match, LinkedHashSet<Variable> vars, Filterable.Sorting sorting, long offset, long limit) {
        if (match == null) {
            throw new NullPointerException("Null match");
        }
        this.match = match;

        if (vars == null) {
            throw new NullPointerException("Null vars");
        }
        for (Variable var : vars) {
            if (!match.getSelectedNames().contains(var)) {
                throw GraqlException.variableOutOfScope(var.toString());
            }
        }
        this.vars = vars;

        if (sorting != null && !vars().contains(sorting.var())) {
            throw GraqlException.variableOutOfScope(sorting.var().toString());
        }
        this.sorting = sorting;
        this.offset = offset;
        this.limit = limit;
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

    @Override
    public Optional<Sorting> sort() {
        return Optional.ofNullable(sorting);
    }

    @Override
    public Optional<Long> offset(){
        return this.offset < 0 ? Optional.empty() : Optional.of(this.offset);
    }

    @Override
    public Optional<Long> limit() {
        return limit < 0 ? Optional.empty() : Optional.of(limit);
    }

    @Override @SuppressWarnings("Duplicates")
    public String toString() {
        StringBuilder query = new StringBuilder(match().toString());
        if (match().getPatterns().getPatterns().size()>1) query.append(Token.Char.NEW_LINE);
        else query.append(Token.Char.SPACE);

        query.append(Token.Command.DELETE);
        if (!vars.isEmpty()) { // Which is not equal to !vars().isEmpty()
            query.append(Token.Char.SPACE).append(
                    vars.stream().map(Variable::toString)
                            .collect(joining(Token.Char.COMMA_SPACE.toString()))
            );
        }
        query.append(Token.Char.SEMICOLON);

        if (sort().isPresent() || offset().isPresent() || limit().isPresent()) {
            query.append(Token.Char.SPACE).append(printFilters());
        }

        return query.toString();
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!getClass().isAssignableFrom(o.getClass()) && !o.getClass().isAssignableFrom(getClass())) {
            return false;
        }

        GraqlDelete that = (GraqlDelete) o;

        // It is important that we use vars() (the method) and not vars (the property)
        // vars (the property) stores the variables as the user defined
        // vars() (the method) returns match.vars() if vars (the property) is empty
        // we want to compare vars() (the method) which determines the final value
        return (this.match().equals(that.match()) &&
                this.vars().equals(that.vars()) &&
                this.sort().equals(that.sort()) &&
                this.offset().equals(that.offset()) &&
                this.limit().equals(that.limit()));
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
        h *= 1000003;
        h ^= this.sort().hashCode();
        h *= 1000003;
        h ^= this.offset().hashCode();
        h *= 1000003;
        h ^= this.limit().hashCode();
        return h;
    }

    public static class Unfiltered extends GraqlDelete
            implements Filterable.Unfiltered<GraqlDelete.Sorted, GraqlDelete.Offsetted, GraqlDelete.Limited> {

        Unfiltered(MatchClause match, LinkedHashSet<Variable> vars) {
            super(match, vars);
        }

        @Override
        public GraqlDelete.Sorted sort(Sorting sorting) {
            return new GraqlDelete.Sorted(this, sorting);
        }

        @Override
        public GraqlDelete.Offsetted offset(long offset) {
            return new GraqlDelete.Offsetted(this, offset);
        }

        @Override
        public GraqlDelete.Limited limit(long limit) {
            return new GraqlDelete.Limited(this, limit);
        }
    }

    public class Sorted extends GraqlDelete implements Filterable.Sorted<GraqlDelete.Offsetted, GraqlDelete.Limited> {

        Sorted(GraqlDelete graqlDelete, Filterable.Sorting sorting) {
            super(graqlDelete.match, graqlDelete.vars, sorting, graqlDelete.offset, graqlDelete.limit);
        }

        @Override
        public GraqlDelete.Offsetted offset(long offset) {
            return new GraqlDelete.Offsetted(this, offset);
        }

        @Override
        public GraqlDelete.Limited limit(long limit) {
            return new GraqlDelete.Limited(this, limit);
        }
    }

    public class Offsetted extends GraqlDelete implements Filterable.Offsetted<GraqlDelete.Limited> {

        Offsetted(GraqlDelete graqlDelete, long offset) {
            super(graqlDelete.match, graqlDelete.vars, graqlDelete.sorting, offset, graqlDelete.limit);
        }

        @Override
        public GraqlDelete.Limited limit(long limit) {
            return new GraqlDelete.Limited(this, limit);
        }
    }

    public class Limited extends GraqlDelete implements Filterable.Limited {

        Limited(GraqlDelete graqlDelete, long limit) {
            super(graqlDelete.match, graqlDelete.vars, graqlDelete.sorting, graqlDelete.offset, limit);
        }
    }
}
