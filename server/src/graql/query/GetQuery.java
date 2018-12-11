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

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.Transaction;

import javax.annotation.CheckReturnValue;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A query used for finding data in a knowledge base that matches the given patterns. The {@link GetQuery} is a
 * pattern-matching query. The patterns are described in a declarative fashion, then the {@link GetQuery} will traverse
 * the knowledge base in an efficient fashion to find any matching answers.
 */
public class GetQuery implements Query<ConceptMap> {

    private final ImmutableSet<Variable> vars;
    private final Match match;

    public GetQuery(ImmutableSet<Variable> vars, Match match) {
        if (vars == null) {
            throw new NullPointerException("Null vars");
        }
        this.vars = vars;
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
    public ImmutableSet<Variable> vars() {
        return vars;
    }

    @CheckReturnValue
    public Match match() {
        return match;
    }

    @Override
    public final Transaction tx() {
        return match().tx();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Stream<ConceptMap> stream() {
        return executor().run(this);
    }

    @Override
    public Stream<ConceptMap> stream(boolean infer) {
        return executor(infer).run(this);
    }

    @Override
    public String toString() {
        return match().toString() + " get " + vars().stream().map(Object::toString).collect(joining(", ")) + ";";
    }

    @Override
    public final Boolean inferring() {
        return match().inferring();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof GetQuery) {
            GetQuery that = (GetQuery) o;
            return (this.vars.equals(that.vars()))
                    && (this.match.equals(that.match()));
        }
        return false;
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
