/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A {@link DeleteQuery} that will execute deletions for every result of a {@link Match}
 */
@AutoValue
abstract class DeleteQueryImpl implements DeleteQueryAdmin {

    abstract ImmutableCollection<Var> vars();

    @Override
    public abstract Match match();

    /**
     * @param vars a collection of variables to delete
     * @param match a pattern to match and delete for each result
     */
    static DeleteQueryImpl of(Collection<? extends Var> vars, Match match) {
        return new AutoValue_DeleteQueryImpl(ImmutableSet.copyOf(vars), match);
    }

    @Override
    public Stream<?> stream() {
        List<Answer> results = match().stream().collect(toList());
        results.forEach(this::deleteResult);
        return Stream.empty();
    }

    @Override
    public Void convert(Stream<?> results) {
        // Consume whole stream
        results.forEach(result -> {});
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public DeleteQuery withTx(GraknTx tx) {
        return Queries.delete(vars(), match().withTx(tx));
    }

    @Override
    public DeleteQueryAdmin admin() {
        return this;
    }

    private void deleteResult(Answer result) {
        Collection<Var> toDelete = vars().isEmpty() ? result.vars() : vars();

        for (Var var : toDelete) {
            Concept concept = result.get(var);

            if (concept.isSchemaConcept()) {
                throw GraqlQueryException.deleteSchemaConcept(concept.asSchemaConcept());
            }

            concept.delete();
        }
    }

    @Override
    public String toString() {
        return match() + " delete " + vars().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }
}
