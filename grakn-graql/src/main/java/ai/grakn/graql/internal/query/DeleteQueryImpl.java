/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A DeleteQuery that will execute deletions for every result of a MatchQuery
 */
class DeleteQueryImpl implements DeleteQueryAdmin {
    private final ImmutableCollection<VarPatternAdmin> deleters;
    private final MatchQueryAdmin matchQuery;

    /**
     * @param deleters a collection of variable patterns to delete
     * @param matchQuery a pattern to match and delete for each result
     */
    DeleteQueryImpl(Collection<VarPatternAdmin> deleters, MatchQuery matchQuery) {
        if (deleters.isEmpty()) {
            throw GraqlQueryException.noPatterns();
        }

        this.deleters = ImmutableSet.copyOf(deleters);
        this.matchQuery = matchQuery.admin();
    }

    @Override
    public Void execute() {
        List<Answer> results = matchQuery.execute();
        results.forEach(this::deleteResult);
        return null;
    }

    @Override
    public Stream<String> resultsString(Printer printer) {
        execute();
        return Stream.empty();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public DeleteQuery withTx(GraknTx tx) {
        return Queries.delete(deleters, matchQuery.withTx(tx));
    }

    @Override
    public DeleteQueryAdmin admin() {
        return this;
    }

    private void deleteResult(Answer result) {
        for (VarPatternAdmin deleter : deleters) {
            Concept concept = result.get(deleter.var());

            if (concept == null) {
                throw GraqlQueryException.varNotInQuery(deleter.var());
            }

            deletePattern(concept, deleter);
        }
    }

    /**
     * Delete a result from a query. This may involve deleting the whole concept or specific edges, depending
     * on what deleters were provided.
     * @param result the concept that matches the variable in the graph
     * @param deleter the pattern to delete on the concept
     */
    private void deletePattern(Concept result, VarPatternAdmin deleter) {
        if (!deleter.getProperties().findAny().isPresent()) {
            // Delete whole concept if nothing specified to delete
            result.delete();
        } else {
            deleter.getProperties().forEach(property ->
                    ((VarPropertyInternal) property).delete(tx(), result)
            );
        }
    }

    private GraknTx tx() {
        return matchQuery.tx().orElseThrow(GraqlQueryException::noTx);
    }

    @Override
    public Collection<VarPatternAdmin> getDeleters() {
        return deleters;
    }

    @Override
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }

    @Override
    public String toString() {
        return matchQuery + " delete " + deleters.stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeleteQueryImpl that = (DeleteQueryImpl) o;

        if (!deleters.equals(that.deleters)) return false;
        return matchQuery.equals(that.matchQuery);
    }

    @Override
    public int hashCode() {
        int result = deleters.hashCode();
        result = 31 * result + matchQuery.hashCode();
        return result;
    }
}
