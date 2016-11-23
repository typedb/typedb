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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.exception.ConceptException;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.ErrorMessage;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * A DeleteQuery that will execute deletions for every result of a MatchQuery
 */
class DeleteQueryImpl implements DeleteQueryAdmin {
    private final ImmutableMap<String, VarAdmin> deleters;
    private final MatchQueryAdmin matchQuery;

    /**
     * @param deleters a collection of variable patterns to delete
     * @param matchQuery a pattern to match and delete for each result
     */
    DeleteQueryImpl(Collection<VarAdmin> deleters, MatchQuery matchQuery) {
        this.deleters = Maps.uniqueIndex(deleters, VarAdmin::getVarName);
        this.matchQuery = matchQuery.admin();
    }

    @Override
    public Void execute() {
        matchQuery.forEach(results -> results.forEach(this::deleteResult));
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
    public DeleteQuery withGraph(GraknGraph graph) {
        return Queries.delete(deleters.values(), matchQuery.withGraph(graph));
    }

    @Override
    public DeleteQuery infer() {
        return new DeleteQueryImpl(deleters.values(), matchQuery.infer());
    }

    @Override
    public DeleteQueryAdmin admin() {
        return this;
    }

    /**
     * Delete a result from a query. This may involve deleting the whole concept or specific edges, depending
     * on what deleters were provided.
     * @param name the variable name to delete
     * @param result the concept that matches the variable in the graph
     */
    private void deleteResult(String name, Concept result) {
        VarAdmin deleter = deleters.get(name);

        // Check if this has been requested to be deleted
        if (deleter == null) return;

        String id = result.getId();

        if (!deleter.getProperties().findAny().isPresent()) {
            // Delete whole concept if nothing specified to delete
            deleteConcept(id);
        } else {
            deleter.getProperties().forEach(property ->
                    ((VarPropertyInternal) property).delete(getGraph(), result)
            );
        }
    }

    /**
     * Delete a concept by ID, rethrowing errors as RuntimeExceptions
     * @param id an ID to delete in the graph
     */
    private void deleteConcept(String id) {
        try {
            getGraph().getConcept(id).delete();
        } catch (ConceptException e) {
            throw new RuntimeException(e);
        }
    }

    private GraknGraph getGraph() {
        return matchQuery.getGraph().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage())
        );
    }

    @Override
    public Collection<VarAdmin> getDeleters() {
        return deleters.values();
    }

    @Override
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }
}
