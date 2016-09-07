/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query;

import com.google.common.collect.ImmutableMap;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.exception.ConceptException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Resource;
import io.mindmaps.graql.DeleteQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.admin.DeleteQueryAdmin;
import io.mindmaps.graql.admin.MatchQueryAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.validation.DeleteQueryValidator;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A DeleteQuery that will execute deletions for every result of a MatchQuery
 */
public class DeleteQueryImpl implements DeleteQueryAdmin {
    private final ImmutableMap<String, VarAdmin> deleters;
    private final MatchQueryAdmin matchQuery;

    /**
     * @param deleters a collection of variable patterns to delete
     * @param matchQuery a pattern to match and delete for each result
     */
    public DeleteQueryImpl(Collection<VarAdmin> deleters, MatchQuery matchQuery) {
        Map<String, VarAdmin> deletersMap =
                deleters.stream().collect(Collectors.toMap(VarAdmin::getName, Function.identity()));
        this.deleters = ImmutableMap.copyOf(deletersMap);

        this.matchQuery = matchQuery.admin();

        matchQuery.admin().getGraph().ifPresent(
                graph -> new DeleteQueryValidator(this).validate(graph)
        );
    }

    @Override
    public void execute() {
        matchQuery.forEach(results -> results.forEach(this::deleteResult));
    }

    @Override
    public DeleteQuery withGraph(MindmapsGraph graph) {
        return new DeleteQueryImpl(deleters.values(), matchQuery.withGraph(graph));
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

        if (deleter.hasNoProperties()) {
            // Delete whole concept if nothing specified to delete
            deleteConcept(id);
        } else {
            deleter.getHasRoles().forEach(
                    role -> role.getId().ifPresent(
                            typeName -> getGraph().getRelationType(id).deleteHasRole(getGraph().getRoleType(typeName))
                    )
            );

            deleter.getPlaysRoles().forEach(
                    role -> role.getId().ifPresent(
                            typeName -> getGraph().getType(id).deletePlaysRole(getGraph().getRoleType(typeName))
                    )
            );

            deleter.getScopes().forEach(
                    scope -> scope.getId().ifPresent(
                            scopeName -> getGraph().getRelation(id).deleteScope(getGraph().getInstance(scopeName))
                    )
            );

            deleter.getResourceEqualsPredicates().forEach((type, values) -> deleteResources(id, type, values));
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

    /**
     * Delete all resources of the given type and given values on the given concept
     * @param id the ID of a concept
     * @param type a variable representing the resource type
     * @param values a set of values of resources
     */
    private void deleteResources(String id, VarAdmin type, Set<?> values) {
        String typeId = type.getId().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.DELETE_RESOURCE_TYPE_NO_ID.getMessage(id))
        );

        resources(id).stream()
                .filter(r -> r.type().getId().equals(typeId))
                .filter(r -> values.isEmpty() || values.contains(r.getValue()))
                .forEach(Concept::delete);
    }

    /**
     * @param id the ID of a concept
     * @return all resources on the given concept
     */
    private Collection<Resource<?>> resources(String id) {
        // Get resources attached to a concept
        // This method is necessary because the 'resource' method appears in 3 separate interfaces
        Concept concept = getGraph().getConcept(id);

        if (concept.isEntity()) {
            return concept.asEntity().resources();
        } else if (concept.isRelation()) {
            return concept.asRelation().resources();
        } else if (concept.isRule()) {
            return concept.asRule().resources();
        } else {
            return new HashSet<>();
        }
    }

    private MindmapsGraph getGraph() {
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
