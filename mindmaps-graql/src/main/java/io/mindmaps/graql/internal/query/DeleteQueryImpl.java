package io.mindmaps.graql.internal.query;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Resource;
import io.mindmaps.graql.api.query.DeleteQuery;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.Var;
import io.mindmaps.graql.internal.validation.DeleteQueryValidator;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A DeleteQuery that will execute deletions for every result of a MatchQuery
 */
public class DeleteQueryImpl implements DeleteQuery.Admin {
    private MindmapsTransaction transaction;
    private final Map<String, Var.Admin> deleters;
    private final MatchQuery matchQuery;

    /**
     * @param transaction a transaction to delete concepts from
     * @param deleters a collection of variable patterns to delete
     * @param matchQuery a pattern to match and delete for each result
     */
    public DeleteQueryImpl(MindmapsTransaction transaction, Collection<Var.Admin> deleters, MatchQuery matchQuery) {
        this.transaction = transaction;
        this.deleters = deleters.stream().collect(Collectors.toMap(Var.Admin::getName, Function.identity()));
        this.matchQuery = matchQuery;

        new DeleteQueryValidator(this).validate(transaction);
    }

    @Override
    public void execute() {
        matchQuery.forEach(results -> results.forEach(this::deleteResult));
    }

    @Override
    public DeleteQuery withTransaction(MindmapsTransaction transaction) {
        this.transaction = Objects.requireNonNull(transaction);
        matchQuery.withTransaction(transaction);
        return this;
    }

    @Override
    public Admin admin() {
        return this;
    }

    /**
     * Delete a result from a query. This may involve deleting the whole concept or specific edges, depending
     * on what deleters were provided.
     * @param name the variable name to delete
     * @param result the concept that matches the variable in the graph
     */
    private void deleteResult(String name, Concept result) {
        Var.Admin deleter = deleters.get(name);

        // Check if this has been requested to be deleted
        if (deleter == null) return;

        String id = result.getId();

        if (deleter.hasNoProperties()) {
            // Delete whole concept if nothing specified to delete
            deleteConcept(id);
        } else {
            deleter.getHasRoles().forEach(
                    role -> role.getId().ifPresent(
                            typeName -> transaction.getRelationType(id).deleteHasRole(transaction.getRoleType(typeName))
                    )
            );

            deleter.getPlaysRoles().forEach(
                    role -> role.getId().ifPresent(
                            typeName -> transaction.getType(id).deletePlaysRole(transaction.getRoleType(typeName))
                    )
            );

            deleter.getScopes().forEach(
                    scope -> scope.getId().ifPresent(
                            scopeName -> transaction.getRelation(id).deleteScope(transaction.getInstance(scopeName))
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
            transaction.getConcept(id).delete();
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
    private void deleteResources(String id, Var.Admin type, Set<?> values) {
        resources(id).stream()
                .filter(r -> r.type().getId().equals(type.getId().get()))
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
        Concept concept = transaction.getConcept(id);

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

    @Override
    public Collection<Var.Admin> getDeleters() {
        return deleters.values();
    }

    @Override
    public MatchQuery getMatchQuery() {
        return matchQuery;
    }
}
