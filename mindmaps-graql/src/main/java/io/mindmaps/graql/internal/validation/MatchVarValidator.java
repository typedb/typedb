package io.mindmaps.graql.internal.validation;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.Var;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A validator for a Var in a MatchQuery
 */
class MatchVarValidator implements Validator {

    private final Var.Admin var;

    private MindmapsTransaction transaction;
    private List<String> errors = new ArrayList<>();

    /**
     * @param var the Var in a MatchQuery to validate
     */
    public MatchVarValidator(Var.Admin var) {
        this.var = var;
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        this.transaction = transaction;
        errors = new ArrayList<>();

        var.getType().flatMap(Var.Admin::getIdOnly).ifPresent(this::validateType);

        getExpectedIds().forEach(this::validateIdExists);
        var.getRoleTypes().forEach(this::validateRoleTypeExists);

        Optional<String> optType = var.getType().flatMap(Var.Admin::getId);
        optType.ifPresent(type -> {
            if (var.isRelation()) {
                validateRelation(type, var.getRoleTypes());
            }
        });

        new ResourceValidator(var.getResourceTypes()).getErrors(this.transaction).forEach(errors::add);

        return errors.stream();
    }

    /**
     * @return all type IDs that are expected to be present in the graph
     */
    private Set<String> getExpectedIds() {
        Set<String> typeNames = new HashSet<>();
        var.getInnerVars().stream()
                .filter(v -> !v.equals(var))
                .forEach(v -> v.getId().ifPresent(typeNames::add));

        typeNames.addAll(var.getResourceTypes());
        typeNames.addAll(var.getRoleTypes());
        return typeNames;
    }

    /**
     * Confirm the given ID exists, add an error otherwise
     * @param id the ID to check in the graph
     */
    private void validateIdExists(String id) {
        if (transaction.getConcept(id) == null) {
            errors.add(ErrorMessage.ID_NOT_FOUND.getMessage(id));
        }
    }

    /**
     * Confirm the given type ID exists and can have instances
     * @param id the type ID
     */
    private void validateType(String id) {
        Type type = transaction.getType(id);
        if (type != null && type.isRoleType()) {
            errors.add(ErrorMessage.INSTANCE_OF_ROLE_TYPE.getMessage(id));
        }
    }

    /**
     * Confirm the given role type exists, add an error otherwise
     * @param id the ID to check in the graph
     */
    private void validateRoleTypeExists(String id) {
        if (transaction.getRoleType(id) == null) {
            errors.add(ErrorMessage.NOT_A_ROLE_TYPE.getMessage(id, id));
        }
    }

    /**
     * Confirm the given relation type and role type exists
     * @param id the ID of the relation type
     * @param roleTypes the IDs of the role types
     */
    private void validateRelation(String id, Collection<String> roleTypes) {
        RelationType relationType = transaction.getRelationType(id);

        if (relationType == null) {
            errors.add(ErrorMessage.NOT_A_RELATION_TYPE.getMessage(id));
        } else {

            Set<String> validRoles = relationType.hasRoles().stream()
                    .map(Concept::getId).collect(Collectors.toSet());

            roleTypes.stream()
                    .filter(roleName -> !validRoles.contains(roleName))
                    .map(roleName -> ErrorMessage.NOT_ROLE_IN_RELATION.getMessage(roleName, id, validRoles))
                    .forEach(errors::add);
        }
    }
}
