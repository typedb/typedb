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

package io.mindmaps.graql.internal.validation;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.VarProperty;
import io.mindmaps.graql.internal.util.CommonUtil;
import io.mindmaps.util.ErrorMessage;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * A validator for a Var in a MatchQuery
 */
class MatchVarValidator implements Validator {

    private final VarAdmin var;

    private MindmapsGraph graph;
    private List<String> errors = new ArrayList<>();

    /**
     * @param var the Var in a MatchQuery to validate
     */
    MatchVarValidator(VarAdmin var) {
        this.var = var;
    }

    @Override
    public Stream<String> getErrors(MindmapsGraph graph) {
        this.graph = graph;
        errors = new ArrayList<>();

        var.getType().flatMap(VarAdmin::getIdOnly).ifPresent(this::validateType);

        getExpectedIds().forEach(this::validateIdExists);
        var.getRoleTypes().forEach(this::validateRoleTypeExists);

        Optional<String> optType = var.getType().flatMap(VarAdmin::getId);
        optType.ifPresent(type -> {
            if (var.isRelation()) {
                validateRelation(type, var.getRoleTypes());
            }
        });

        new ResourceValidator(var.getResourceTypes()).getErrors(this.graph).forEach(errors::add);

        return errors.stream();
    }

    /**
     * @return all type IDs that are expected to be present in the graph
     */
    private Set<String> getExpectedIds() {
        return var.getInnerVars().stream()
                .flatMap(VarAdmin::getProperties)
                .flatMap(VarProperty::getTypes)
                .filter(other -> !other.equals(var))
                .map(VarAdmin::getId)
                .flatMap(CommonUtil::optionalToStream)
                .collect(toSet());
    }

    /**
     * Confirm the given ID exists, add an error otherwise
     * @param id the ID to check in the graph
     */
    private void validateIdExists(String id) {
        if (graph.getConcept(id) == null) {
            errors.add(ErrorMessage.ID_NOT_FOUND.getMessage(id));
        }
    }

    /**
     * Confirm the given type ID exists and can have instances
     * @param id the type ID
     */
    private void validateType(String id) {
        Type type = graph.getType(id);
        if (type != null && type.isRoleType()) {
            errors.add(ErrorMessage.INSTANCE_OF_ROLE_TYPE.getMessage(id));
        }
    }

    /**
     * Confirm the given role type exists, add an error otherwise
     * @param id the ID to check in the graph
     */
    private void validateRoleTypeExists(String id) {
        if (graph.getRoleType(id) == null) {
            errors.add(ErrorMessage.NOT_A_ROLE_TYPE.getMessage(id, id));
        }
    }

    /**
     * Confirm the given relation type and role type exists
     * @param id the ID of the relation type
     * @param roleTypes the IDs of the role types
     */
    private void validateRelation(String id, Collection<String> roleTypes) {
        RelationType relationType = graph.getRelationType(id);

        if (relationType == null) {
            errors.add(ErrorMessage.NOT_A_RELATION_TYPE.getMessage(id));
            return;
        }

        Collection<RelationType> relationTypes = relationType.subTypes();

        Set<String> validRoles = relationTypes.stream()
                .flatMap(r -> r.hasRoles().stream())
                .map(Concept::getId)
                .collect(toSet());

        roleTypes.stream()
                .filter(roleType -> roleType != null && !validRoles.contains(roleType))
                .map(roleType -> ErrorMessage.NOT_ROLE_IN_RELATION.getMessage(roleType, id, validRoles))
                .forEach(errors::add);
    }
}
