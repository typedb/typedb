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

package ai.grakn.graph.internal;

import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.VALIDATION_CASTING;
import static ai.grakn.util.ErrorMessage.VALIDATION_INSTANCE;
import static ai.grakn.util.ErrorMessage.VALIDATION_IS_ABSTRACT;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_CASTING_LOOP_FAIL;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_DUPLICATE;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_MORE_CASTING_THAN_ROLES;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_TYPE;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA;
import static ai.grakn.util.ErrorMessage.VALIDATION_REQUIRED_RELATION;
import static ai.grakn.util.ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE;

/**
 * <p>
 *     Specific Validation Rules
 * </p>
 *
 * <p>
 *     This class contains the implementation for the following validation rules:
 *     1. Plays Role Validation which ensures that a {@link Instance} is allowed to play the {@link RoleType}
 *        it has been assigned to.
 *     2. Has Role Validation which ensures that every {@link RoleType} which is not abstract is
 *        assigned to a {@link RelationType} via {@link RelationType#hasRole(RoleType)}.
 *     3. Minimum Role Validation which ensures that every {@link RelationType} has at least 2 {@link RoleType}
 *        assigned to it via {@link RelationType#hasRole(RoleType)}.
 *     4. Relation Structure Validation which ensures that each {@link ai.grakn.concept.Relation} has the
 *        correct structure.
 *     5. Abstract Type Validation which ensures that each abstract {@link Type} has no {@link Instance}.
 *     6. Relation Type Hierarchy Validation which ensures that {@link RelationType} with a hierarchical structure
 *        have a valid matching {@link RoleType} hierarchical structure.
 *     7. Required Resources validation which ensures that each {@link Instance} with required
 *        {@link ai.grakn.concept.Resource} has a valid {@link ai.grakn.concept.Relation} to that Resource.
 *     8. Unique Relation Validation which ensures that no duplicate {@link ai.grakn.concept.Relation} are created.
 * </p>
 *
 * @author fppt
 */
class ValidateGlobalRules {
    private ValidateGlobalRules() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method checks if the plays-role edge has been added successfully. It does so By checking
     * Casting -CAST-> ConceptInstance -ISA-> Concept -PLAYS_ROLE-> X =
     * Casting -ISA-> X
     * @param casting The casting to be validated
     * @return A specific error if one is found.
     */
    static Optional<String> validatePlaysRoleStructure(CastingImpl casting) {
        InstanceImpl<?, ?> rolePlayer = casting.getRolePlayer();
        TypeImpl<?, ?> currentConcept = (TypeImpl<?, ?>) rolePlayer.type();
        RoleType roleType = casting.getRole();

        boolean satisfiesPlaysRole = false;

        while(currentConcept != null){
            Set<EdgeImpl> edges = currentConcept.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS_ROLE);

            for (EdgeImpl edge : edges) {
                if (edge.getTarget().asType().getName().equals(roleType.getName())) {
                    satisfiesPlaysRole = true;

                    // Assert unique relation for this role type
                    Boolean required = edge.getPropertyBoolean(Schema.EdgeProperty.REQUIRED);
                    if (required && rolePlayer.relations(roleType).size() != 1) {
                        return Optional.of(VALIDATION_REQUIRED_RELATION.getMessage(rolePlayer.getId(), roleType.getName(), rolePlayer.relations(roleType).size()));
                    }
                }
            }

            currentConcept = (TypeImpl) currentConcept.superType();
        }

        if(satisfiesPlaysRole) {
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_CASTING.getMessage(rolePlayer.type().getName(), rolePlayer.getId(), casting.getRole().getName()));
        }
    }

    /**
     *
     * @param roleType The RoleType to validate
     * @return An error message if the hasRole does not have a single incoming HAS_ROLE edge
     */
    static Optional<String> validateHasSingleIncomingHasRoleEdge(RoleType roleType){
        if(roleType.isAbstract()) {
            return Optional.empty();
        }
        if(roleType.relationTypes().isEmpty()) {
            return Optional.of(VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(roleType.getName()));
        }
        return Optional.empty();
    }

    /**
     *
     * @param relationType The RelationType to validate
     * @return An error message if the relationTypes does not have at least 2 roles
     */
    static Optional<String> validateHasMinimumRoles(RelationType relationType) {
        if(relationType.isAbstract() || relationType.hasRoles().size() >= 2){
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_RELATION_TYPE.getMessage(relationType.getName()));
        }
    }

    /**
     *
     * @param relation The assertion to validate
     * @return An error message indicating if the relation has an incorrect structure. This includes checking if there an equal
     * number of castings and roles as well as looping the structure to make sure castings lead to the same relation type.
     */
    static Optional<String> validateRelationshipStructure(RelationImpl relation){
        RelationType relationType = relation.type();
        Set<CastingImpl> castings = relation.getMappingCasting();
        Collection<RoleType> roleTypes = relationType.hasRoles();

        if(castings.size() > roleTypes.size()) {
            return Optional.of(VALIDATION_RELATION_MORE_CASTING_THAN_ROLES.getMessage(relation.getId(), castings.size(), relationType.getName(), roleTypes.size()));
        }

        for(CastingImpl casting: castings){
            boolean notFound = true;
            for (RelationType innerRelationType : casting.getRole().relationTypes()) {
                if(innerRelationType.getName().equals(relationType.getName())){
                    notFound = false;
                    break;
                }
            }

            if(notFound) {
                return Optional.of(VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relation.getId(), casting.getRole().getName(), relationType.getName()));
            }
        }

        return Optional.empty();
    }

    /**
     *
     * @param conceptType The concept type to be validated
     * @return An error message if the conceptType  abstract and has incoming isa edges
     */
    static Optional<String> validateIsAbstractHasNoIncomingIsaEdges(TypeImpl conceptType){
        if(conceptType.isAbstract() && conceptType.getVertex().edges(Direction.IN, Schema.EdgeLabel.ISA.getLabel()).hasNext()){
            return Optional.of(VALIDATION_IS_ABSTRACT.getMessage(conceptType.getName()));
        }
        return Optional.empty();
    }

    /**
     *
     * @param relationType the relation type to be validated
     * @return Error messages if the role type sub structure does not match the relation type sub structure
     */
    static Set<String> validateRelationTypesToRolesSchema(RelationTypeImpl relationType){
        RelationTypeImpl superRelationType = (RelationTypeImpl) relationType.superType();
        if(Schema.MetaSchema.isMetaName(superRelationType.getName())){ //If super type is a meta type no validation needed
            return Collections.emptySet();
        }

        Set<String> errorMessages = new HashSet<>();

        Collection<RoleType> superHasRoles = superRelationType.hasRoles();
        Collection<RoleType> hasRoles = relationType.hasRoles();
        Set<TypeName> hasRolesNames = hasRoles.stream().map(Type::getName).collect(Collectors.toSet());

        //TODO: Determine if this check is redundant
        //Check 1) Every role of relationTypes is the sub of a role which is in the hasRoles of it's supers
        if(!superRelationType.isAbstract()) {
            Set<TypeName> allSuperRolesPlayed = new HashSet<>();
            superRelationType.getSuperSet().forEach(rel -> rel.hasRoles().forEach(roleType -> allSuperRolesPlayed.add(roleType.getName())));

            for (RoleType hasRole : hasRoles) {
                RoleType superRoleType = hasRole.superType();
                if (superRoleType == null || !allSuperRolesPlayed.contains(superRoleType.getName())) {
                    errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(hasRole.getName(), relationType.getName(), "super", "super", superRelationType.getName()));
                }
            }
        }

        //Check 2) Every role of superRelationType has a sub role which is in the hasRoles of relationTypes
        for (RoleType superHasRole : superHasRoles) {
            boolean subRoleNotFoundInHasRoles = true;

            for (RoleType subRoleType : superHasRole.subTypes()) {
                if(hasRolesNames.contains(subRoleType.getName())){
                    subRoleNotFoundInHasRoles = false;
                    break;
                }
            }

            if(subRoleNotFoundInHasRoles){
                errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(superHasRole.getName(), superRelationType.getName(), "sub", "sub", relationType.getName()));
            }
        }

       return errorMessages;
    }

    /**
     *
     * @param instance The instance to be validated
     * @return An error message if the instance does not have all the required resources
     */
    static Optional<String> validateInstancePlaysAllRequiredRoles(Instance instance) {
        TypeImpl<?, ?> currentConcept = (TypeImpl) instance.type();

        while(currentConcept != null){
            Set<EdgeImpl> edges = currentConcept.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS_ROLE);
            for (EdgeImpl edge : edges) {
                Boolean required = edge.getPropertyBoolean(Schema.EdgeProperty.REQUIRED);
                if (required) {
                    RoleType roleType = edge.getTarget().asRoleType();
                    // Assert there is a relation for this type
                    if (instance.relations(roleType).isEmpty()) {
                        return Optional.of(VALIDATION_INSTANCE.getMessage(instance.getId()));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.superType();
        }
        return Optional.empty();
    }

    /**
     *
     * @param relation The relation whose hash needs to be set.
     * @return An error message if the relation is not unique.
     */
    static Optional<String> validateRelationIsUnique(RelationImpl relation){
        try{
            relation.setHash();
            return Optional.empty();
        } catch (ConceptNotUniqueException e){
            return Optional.of(VALIDATION_RELATION_DUPLICATE.getMessage(relation));
        }
    }
}
