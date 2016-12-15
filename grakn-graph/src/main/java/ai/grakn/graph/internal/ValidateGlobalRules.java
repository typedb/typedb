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
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.exception.MoreThanOneEdgeException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The global structural rules to validate.
 * This ensures the graph conforms to our concept.
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
     * @return A flag indicating if a valid plays-role structure exists
     */
    static boolean validatePlaysRoleStructure(CastingImpl casting) {
        InstanceImpl rolePlayer = casting.getRolePlayer();
        TypeImpl currentConcept = rolePlayer.getParentIsa();
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
                        return false;
                    }
                }
            }

            currentConcept = (TypeImpl) currentConcept.superType();
        }

        return satisfiesPlaysRole;
    }

    /**
     *
     * @param roleType The RoleType to validate
     * @return A flag indicating if the hasRole has a single incoming HAS_ROLE edge
     */
    static boolean validateHasSingleIncomingHasRoleEdge(RoleType roleType){
        if(roleType.isAbstract())
            return true;

        try {
            if(roleType.relationType() == null)
                return false;
        } catch (MoreThanOneEdgeException e){
            return false;
        }
        return true;
    }

    /**
     *
     * @param relationType The RelationType to validate
     * @return A flag indicating if the relationType has at least 2 roles
     */
    static boolean validateHasMinimumRoles(RelationType relationType) {
        return relationType.isAbstract() || relationType.hasRoles().size() >= 2;
    }

    /**
     *
     * @param relation The assertion to validate
     * @return A flag indicating that the assertions has the correct structure. This includes checking if there an equal
     * number of castings and roles as well as looping the structure to make sure castings lead to the same relation type.
     */
    static boolean validateRelationshipStructure(RelationImpl relation){
        RelationType relationType = relation.type();
        Set<CastingImpl> castings = relation.getMappingCasting();
        Collection<RoleType> roleTypes = relationType.hasRoles();

        if(castings.size() > roleTypes.size())
            return false;

        for(CastingImpl casting: castings){
            if(!casting.getRole().relationType().getName().equals(relationType.getName()))
                return false;
        }

        return true;
    }

    /**
     *
     * @param conceptType The concept type to be validated
     * @return true if the conceptType is abstract and has not incoming edges
     */
    static boolean validateIsAbstractHasNoIncomingIsaEdges(TypeImpl conceptType){
        return !conceptType.getVertex().edges(Direction.IN, Schema.EdgeLabel.ISA.getLabel()).hasNext();
    }

    /**
     *
     * @param relationType the relation type to be validated
     * @return true if the sub hierarchy of the relation type matches the sub hierarchy of the role type
     */
    static Collection<RoleType> validateRelationTypesToRolesSchema(RelationTypeImpl relationType){
        RelationTypeImpl superRelationType = (RelationTypeImpl) relationType.superType();
        if(Schema.MetaSchema.isMetaName(superRelationType.getName())){ //No super type, no validation rule
            return Collections.emptyList();
        }

        Set<RoleType> invalidTypes = new HashSet<>();

        Collection<RoleType> superHasRoles = superRelationType.hasRoles();
        Collection<RoleType> hasRoles = relationType.hasRoles();
        Set<String> hasRolesNames = hasRoles.stream().map(Type::getName).collect(Collectors.toSet());

        //TODO: Determine if this check is redundant
        //Check 1) Every role of relationType is the sub of a role which is in the hasRoles of it's supers
        if(!superRelationType.isAbstract()) {
            Set<String> allSuperRolesPlayed = new HashSet<>();
            superRelationType.getSuperSet().forEach(rel -> rel.hasRoles().forEach(roleType -> allSuperRolesPlayed.add(roleType.getName())));

            for (RoleType hasRole : hasRoles) {
                RoleType superRoleType = hasRole.superType();
                if (superRoleType == null || !allSuperRolesPlayed.contains(superRoleType.getName())) {
                    invalidTypes.add(hasRole);
                }
            }
        }

        //Check 2) Every role of superRelationType has a sub role which is in the hasRoles of relationType
        for (RoleType superHasRole : superHasRoles) {
            boolean subRoleNotFoundInHasRoles = true;

            for (RoleType subRoleType : superHasRole.subTypes()) {
                if(hasRolesNames.contains(subRoleType.getName())){
                    subRoleNotFoundInHasRoles = false;
                    break;
                }
            }

            if(subRoleNotFoundInHasRoles){
                invalidTypes.add(superHasRole);
            }
        }

        return invalidTypes;
    }

    /**
     *
     * @param instance The instance to be validated
     * @return true if the instance has all the required resources
     */
    static boolean validateInstancePlaysAllRequiredRoles(Instance instance) {
        TypeImpl<?, ?> currentConcept = (TypeImpl) instance.type();

        while(currentConcept != null){
            Set<EdgeImpl> edges = currentConcept.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.PLAYS_ROLE);
            for (EdgeImpl edge : edges) {
                Boolean required = edge.getPropertyBoolean(Schema.EdgeProperty.REQUIRED);
                if (required) {
                    RoleType roleType = edge.getTarget().asRoleType();
                    // Assert there is a relation for this type
                    if (instance.relations(roleType).isEmpty()) {
                        return false;
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.superType();
        }
        return true;
    }

    /**
     *
     * @param relation The relation whose hash needs to be set.
     * @return true if the relation is unique.
     */
    static boolean validateRelationIsUnique(RelationImpl relation){
        try{
            relation.setHash();
            return true;
        } catch (ConceptNotUniqueException e){
            return false;
        }
    }
}
