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
import ai.grakn.exception.MoreThanOneEdgeException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
                if (edge.getTarget().equals(roleType)) {
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
            if(!casting.getRole().relationType().equals(relationType))
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
     * Schema validation which makes sure that the roles the entity type plays are correct. They are correct if
     * For every T1 such that T1 plays-role R1 there must exist some T2, such that T1 sub* T2 and T2 plays-role R2
     * @param roleType The entity type to validate
     */
    @SuppressWarnings("unchecked")
    static Collection<Type> validateRolesPlayedSchema(RoleTypeImpl roleType){
        RoleType superRoleType = roleType.superType();
        if(superRoleType == null){ //No super role type no validation. I.e R1 sub R2 does not exist
            return Collections.emptyList();
        }

        Set<Type> invalidTypes = new HashSet<>();
        Collection<Type> typesAllowedToPlay = roleType.playedByTypes();
        for (Type typeAllowedToPlay : typesAllowedToPlay) {

            //Getting T1 sub* T2
            Set<Type> superTypesAllowedToPlay = ((TypeImpl) typeAllowedToPlay).getSubHierarchySuperSet();
            boolean superRoleTypeFound = false;

            for (Type superTypeAllowedToPlay : superTypesAllowedToPlay) {
                if(superTypeAllowedToPlay.playsRoles().contains(superRoleType)){
                    superRoleTypeFound = true;
                    break;
                }
            }

            if(!superRoleTypeFound){ //We found a type whose parent cannot play R2
                invalidTypes.add(typeAllowedToPlay);
            }
        }

        return invalidTypes;
    }

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
}
