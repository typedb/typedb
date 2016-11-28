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

import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.MoreThanOneEdgeException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
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

    /*------------------------------------------------- System Rules -------------------------------------------------*/
    /**
     * This method checks if the plays-role edge has been added successfully. It does so By checking
     * Casting -CAST-> ConceptInstance -ISA-> Concept -PLAYS_ROLE-> X =
     * Casting -ISA-> X
     * @param casting The casting to be validated
     * @return A flag indicating if a valid plays-role structure exists
     */
    static boolean validatePlaysRoleStructure(CastingImpl casting) {
        InstanceImpl rolePlayer = casting.getRolePlayer();
        TypeImpl<?, ?> currentConcept = rolePlayer.getParentIsa();
        RoleType roleType = casting.getRole();

        boolean satisfiesPlaysRole = false;
        Set<EdgeImpl> requiredPlaysRoles = new HashSet<>();

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

            currentConcept = currentConcept.getParentSub();
        }

        return satisfiesPlaysRole;
    }

    /*------------------------------------------------- Axiom Rules --------------------------------------------------*/

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



    /*--------------------------------------- Global Related TO Local Rules ------------------------------------------*/
    static boolean validateIsAbstractHasNoIncomingIsaEdges(TypeImpl conceptType){
        return !conceptType.getVertex().edges(Direction.IN, Schema.EdgeLabel.ISA.getLabel()).hasNext();
    }
}
