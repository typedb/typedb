package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.MoreThanOneEdgeException;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Set;

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
    public static boolean validatePlaysRoleStructure(CastingImpl casting) {
        InstanceImpl rolePlayer = casting.getRolePlayer();
        TypeImpl<?, ?> currentRolePlayerType = rolePlayer.getParentIsa();
        RoleType roleType = casting.getRole();
        Set<Type> rolePlayerTypes = currentRolePlayerType.getAkoHierarchySuperSet();
        Collection<Type> allowedTypes = roleType.playedByTypes();

        if(allowedTypes.size() == 0)
            return false;

        return rolePlayerTypes.containsAll(roleType.playedByTypes());
    }

    /*------------------------------------------------- Axiom Rules --------------------------------------------------*/

    /**
     *
     * @param roleType The RoleType to validate
     * @return A flag indicating if the hasRole has a single incoming HAS_ROLE edge
     */
    public static boolean validateHasSingleIncomingHasRoleEdge(RoleType roleType){
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
     * @param roleType The RoleType to validate
     * @return A flag indicating if an abstrcat role has an incoming plays role edge
     */
    public static boolean validateAbstractRoleTypeNotPlayingRole(RoleTypeImpl roleType){
        if(roleType.isAbstract()){
            return !roleType.getVertex().edges(Direction.IN, DataType.EdgeLabel.PLAYS_ROLE.getLabel()).hasNext();
        }
        return true;
    }

    /**
     *
     * @param relationType The RelationType to validate
     * @return A flag indicating if the relationType has at least 2 roles
     */
    public static boolean validateHasMinimumRoles(RelationType relationType) {
        return relationType.isAbstract() || relationType.hasRoles().size() >= 2;
    }

    /**
     *
     * @param relation The assertion to validate
     * @return A flag indicating that the assertions has the correct structure. This includes checking if there an equal
     * number of castings and roles as well as looping the structure to make sure castings lead to the same relation type.
     */
    public static boolean validateRelationshipStructure(RelationImpl relation){
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
    public static boolean validateIsAbstractHasNoIncomingIsaEdges(TypeImpl conceptType){
        return !conceptType.getVertex().edges(Direction.IN, DataType.EdgeLabel.ISA.getLabel()).hasNext();
    }
}
