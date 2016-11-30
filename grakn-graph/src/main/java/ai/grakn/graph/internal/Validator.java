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
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.util.ErrorMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles calling the relevant validation depending on the type of the concept.
 */
class Validator {
    private final AbstractGraknGraph graknGraph;
    private final List<String> errorsFound = new ArrayList<>();

    public Validator(AbstractGraknGraph graknGraph){
        this.graknGraph = graknGraph;
    }

    /**
     *
     * @return Any errors found during validation
     */
    public List<String> getErrorsFound(){
        return errorsFound;
    }

    /**
     *
     * @return True if the data and schema conforms to our concept.
     */
    public boolean validate(){
        Set<ConceptImpl> validationList = new HashSet<>(graknGraph.getConceptLog().getModifiedConcepts());

        for(ConceptImpl nextToValidate: validationList){
            if(nextToValidate.isAlive()) {
                if (nextToValidate.isInstance()) {
                    validateInstance((InstanceImpl) nextToValidate);
                    if (nextToValidate.isRelation()) {
                        validateRelation((RelationImpl) nextToValidate);
                    }
                } else if (nextToValidate.isCasting()) {
                    validateCasting((CastingImpl) nextToValidate);
                } else if (nextToValidate.isType()) {
                    validateType((TypeImpl) nextToValidate);

                    if (nextToValidate.isRoleType()) {
                        validateRoleType((RoleTypeImpl) nextToValidate);
                    } else if (nextToValidate.isRelationType()) {
                        validateRelationType((RelationTypeImpl) nextToValidate);
                    }
                }
            }
        }
        return errorsFound.size() == 0;
    }


    /**
     * Validation rules exclusive to relations
     * @param relation The relation to validate
     */
    private void validateRelation(RelationImpl relation){
        if(!ValidateGlobalRules.validateRelationshipStructure(relation)) {
            String roles = "";
            String rolePlayers = "";
            for(Map.Entry<RoleType, Instance> entry: relation.rolePlayers().entrySet()){
                if(entry.getKey() != null)
                    roles = roles + entry.getKey().getName() + ",";
                if(entry.getValue() != null)
                    rolePlayers = rolePlayers + entry.getValue().getId() + ",";
            }
            errorsFound.add(ErrorMessage.VALIDATION_RELATION.getMessage(relation.getId(), relation.type().getName(),
                    roles.split(",").length, roles,
                    rolePlayers.split(",").length, roles));
        }
    }

    /**
     * Validation rules exclusive to castings
     * @param casting The casting to validate
     */
    private void validateCasting(CastingImpl casting){
        if(!ValidateGlobalRules.validatePlaysRoleStructure(casting)) {
            Instance rolePlayer = casting.getRolePlayer();
            errorsFound.add(ErrorMessage.VALIDATION_CASTING.getMessage(rolePlayer.type().getName(), rolePlayer.getId(), casting.getRole().getName()));
        }
    }

    /**
     * Validation rules exclusive to types
     * @param conceptType The type to validate
     */
    private void validateType(TypeImpl conceptType){
        if(conceptType.isAbstract() && !ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(conceptType))
            errorsFound.add(ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(conceptType.getName()));
    }

    /**
     * Validation rules exclusive to role types
     * @param roleType The roleType to validate
     */
    private void validateRoleType(RoleTypeImpl roleType){
        if(!ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(roleType))
            errorsFound.add(ErrorMessage.VALIDATION_ROLE_TYPE.getMessage(roleType.getName()));

        Collection<Type> invalidTypes = ValidateGlobalRules.validateRolesPlayedSchema(roleType);

        if(!invalidTypes.isEmpty()){
            invalidTypes.forEach(invalidType -> {
                errorsFound.add(ErrorMessage.VALIDATION_RULE_PLAYS_ROLES_SCHEMA.getMessage(invalidType.getName(), roleType.superType().getName()));
            });
        }
    }

    /**
     * Validation rules exclusive to relation types
     * @param relationType The relationType to validate
     */
    private void validateRelationType(RelationTypeImpl relationType){
        if(!ValidateGlobalRules.validateHasMinimumRoles(relationType))
            errorsFound.add(ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(relationType.getName()));
    }

    /**
     * Validation rules exclusive to instances
     * @param instance The instance to validate
     */
    private void validateInstance(InstanceImpl instance) {
        if (!ValidateGlobalRules.validateInstancePlaysAllRequiredRoles(instance))
            errorsFound.add(ErrorMessage.VALIDATION_INSTANCE.getMessage(instance.getId()));
    }
}
