package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.RoleType;

import java.util.*;

class Validator {
    private final MindmapsTransactionImpl mindmapsGraph;
    private final List<String> errorsFound = new ArrayList<>();

    public Validator(MindmapsTransactionImpl mindmapsGraph){
        this.mindmapsGraph = mindmapsGraph;
    }


    public List<String> getErrorsFound(){
        return errorsFound;
    }

    public boolean validate(){
        Set<ConceptImpl> validationList = new HashSet<>(mindmapsGraph.getModifiedConcepts());

        for(ConceptImpl nextToValidate: validationList){
            if(nextToValidate.isRelation()){
                validateRelation((RelationImpl) nextToValidate);
            } else if(nextToValidate.isCasting()){
                validateCasting((CastingImpl) nextToValidate);
            } else if(nextToValidate.isType()){
                validateType((TypeImpl) nextToValidate);
                if(nextToValidate.isRoleType()){
                    validateRoleType((RoleTypeImpl) nextToValidate);
                } else if(nextToValidate.isRelationType()){
                    validateRelationType((RelationTypeImpl) nextToValidate);
                }
            }
        }
        return errorsFound.size() == 0;
    }


    private void logError(String error){
        errorsFound.add(error);
    }

    private void validateRelation(RelationImpl relation){
        if(!ValidateGlobalRules.validateRelationshipStructure(relation)) {
            String roles = "";
            String rolePlayers = "";
            for(Map.Entry<RoleType, Instance> entry: relation.rolePlayers().entrySet()){
                if(entry.getKey() != null)
                    roles = roles + entry.getKey().getId() + ",";
                if(entry.getValue() != null)
                    rolePlayers = rolePlayers + entry.getValue().getId() + ",";
            }
            logError(ErrorMessage.VALIDATION_RELATION.getMessage(relation.getId(), relation.type().getId(),
                    roles.split(",").length, roles,
                    rolePlayers.split(",").length, roles));
        }
    }

    private void validateCasting(CastingImpl casting){
        if(!ValidateGlobalRules.validatePlaysRoleStructure(casting)) {
            Instance rolePlayer = casting.getRolePlayer();
            logError(ErrorMessage.VALIDATION_CASTING.getMessage(rolePlayer.type().getId(), rolePlayer.getId(), casting.getRole().getId()));
        }
    }

    private void validateType(TypeImpl conceptType){
        if(conceptType.isAbstract() && !ValidateGlobalRules.validateIsAbstractHasNoIncomingIsaEdges(conceptType))
            logError(ErrorMessage.VALIDATION_IS_ABSTRACT.getMessage(conceptType.getId()));
    }

    private void validateRoleType(RoleTypeImpl roleType){
        if(!ValidateGlobalRules.validateHasSingleIncomingHasRoleEdge(roleType))
            logError(ErrorMessage.VALIDATION_ROLE_TYPE.getMessage(roleType.getId()));
        if(!ValidateGlobalRules.validateAbstractRoleTypeNotPlayingRole(roleType))
            logError(ErrorMessage.VALIDATION_ROLE_TYPE_ABSTRACT.getMessage(roleType.getId(), roleType.playedByTypes().iterator().next().getId()));
    }

    private void validateRelationType(RelationTypeImpl relationType){
        if(!ValidateGlobalRules.validateHasMinimumRoles(relationType))
            logError(ErrorMessage.VALIDATION_RELATION_TYPE.getMessage(relationType.getId()));
    }
}
