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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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
 *     1. Plays Validation which ensures that a {@link Instance} is allowed to play the {@link RoleType}
 *        it has been assigned to.
 *     2. Relates Validation which ensures that every {@link RoleType} which is not abstract is
 *        assigned to a {@link RelationType} via {@link RelationType#relates(RoleType)}.
 *     3. Minimum Role Validation which ensures that every {@link RelationType} has at least 2 {@link RoleType}
 *        assigned to it via {@link RelationType#relates(RoleType)}.
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
     * This method checks if the plays edge has been added successfully. It does so By checking
     * Casting -CAST-> ConceptInstance -ISA-> Concept -PLAYS-> X =
     * Casting -ISA-> X
     * @param casting The casting to be validated
     * @return A specific error if one is found.
     */
    static Optional<String> validatePlaysStructure(CastingImpl casting) {
        Instance rolePlayer = casting.getRolePlayer();
        TypeImpl<?, ?> currentConcept = (TypeImpl<?, ?>) rolePlayer.type();
        RoleType roleType = casting.getRole();

        boolean satisfiesPlays = false;

        while(currentConcept != null){
            Map<RoleType, Boolean> plays = currentConcept.directPlays();

            for (Map.Entry<RoleType, Boolean> playsEntry : plays.entrySet()) {
                RoleType rolePlayed = playsEntry.getKey();
                Boolean required = playsEntry.getValue();
                if(rolePlayed.getLabel().equals(roleType.getLabel())){
                    satisfiesPlays = true;

                    // Assert unique relation for this role type
                    if (required && rolePlayer.relations(roleType).size() != 1) {
                        return Optional.of(VALIDATION_REQUIRED_RELATION.getMessage(rolePlayer.getId(), rolePlayer.type().getLabel(), roleType.getLabel(), rolePlayer.relations(roleType).size()));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.superType();
        }

        if(satisfiesPlays) {
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_CASTING.getMessage(rolePlayer.type().getLabel(), rolePlayer.getId(), casting.getRole().getLabel()));
        }
    }

    /**
     *
     * @param roleType The RoleType to validate
     * @return An error message if the relates does not have a single incoming RELATES edge
     */
    static Optional<String> validateHasSingleIncomingRelatesEdge(RoleType roleType){
        if(roleType.isAbstract()) {
            return Optional.empty();
        }
        if(roleType.relationTypes().isEmpty()) {
            return Optional.of(VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(roleType.getLabel()));
        }
        return Optional.empty();
    }

    /**
     *
     * @param relationType The RelationType to validate
     * @return An error message if the relationTypes does not have at least 2 roles
     */
    static Optional<String> validateHasMinimumRoles(RelationType relationType) {
        if(relationType.isAbstract() || relationType.relates().size() >= 1){
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_RELATION_TYPE.getMessage(relationType.getLabel()));
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
        Collection<RoleType> roleTypes = relationType.relates();

        Set<RoleType> rolesViaCastings = castings.stream().map(CastingImpl::getRole).collect(Collectors.toSet());

        if(rolesViaCastings.size() > roleTypes.size()) {
            return Optional.of(VALIDATION_RELATION_MORE_CASTING_THAN_ROLES.getMessage(relation.getId(), castings.size(), relationType.getLabel(), roleTypes.size()));
        }

        for(CastingImpl casting: castings){
            boolean notFound = true;
            for (RelationType innerRelationType : casting.getRole().relationTypes()) {
                if(innerRelationType.getLabel().equals(relationType.getLabel())){
                    notFound = false;
                    break;
                }
            }

            if(notFound) {
                return Optional.of(VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relation.getId(), casting.getRole().getLabel(), relationType.getLabel()));
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
            return Optional.of(VALIDATION_IS_ABSTRACT.getMessage(conceptType.getLabel()));
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
        if(Schema.MetaSchema.isMetaLabel(superRelationType.getLabel())){ //If super type is a meta type no validation needed
            return Collections.emptySet();
        }

        Set<String> errorMessages = new HashSet<>();

        Collection<RoleType> superRelates = superRelationType.relates();
        Collection<RoleType> relates = relationType.relates();
        Set<TypeLabel> relatesLabels = relates.stream().map(Type::getLabel).collect(Collectors.toSet());

        //TODO: Determine if this check is redundant
        //Check 1) Every role of relationTypes is the sub of a role which is in the relates of it's supers
        if(!superRelationType.isAbstract()) {
            Set<TypeLabel> allSuperRolesPlayed = new HashSet<>();
            superRelationType.superTypeSet().forEach(rel -> rel.relates().forEach(roleType -> allSuperRolesPlayed.add(roleType.getLabel())));

            for (RoleType relate : relates) {
                boolean validRoleTypeFound = false;
                Set<RoleType> superRoleTypes = ((RoleTypeImpl) relate).superTypeSet();
                for (RoleType superRoleType : superRoleTypes) {
                    if(allSuperRolesPlayed.contains(superRoleType.getLabel())){
                        validRoleTypeFound = true;
                        break;
                    }
                }

                if(!validRoleTypeFound){
                    errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(relate.getLabel(), relationType.getLabel(), "super", "super", superRelationType.getLabel()));
                }
            }
        }

        //Check 2) Every role of superRelationType has a sub role which is in the relates of relationTypes
        for (RoleType superRelate : superRelates) {
            boolean subRoleNotFoundInRelates = true;

            for (RoleType subRoleType : superRelate.subTypes()) {
                if(relatesLabels.contains(subRoleType.getLabel())){
                    subRoleNotFoundInRelates = false;
                    break;
                }
            }

            if(subRoleNotFoundInRelates){
                errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(superRelate.getLabel(), superRelationType.getLabel(), "sub", "sub", relationType.getLabel()));
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

            Map<RoleType, Boolean> plays = currentConcept.directPlays();
            for (Map.Entry<RoleType, Boolean> playsEntry : plays.entrySet()) {
                if(playsEntry.getValue()){
                    RoleType roleType = playsEntry.getKey();
                    // Assert there is a relation for this type
                    if (instance.relations(roleType).isEmpty()) {
                        return Optional.of(VALIDATION_INSTANCE.getMessage(instance.getId(), instance.type().getLabel(), roleType.getLabel()));
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

    /**
     *
     * @param rule The rule to be validated
     * @return Error messages if the lhs or rhs of a rule refers to a non existent type
     */
    static Set<String> validateRuleOntologyElementsExist(GraknGraph graph, RuleImpl rule){
        Set<String> errors = new HashSet<>();
        errors.addAll(checkRuleSideInvalid(graph, rule, "LHS", rule.getLHS()));
        errors.addAll(checkRuleSideInvalid(graph, rule, "RHS", rule.getRHS()));
        return errors;
    }

    /**
     *
     * @param graph The graph to query against
     * @param rule The rule the pattern was extracted from
     * @param side The side from which the pattern was extracted
     * @param pattern The pattern from which we will extract the types in the pattern
     * @return A list of errors if the pattern refers to any non-existent types in the graph
     */
    private static Set<String> checkRuleSideInvalid(GraknGraph graph, RuleImpl rule, String side, Pattern pattern) {
        Set<String> errors = new HashSet<>();

        pattern.admin().getVars().stream()
                .flatMap(v -> v.getInnerVars().stream())
                .flatMap(v -> v.getTypeLabels().stream()).forEach(typeLabel -> {
                    Type type = graph.getType(typeLabel);
                    if(type == null){
                        errors.add(ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(side, rule.getId(), rule.type().getLabel(), typeLabel));
                    } else {
                        if(side.equalsIgnoreCase("LHS")){
                            rule.addHypothesis(type);
                        } else {
                            rule.addConclusion(type);
                        }
                    }
                });

        return errors;
    }
}
