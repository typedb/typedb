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
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.util.ErrorMessage.VALIDATION_CASTING;
import static ai.grakn.util.ErrorMessage.VALIDATION_INSTANCE;
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
 *     1. Plays Validation which ensures that a {@link Thing} is allowed to play the {@link Role}
 *        it has been assigned to.
 *     2. Relates Validation which ensures that every {@link Role} which is not abstract is
 *        assigned to a {@link RelationType} via {@link RelationType#relates(Role)}.
 *     3. Minimum Role Validation which ensures that every {@link RelationType} has at least 2 {@link Role}
 *        assigned to it via {@link RelationType#relates(Role)}.
 *     4. Relation Structure Validation which ensures that each {@link ai.grakn.concept.Relation} has the
 *        correct structure.
 *     5. Abstract Type Validation which ensures that each abstract {@link Type} has no {@link Thing}.
 *     6. Relation Type Hierarchy Validation which ensures that {@link RelationType} with a hierarchical structure
 *        have a valid matching {@link Role} hierarchical structure.
 *     7. Required Resources validation which ensures that each {@link Thing} with required
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
    static Optional<String> validatePlaysStructure(Casting casting) {
        Thing thing = casting.getInstance();
        TypeImpl<?, ?> currentConcept = (TypeImpl<?, ?>) thing.type();
        Role role = casting.getRoleType();

        boolean satisfiesPlays = false;

        while(currentConcept != null){
            Map<Role, Boolean> plays = currentConcept.directPlays();

            for (Map.Entry<Role, Boolean> playsEntry : plays.entrySet()) {
                Role rolePlayed = playsEntry.getKey();
                Boolean required = playsEntry.getValue();
                if(rolePlayed.getLabel().equals(role.getLabel())){
                    satisfiesPlays = true;

                    // Assert unique relation for this role type
                    if (required && thing.relations(role).size() != 1) {
                        return Optional.of(VALIDATION_REQUIRED_RELATION.getMessage(thing.getId(), thing.type().getLabel(), role.getLabel(), thing.relations(role).size()));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.sup();
        }

        if(satisfiesPlays) {
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_CASTING.getMessage(thing.type().getLabel(), thing.getId(), casting.getRoleType().getLabel()));
        }
    }

    /**
     *
     * @param role The Role to validate
     * @return An error message if the relates does not have a single incoming RELATES edge
     */
    static Optional<String> validateHasSingleIncomingRelatesEdge(Role role){
        if(role.relationTypes().isEmpty()) {
            return Optional.of(VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.getLabel()));
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
        Collection<Casting> castings = relation.castingsRelation().collect(Collectors.toSet());
        Collection<Role> roles = relationType.relates();

        Set<Role> rolesViaRolePlayers = castings.stream().map(Casting::getRoleType).collect(Collectors.toSet());

        if(rolesViaRolePlayers.size() > roles.size()) {
            return Optional.of(VALIDATION_RELATION_MORE_CASTING_THAN_ROLES.getMessage(relation.getId(), rolesViaRolePlayers.size(), relationType.getLabel(), roles.size()));
        }

        for(Casting casting : castings){
            boolean notFound = true;
            for (RelationType innerRelationType : casting.getRoleType().relationTypes()) {
                if(innerRelationType.getLabel().equals(relationType.getLabel())){
                    notFound = false;
                    break;
                }
            }

            if(notFound) {
                return Optional.of(VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relation.getId(), casting.getRoleType().getLabel(), relationType.getLabel()));
            }
        }

        return Optional.empty();
    }

    /**
     *
     * @param relationType the relation type to be validated
     * @return Error messages if the role type sub structure does not match the relation type sub structure
     */
    static Set<String> validateRelationTypesToRolesSchema(RelationTypeImpl relationType){
        RelationTypeImpl superRelationType = (RelationTypeImpl) relationType.sup();
        if(Schema.MetaSchema.isMetaLabel(superRelationType.getLabel())){ //If super type is a meta type no validation needed
            return Collections.emptySet();
        }

        Set<String> errorMessages = new HashSet<>();

        Collection<Role> superRelates = superRelationType.relates();
        Collection<Role> relates = relationType.relates();
        Set<Label> relatesLabels = relates.stream().map(OntologyConcept::getLabel).collect(Collectors.toSet());

        //TODO: Determine if this check is redundant
        //Check 1) Every role of relationTypes is the sub of a role which is in the relates of it's supers
        if(!superRelationType.isAbstract()) {
            Set<Label> allSuperRolesPlayed = new HashSet<>();
            superRelationType.superSet().forEach(rel -> rel.relates().forEach(roleType -> allSuperRolesPlayed.add(roleType.getLabel())));

            for (Role relate : relates) {
                boolean validRoleTypeFound = false;
                Set<Role> superRoles = ((RoleImpl) relate).superSet();
                for (Role superRole : superRoles) {
                    if(allSuperRolesPlayed.contains(superRole.getLabel())){
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
        for (Role superRelate : superRelates) {
            boolean subRoleNotFoundInRelates = true;

            for (Role subRole : superRelate.subs()) {
                if(relatesLabels.contains(subRole.getLabel())){
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
     * @param thing The thing to be validated
     * @return An error message if the thing does not have all the required resources
     */
    static Optional<String> validateInstancePlaysAllRequiredRoles(Thing thing) {
        TypeImpl<?, ?> currentConcept = (TypeImpl) thing.type();

        while(currentConcept != null){

            Map<Role, Boolean> plays = currentConcept.directPlays();
            for (Map.Entry<Role, Boolean> playsEntry : plays.entrySet()) {
                if(playsEntry.getValue()){
                    Role role = playsEntry.getKey();
                    // Assert there is a relation for this type
                    if (thing.relations(role).isEmpty()) {
                        return Optional.of(VALIDATION_INSTANCE.getMessage(thing.getId(), thing.type().getLabel(), role.getLabel()));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.sup();
        }
        return Optional.empty();
    }

    /**
     * @param graph graph used to ensure the relation is unique
     * @param relation The relation whose hash needs to be set.
     * @return An error message if the relation is not unique.
     */
    static Optional<String> validateRelationIsUnique(AbstractGraknGraph<?> graph, RelationImpl relation){
        Relation foundRelation = graph.getConcept(Schema.VertexProperty.INDEX, RelationImpl.generateNewHash(relation.type(), relation.allRolePlayers()));
        if(foundRelation == null){
            relation.setHash();
        } else if(!foundRelation.equals(relation)){
            return Optional.of(VALIDATION_RELATION_DUPLICATE.getMessage(relation));
        }
        return Optional.empty();
    }

    /**
     *
     * @param rule The rule to be validated
     * @return Error messages if the when or then of a rule refers to a non existent type
     */
    static Set<String> validateRuleOntologyElementsExist(GraknGraph graph, RuleImpl rule){
        Set<String> errors = new HashSet<>();
        errors.addAll(checkRuleSideInvalid(graph, rule, "LHS", rule.getWhen()));
        errors.addAll(checkRuleSideInvalid(graph, rule, "RHS", rule.getThen()));
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
                    OntologyConcept ontologyConcept = graph.getOntologyConcept(typeLabel);
                    if(ontologyConcept == null){
                        errors.add(ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(side, rule.getId(), rule.type().getLabel(), typeLabel));
                    } else {
                        if(side.equalsIgnoreCase("LHS")){
                            rule.addHypothesis(ontologyConcept);
                        } else {
                            rule.addConclusion(ontologyConcept);
                        }
                    }
                });

        return errors;
    }
}
