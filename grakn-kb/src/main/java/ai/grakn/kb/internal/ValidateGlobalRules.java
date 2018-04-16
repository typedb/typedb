/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.kb.internal;

/*-
 * #%L
 * grakn-kb
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.kb.internal.concept.RelationshipTypeImpl;
import ai.grakn.kb.internal.concept.RuleImpl;
import ai.grakn.kb.internal.concept.SchemaConceptImpl;
import ai.grakn.kb.internal.concept.TypeImpl;
import ai.grakn.kb.internal.structure.Casting;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.util.ErrorMessage.VALIDATION_CASTING;
import static ai.grakn.util.ErrorMessage.VALIDATION_NOT_EXACTLY_ONE_KEY;
import static ai.grakn.util.ErrorMessage.VALIDATION_RELATION_CASTING_LOOP_FAIL;
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
 *        assigned to a {@link RelationshipType} via {@link RelationshipType#relates(Role)}.
 *     3. Minimum Role Validation which ensures that every {@link RelationshipType} has at least 2 {@link Role}
 *        assigned to it via {@link RelationshipType#relates(Role)}.
 *     4. {@link Relationship} Structure Validation which ensures that each {@link Relationship} has the
 *        correct structure.
 *     5. Abstract Type Validation which ensures that each abstract {@link Type} has no {@link Thing}.
 *     6. {@link RelationshipType} Hierarchy Validation which ensures that {@link RelationshipType} with a hierarchical structure
 *        have a valid matching {@link Role} hierarchical structure.
 *     7. Required Resources validation which ensures that each {@link Thing} with required
 *        {@link Attribute} has a valid {@link Relationship} to that {@link Attribute}.
 *     8. Unique {@link Relationship} Validation which ensures that no duplicate {@link Relationship} are created.
 * </p>
 *
 * @author fppt
 */
class ValidateGlobalRules {
    private ValidateGlobalRules() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method checks if the plays edge has been added between the roleplayer's {@link Type} and
     * the {@link Role} being played.
     *
     * It also checks if the {@link Role} of the {@link Casting} has been linked to the {@link RelationshipType} of the
     * {@link Relationship} which the {@link Casting} connects to.
     *
     * @return Specific errors if any are found
     */
    static Set<String> validatePlaysAndRelatesStructure(Casting casting) {
        Set<String> errors = new HashSet<>();

        //Gets here to make sure we traverse/read only once
        Thing thing = casting.getRolePlayer();
        Role role = casting.getRole();
        Relationship relationship = casting.getRelationship();

        //Actual checks
        roleNotAllowedToBePlayed(role, thing).ifPresent(errors::add);
        roleNotLinkedToRelationShip(role, relationship.type(), relationship).ifPresent(errors::add);

        return errors;
    }

    /**
     * Checks if the {@link Role} of the {@link Casting} has been linked to the {@link RelationshipType} of
     * the {@link Relationship} which the {@link Casting} connects to.
     *
     * @param role the {@link Role} which the {@link Casting} refers to
     * @param relationshipType the {@link RelationshipType} which should connect to the role
     * @param relationship the {@link Relationship} which the {@link Casting} refers to
     * @return an error if one is found
     */
    private static Optional<String> roleNotLinkedToRelationShip(Role role, RelationshipType relationshipType, Relationship relationship){
        boolean notFound = role.relationshipTypes().
                noneMatch(innerRelationType -> innerRelationType.getLabel().equals(relationshipType.getLabel()));
        if(notFound){
            return Optional.of(VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relationship.getId(), role.getLabel(), relationshipType.getLabel()));
        }
        return Optional.empty();
    }

    /**
     * Checks  if the plays edge has been added between the roleplayer's {@link Type} and
     * the {@link Role} being played.
     *
     * Also checks that required {@link Role} are satisfied
     *
     * @param role The {@link Role} which the role-player is playing
     * @param thing the role-player
     * @return an error if one is found
     */
    private static Optional<String> roleNotAllowedToBePlayed(Role role, Thing thing){
        TypeImpl<?, ?> currentConcept = (TypeImpl<?, ?>) thing.type();

        boolean satisfiesPlays = false;
        while(currentConcept != null){
            Map<Role, Boolean> plays = currentConcept.directPlays();

            for (Map.Entry<Role, Boolean> playsEntry : plays.entrySet()) {
                Role rolePlayed = playsEntry.getKey();
                Boolean required = playsEntry.getValue();
                if(rolePlayed.getLabel().equals(role.getLabel())){
                    satisfiesPlays = true;

                    // Assert unique relationship for this role type
                    if (required && !CommonUtil.containsOnly(thing.relationships(role), 1)) {
                        return Optional.of(VALIDATION_REQUIRED_RELATION.getMessage(thing.getId(), thing.type().getLabel(), role.getLabel(), thing.relationships(role).count()));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.sup();
        }

        if(satisfiesPlays) {
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_CASTING.getMessage(thing.type().getLabel(), thing.getId(), role.getLabel()));
        }
    }

    /**
     *
     * @param role The Role to validate
     * @return An error message if the relates does not have a single incoming RELATES edge
     */
    static Optional<String> validateHasSingleIncomingRelatesEdge(Role role){
        if(!role.relationshipTypes().findAny().isPresent()) {
            return Optional.of(VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.getLabel()));
        }
        return Optional.empty();
    }

    /**
     *
     * @param relationshipType The {@link RelationshipType} to validate
     * @return An error message if the relationTypes does not have at least 1 role
     */
    static Optional<String> validateHasMinimumRoles(RelationshipType relationshipType) {
        if(relationshipType.isAbstract() || relationshipType.relates().iterator().hasNext()){
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_RELATION_TYPE.getMessage(relationshipType.getLabel()));
        }
    }

    /**
     *
     * @param relationshipType the {@link RelationshipType} to be validated
     * @return Error messages if the role type sub structure does not match the {@link RelationshipType} sub structure
     */
    static Set<String> validateRelationTypesToRolesSchema(RelationshipType relationshipType){
        RelationshipTypeImpl superRelationType = (RelationshipTypeImpl) relationshipType.sup();
        if(Schema.MetaSchema.isMetaLabel(superRelationType.getLabel()) || superRelationType.isAbstract()){ //If super type is a meta type no validation needed
            return Collections.emptySet();
        }

        Set<String> errorMessages = new HashSet<>();

        Collection<Role> superRelates = superRelationType.relates().collect(Collectors.toSet());
        Collection<Role> relates = relationshipType.relates().collect(Collectors.toSet());
        Set<Label> relatesLabels = relates.stream().map(SchemaConcept::getLabel).collect(Collectors.toSet());

        //TODO: Determine if this check is redundant
        //Check 1) Every role of relationTypes is the sub of a role which is in the relates of it's supers
        if(!superRelationType.isAbstract()) {
            Set<Label> allSuperRolesPlayed = new HashSet<>();
            superRelationType.sups().forEach(rel -> rel.relates().forEach(roleType -> allSuperRolesPlayed.add(roleType.getLabel())));

            for (Role relate : relates) {
                boolean validRoleTypeFound = SchemaConceptImpl.from(relate).sups().
                        anyMatch(superRole -> allSuperRolesPlayed.contains(superRole.getLabel()));

                if(!validRoleTypeFound){
                    errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(relate.getLabel(), relationshipType.getLabel(), "super", "super", superRelationType.getLabel()));
                }
            }
        }

        //Check 2) Every role of superRelationType has a sub role which is in the relates of relationTypes
        for (Role superRelate : superRelates) {
            boolean subRoleNotFoundInRelates = superRelate.subs().noneMatch(sub -> relatesLabels.contains(sub.getLabel()));

            if(subRoleNotFoundInRelates){
                errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(superRelate.getLabel(), superRelationType.getLabel(), "sub", "sub", relationshipType.getLabel()));
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
                    // Assert there is a relationship for this type
                    Stream<Relationship> relationships = thing.relationships(role);

                    if(!CommonUtil.containsOnly(relationships, 1)){
                        Label resourceTypeLabel = Schema.ImplicitType.explicitLabel(role.getLabel());
                        return Optional.of(VALIDATION_NOT_EXACTLY_ONE_KEY.getMessage(thing.getId(), resourceTypeLabel));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.sup();
        }
        return Optional.empty();
    }


    /**
     * @param graph graph used to ensure the rule is a valid Horn clause
     * @param rule the rule to be validated
     * @return Error messages if the rule is not a valid Horn clause (in implication form, conjunction in the body, single-atom conjunction in the head)
     */
    static Set<String> validateRuleIsValidHornClause(GraknTx graph, Rule rule){
        Set<String> errors = new HashSet<>();
        if (rule.getWhen().admin().isDisjunction()){
            errors.add(ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY.getMessage(rule.getLabel()));
        }
        if (errors.isEmpty()){
            errors.addAll(validateRuleHead(graph, rule));
        }
        return errors;
    }

    private static ReasonerQuery combinedRuleQuery(GraknTx graph, Rule rule){
        ReasonerQuery bodyQuery = rule.getWhen().admin().getDisjunctiveNormalForm().getPatterns().iterator().next().toReasonerQuery(graph);
        ReasonerQuery headQuery =  rule.getThen().admin().getDisjunctiveNormalForm().getPatterns().iterator().next().toReasonerQuery(graph);
        return headQuery.conjunction(bodyQuery);
    }

    /**
     * NB: this only gets checked if the rule obeys the Horn clause form
     * @param graph graph used to ensure the rule is a valid Horn clause
     * @param rule the rule to be validated ontologically
     * @return Error messages if the rule has ontological inconsistencies
     */
    static Set<String> validateRuleOntologically(GraknTx graph, Rule rule) {
        Set<String> errors = new HashSet<>();

        //both body and head refer to the same graph and have to be valid with respect to the schema that governs it
        //as a result the rule can be ontologically validated by combining them into a conjunction
        //this additionally allows to cross check body-head references
        ReasonerQuery combinedQuery = combinedRuleQuery(graph, rule);
        errors.addAll(combinedQuery.validateOntologically());
        return errors;
    }

    /**
     * @param graph graph used to ensure the rule head is valid
     * @param rule the rule to be validated
     * @return Error messages if the rule head is invalid - is not a single-atom conjunction, doesn't contain illegal atomics and is ontologically valid
     */
    private static Set<String> validateRuleHead(GraknTx graph, Rule rule) {
        Set<String> errors = new HashSet<>();
        Set<Conjunction<VarPatternAdmin>> headPatterns = rule.getThen().admin().getDisjunctiveNormalForm().getPatterns();

        if (headPatterns.size() != 1){
            errors.add(ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD.getMessage(rule.getLabel()));
        } else {
            ReasonerQuery bodyQuery = Iterables.getOnlyElement(rule.getWhen().admin().getDisjunctiveNormalForm().getPatterns()).toReasonerQuery(graph);
            ReasonerQuery headQuery = Iterables.getOnlyElement(headPatterns).toReasonerQuery(graph);
            ReasonerQuery combinedQuery = headQuery.conjunction(bodyQuery);

            Set<Atomic> headAtoms = headQuery.getAtoms();
            combinedQuery.getAtoms().stream()
                    .filter(headAtoms::contains)
                    .map(at -> at.validateAsRuleHead(rule))
                    .forEach(errors::addAll);
            Set<Atomic> selectableHeadAtoms = headAtoms.stream()
                    .filter(Atomic::isAtom)
                    .filter(Atomic::isSelectable)
                    .collect(Collectors.toSet());

            if (selectableHeadAtoms.size() > 1) {
                errors.add(ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC.getMessage(rule.getLabel()));
            }
        }
        return errors;
    }

    /**
     *
     * @param rule The rule to be validated
     * @return Error messages if the when or then of a rule refers to a non existent type
     */
    static Set<String> validateRuleSchemaConceptExist(GraknTx graph, Rule rule){
        Set<String> errors = new HashSet<>();
        errors.addAll(checkRuleSideInvalid(graph, rule, Schema.VertexProperty.RULE_WHEN, rule.getWhen()));
        errors.addAll(checkRuleSideInvalid(graph, rule, Schema.VertexProperty.RULE_THEN, rule.getThen()));
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
    private static Set<String> checkRuleSideInvalid(GraknTx graph, Rule rule, Schema.VertexProperty side, Pattern pattern) {
        Set<String> errors = new HashSet<>();

        pattern.admin().varPatterns().stream()
                .flatMap(v -> v.innerVarPatterns().stream())
                .flatMap(v -> v.getTypeLabels().stream()).forEach(typeLabel -> {
                    SchemaConcept schemaConcept = graph.getSchemaConcept(typeLabel);
                    if(schemaConcept == null){
                        errors.add(ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(side, rule.getLabel(), typeLabel));
                    } else {
                        if(Schema.VertexProperty.RULE_WHEN.equals(side)){
                            if (schemaConcept.isType()){
                                RuleImpl.from(rule).addHypothesis(schemaConcept.asType());
                            }
                        } else if (Schema.VertexProperty.RULE_THEN.equals(side)){
                            if (schemaConcept.isType()) {
                                RuleImpl.from(rule).addConclusion(schemaConcept.asType());
                            }
                        } else {
                            throw GraknTxOperationException.invalidPropertyUse(rule, side);
                        }
                    }
                });

        return errors;
    }

    /**
     * Checks if a {@link Relationship} has at least one role player.
     * @param relationship The {@link Relationship} to check
     */
    static Optional<String> validateRelationshipHasRolePlayers(Relationship relationship) {
        if(!relationship.rolePlayers().findAny().isPresent()){
            return Optional.of(ErrorMessage.VALIDATION_RELATIONSHIP_WITH_NO_ROLE_PLAYERS.getMessage(relationship.getId(), relationship.type().getLabel()));
        }
        return Optional.empty();
    }
}
