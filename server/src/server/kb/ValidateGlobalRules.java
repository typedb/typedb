/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.kb;

import com.google.common.collect.Iterables;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.util.CommonUtil;
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.internal.reasoner.rule.RuleUtils;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Disjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.server.Transaction;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.concept.RelationshipTypeImpl;
import grakn.core.server.kb.concept.RuleImpl;
import grakn.core.server.kb.concept.SchemaConceptImpl;
import grakn.core.server.kb.concept.TypeImpl;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.session.TransactionOLTP;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.VALIDATION_CASTING;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_NOT_EXACTLY_ONE_KEY;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_RELATION_CASTING_LOOP_FAIL;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_RELATION_TYPE;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_REQUIRED_RELATION;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE;

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
 *     4. {@link Relation} Structure Validation which ensures that each {@link Relation} has the
 *        correct structure.
 *     5. Abstract Type Validation which ensures that each abstract {@link Type} has no {@link Thing}.
 *     6. {@link RelationType} Hierarchy Validation which ensures that {@link RelationType} with a hierarchical structure
 *        have a valid matching {@link Role} hierarchical structure.
 *     7. Required Resources validation which ensures that each {@link Thing} with required
 *        {@link Attribute} has a valid {@link Relation} to that {@link Attribute}.
 *     8. Unique {@link Relation} Validation which ensures that no duplicate {@link Relation} are created.
 * </p>
 *
 */
class ValidateGlobalRules {
    private ValidateGlobalRules() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method checks if the plays edge has been added between the roleplayer's {@link Type} and
     * the {@link Role} being played.
     *
     * It also checks if the {@link Role} of the {@link Casting} has been linked to the {@link RelationType} of the
     * {@link Relation} which the {@link Casting} connects to.
     *
     * @return Specific errors if any are found
     */
    static Set<String> validatePlaysAndRelatesStructure(Casting casting) {
        Set<String> errors = new HashSet<>();

        //Gets here to make sure we traverse/read only once
        Thing thing = casting.getRolePlayer();
        Role role = casting.getRole();
        Relation relationship = casting.getRelationship();

        //Actual checks
        roleNotAllowedToBePlayed(role, thing).ifPresent(errors::add);
        roleNotLinkedToRelationShip(role, relationship.type(), relationship).ifPresent(errors::add);

        return errors;
    }

    /**
     * Checks if the {@link Role} of the {@link Casting} has been linked to the {@link RelationType} of
     * the {@link Relation} which the {@link Casting} connects to.
     *
     * @param role the {@link Role} which the {@link Casting} refers to
     * @param relationshipType the {@link RelationType} which should connect to the role
     * @param relationship the {@link Relation} which the {@link Casting} refers to
     * @return an error if one is found
     */
    private static Optional<String> roleNotLinkedToRelationShip(Role role, RelationType relationshipType, Relation relationship){
        boolean notFound = role.relationships().
                noneMatch(innerRelationType -> innerRelationType.label().equals(relationshipType.label()));
        if(notFound){
            return Optional.of(VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relationship.id(), role.label(), relationshipType.label()));
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
                if(rolePlayed.label().equals(role.label())){
                    satisfiesPlays = true;

                    // Assert unique relationship for this role type
                    if (required && !CommonUtil.containsOnly(thing.relationships(role), 1)) {
                        return Optional.of(VALIDATION_REQUIRED_RELATION.getMessage(thing.id(), thing.type().label(), role.label(), thing.relationships(role).count()));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.sup();
        }

        if(satisfiesPlays) {
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_CASTING.getMessage(thing.type().label(), thing.id(), role.label()));
        }
    }

    /**
     *
     * @param role The Role to validate
     * @return An error message if the relates does not have a single incoming RELATES edge
     */
    static Optional<String> validateHasSingleIncomingRelatesEdge(Role role){
        if(!role.relationships().findAny().isPresent()) {
            return Optional.of(VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.label()));
        }
        return Optional.empty();
    }

    /**
     *
     * @param relationshipType The {@link RelationType} to validate
     * @return An error message if the relationTypes does not have at least 1 role
     */
    static Optional<String> validateHasMinimumRoles(RelationType relationshipType) {
        if(relationshipType.isAbstract() || relationshipType.roles().iterator().hasNext()){
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_RELATION_TYPE.getMessage(relationshipType.label()));
        }
    }

    /**
     *
     * @param relationshipType the {@link RelationType} to be validated
     * @return Error messages if the role type sub structure does not match the {@link RelationType} sub structure
     */
    static Set<String> validateRelationTypesToRolesSchema(RelationType relationshipType){
        RelationshipTypeImpl superRelationType = (RelationshipTypeImpl) relationshipType.sup();
        if(Schema.MetaSchema.isMetaLabel(superRelationType.label()) || superRelationType.isAbstract()){ //If super type is a meta type no validation needed
            return Collections.emptySet();
        }

        Set<String> errorMessages = new HashSet<>();

        Collection<Role> superRelates = superRelationType.roles().collect(Collectors.toSet());
        Collection<Role> relates = relationshipType.roles().collect(Collectors.toSet());
        Set<Label> relatesLabels = relates.stream().map(SchemaConcept::label).collect(Collectors.toSet());

        //TODO: Determine if this check is redundant
        //Check 1) Every role of relationTypes is the sub of a role which is in the relates of it's supers
        if(!superRelationType.isAbstract()) {
            Set<Label> allSuperRolesPlayed = new HashSet<>();
            superRelationType.sups().forEach(rel -> rel.roles().forEach(roleType -> allSuperRolesPlayed.add(roleType.label())));

            for (Role relate : relates) {
                boolean validRoleTypeFound = SchemaConceptImpl.from(relate).sups().
                        anyMatch(superRole -> allSuperRolesPlayed.contains(superRole.label()));

                if(!validRoleTypeFound){
                    errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(relate.label(), relationshipType.label(), "super", "super", superRelationType.label()));
                }
            }
        }

        //Check 2) Every role of superRelationType has a sub role which is in the relates of relationTypes
        for (Role superRelate : superRelates) {
            boolean subRoleNotFoundInRelates = superRelate.subs().noneMatch(sub -> relatesLabels.contains(sub.label()));

            if(subRoleNotFoundInRelates){
                errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(superRelate.label(), superRelationType.label(), "sub", "sub", relationshipType.label()));
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
                    Stream<Relation> relationships = thing.relationships(role);

                    if(!CommonUtil.containsOnly(relationships, 1)){
                        Label resourceTypeLabel = Schema.ImplicitType.explicitLabel(role.label());
                        return Optional.of(VALIDATION_NOT_EXACTLY_ONE_KEY.getMessage(thing.id(), resourceTypeLabel));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.sup();
        }
        return Optional.empty();
    }

    /**
     *
     * @param graph
     * @param rules
     */
    static Set<String> validateRuleStratifiability(TransactionOLTP graph, Set<Rule> rules){
        Set<String> errors = new HashSet<>();
        if (!RuleUtils.subGraphIsStratifiable(rules, graph)){
            errors.add(ErrorMessage.VALIDATION_RULE_GRAPH_NOT_STRATIFIABLE.getMessage());
        }
        return errors;
    }


    /**
     * @param graph graph used to ensure the rule is a valid Horn clause
     * @param rule the rule to be validated
     * @return Error messages if the rule is not a valid Horn clause (in implication form, conjunction in the body, single-atom conjunction in the head)
     */
    static Set<String> validateRuleIsValidHornClause(TransactionOLTP graph, Rule rule){
        Set<String> errors = new HashSet<>();
        if (rule.when().getDisjunctiveNormalForm().getPatterns().size() > 1){
            errors.add(ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY.getMessage(rule.label()));
        }
        if (errors.isEmpty()){
            errors.addAll(validateRuleHead(graph, rule));
        }
        return errors;
    }

    /**
     * @param graph graph (tx) of interest
     * @param rule the rule to be cast into a combined conjunction query
     * @return a combined conjunction created from statements from both the body and the head of the rule
     */
    private static ReasonerQuery combinedRuleQuery(TransactionOLTP graph, Rule rule){
        ReasonerQuery bodyQuery = ReasonerQueries.create(Graql.and(rule.when().getDisjunctiveNormalForm().getPatterns().stream().flatMap(conj -> conj.getPatterns().stream()).collect(Collectors.toSet())), graph);
        ReasonerQuery headQuery = ReasonerQueries.create(Graql.and(rule.then().getDisjunctiveNormalForm().getPatterns().stream().flatMap(conj -> conj.getPatterns().stream()).collect(Collectors.toSet())), graph);
        return headQuery.conjunction(bodyQuery);
    }

    /**
     * NB: this only gets checked if the rule obeys the Horn clause form
     * @param graph graph used to ensure the rule is a valid Horn clause
     * @param rule the rule to be validated ontologically
     * @return Error messages if the rule has ontological inconsistencies
     */
    static Set<String> validateRuleOntologically(TransactionOLTP graph, Rule rule) {
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
    private static Set<String> validateRuleHead(TransactionOLTP graph, Rule rule) {
        Set<String> errors = new HashSet<>();
        Set<Conjunction<Statement>> headPatterns = rule.then().getDisjunctiveNormalForm().getPatterns();
        Set<Conjunction<Statement>> bodyPatterns = rule.when().getDisjunctiveNormalForm().getPatterns();

        if (headPatterns.size() != 1){
            errors.add(ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD.getMessage(rule.label()));
        } else {
            ReasonerQuery bodyQuery = ReasonerQueries.create(Iterables.getOnlyElement(bodyPatterns), graph);
            ReasonerQuery headQuery = ReasonerQueries.create(Iterables.getOnlyElement(headPatterns), graph);
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
                errors.add(ErrorMessage.VALIDATION_RULE_HEAD_NON_ATOMIC.getMessage(rule.label()));
            }
        }
        return errors;
    }

    /**
     *
     * @param rule The rule to be validated
     * @return Error messages if the when or then of a rule refers to a non existent type
     */
    static Set<String> validateRuleSchemaConceptExist(Transaction graph, Rule rule){
        Set<String> errors = new HashSet<>();
        errors.addAll(checkRuleSideInvalid(graph, rule, Schema.VertexProperty.RULE_WHEN, rule.when()));
        errors.addAll(checkRuleSideInvalid(graph, rule, Schema.VertexProperty.RULE_THEN, rule.then()));
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
    private static Set<String> checkRuleSideInvalid(Transaction graph, Rule rule, Schema.VertexProperty side, Pattern pattern) {
        Set<String> errors = new HashSet<>();

        pattern.statements().stream()
                .flatMap(statement -> statement.innerStatements().stream())
                .flatMap(statement -> statement.getTypes().stream()).forEach(type -> {
                    SchemaConcept schemaConcept = graph.getSchemaConcept(Label.of(type));
                    if(schemaConcept == null){
                        errors.add(ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(side, rule.label(), type));
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
                            throw TransactionException.invalidPropertyUse(rule, side);
                        }
                    }
                });

        return errors;
    }

    /**
     * Checks if a {@link Relation} has at least one role player.
     * @param relationship The {@link Relation} to check
     */
    static Optional<String> validateRelationshipHasRolePlayers(Relation relationship) {
        if(!relationship.rolePlayers().findAny().isPresent()){
            return Optional.of(ErrorMessage.VALIDATION_RELATIONSHIP_WITH_NO_ROLE_PLAYERS.getMessage(relationship.id(), relationship.type().label()));
        }
        return Optional.empty();
    }
}
