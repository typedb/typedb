/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.server;

import com.google.common.collect.Iterables;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.util.Streams;
import grakn.core.concept.impl.RelationTypeImpl;
import grakn.core.concept.impl.RuleImpl;
import grakn.core.concept.impl.SchemaConceptImpl;
import grakn.core.concept.impl.TypeImpl;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.query.CompositeQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.rule.RuleUtils;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.VALIDATION_CASTING;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_MORE_THAN_ONE_USE_OF_KEY;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_NOT_EXACTLY_ONE_KEY;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_RELATION_CASTING_LOOP_FAIL;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_RELATION_TYPE;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_RELATION_TYPES_ROLES_SCHEMA;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_REQUIRED_RELATION;
import static grakn.core.common.exception.ErrorMessage.VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE;

/**
 * Specific Validation Rules
 * This class contains the implementation for the following validation rules:
 * 1. Plays Validation which ensures that a Thing is allowed to play the Role
 * it has been assigned to.
 * 2. Relates Validation which ensures that every Role which is not abstract is
 * assigned to a RelationType via RelationType#relates(Role).
 * 3. Minimum Role Validation which ensures that every RelationType has at least 2 Role
 * assigned to it via RelationType#relates(Role).
 * 4. Relation Structure Validation which ensures that each Relation has the
 * correct structure.
 * 5. Abstract Type Validation which ensures that each abstract Type has no Thing.
 * 6. RelationType Hierarchy Validation which ensures that RelationType with a hierarchical structure
 * have a valid matching Role hierarchical structure.
 * 7. Required Resources validation which ensures that each Thing with required
 * Attribute has a valid Relation to that Attribute.
 * 8. Unique Relation Validation which ensures that no duplicate Relation are created.
 */
public class ValidateGlobalRules {
    private ValidateGlobalRules() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method checks if the plays edge has been added between the roleplayer's Type and
     * the Role being played.
         * It also checks if the Role of the Casting has been linked to the RelationType of the
     * Relation which the Casting connects to.
     *
     * @return Specific errors if any are found
     */
    public static Set<String> validatePlaysAndRelatesStructure(Casting casting) {
        Set<String> errors = new HashSet<>();

        //Gets here to make sure we traverse/read only once
        Thing thing = casting.getRolePlayer();
        Role role = casting.getRole();
        Relation relation = casting.getRelation();

        //Actual checks
        roleNotAllowedToBePlayed(role, thing).ifPresent(errors::add);
        roleNotLinkedToRelation(role, relation.type(), relation).ifPresent(errors::add);

        return errors;
    }

    /**
     * Checks if the Role of the Casting has been linked to the RelationType of
     * the Relation which the Casting connects to.
     *
     * @param role             the Role which the Casting refers to
     * @param relationType the RelationType which should connect to the role
     * @param relation     the Relation which the Casting refers to
     * @return an error if one is found
     */
    private static Optional<String> roleNotLinkedToRelation(Role role, RelationType relationType, Relation relation) {
        boolean notFound = role.relations().
                noneMatch(innerRelationType -> innerRelationType.label().equals(relationType.label()));
        if (notFound) {
            return Optional.of(VALIDATION_RELATION_CASTING_LOOP_FAIL.getMessage(relation.id(), role.label(), relationType.label()));
        }
        return Optional.empty();
    }

    /**
     * Checks  if the plays edge has been added between the roleplayer's Type and
     * the Role being played.
         * Also checks that required Role are satisfied
     *
     * @param role  The Role which the role-player is playing
     * @param thing the role-player
     * @return an error if one is found
     */
    private static Optional<String> roleNotAllowedToBePlayed(Role role, Thing thing) {
        TypeImpl<?, ?> currentConcept = (TypeImpl<?, ?>) thing.type();

        boolean satisfiesPlays = false;
        while (currentConcept != null) {
            Map<Role, Boolean> plays = currentConcept.directPlays();

            for (Map.Entry<Role, Boolean> playsEntry : plays.entrySet()) {
                Role rolePlayed = playsEntry.getKey();
                Boolean required = playsEntry.getValue();
                if (rolePlayed.label().equals(role.label())) {
                    satisfiesPlays = true;

                    // Assert unique relation for this role type
                    if (required && !Streams.containsOnly(thing.relations(role), 1)) {
                        return Optional.of(VALIDATION_REQUIRED_RELATION.getMessage(thing.id(), thing.type().label(), role.label(), thing.relations(role).count()));
                    }
                }
            }
            currentConcept = (TypeImpl) currentConcept.sup();
        }

        if (satisfiesPlays) {
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_CASTING.getMessage(thing.type().label(), thing.id(), role.label()));
        }
    }

    /**
     * @param role The Role to validate
     * @return An error message if the relates does not have a single incoming RELATES edge
     */
    public static Optional<String> validateHasSingleIncomingRelatesEdge(Role role) {
        if (!role.relations().findAny().isPresent()) {
            return Optional.of(VALIDATION_ROLE_TYPE_MISSING_RELATION_TYPE.getMessage(role.label()));
        }
        return Optional.empty();
    }

    /**
     * @param relationType The RelationType to validate
     * @return An error message if the relationTypes does not have at least 1 role
     */
    public static Optional<String> validateHasMinimumRoles(RelationType relationType) {
        if (relationType.isAbstract() || relationType.roles().iterator().hasNext()) {
            return Optional.empty();
        } else {
            return Optional.of(VALIDATION_RELATION_TYPE.getMessage(relationType.label()));
        }
    }

    /**
     * @param relationType the RelationType to be validated
     * @return Error messages if the role type sub structure does not match the RelationType sub structure
     */
    public static Set<String> validateRelationTypesToRolesSchema(RelationType relationType) {
        RelationTypeImpl superRelationType = (RelationTypeImpl) relationType.sup();
        if (Schema.MetaSchema.isMetaLabel(superRelationType.label()) || superRelationType.isAbstract()) { //If super type is a meta type no validation needed
            return Collections.emptySet();
        }

        Set<String> errorMessages = new HashSet<>();

        Collection<Role> superRelates = superRelationType.roles().collect(Collectors.toSet());
        Collection<Role> relates = relationType.roles().collect(Collectors.toSet());
        Set<Label> relatesLabels = relates.stream().map(SchemaConcept::label).collect(Collectors.toSet());

        //TODO: Determine if this check is redundant
        //Check 1) Every role of relationTypes is the sub of a role which is in the relates of it's supers
        if (!superRelationType.isAbstract()) {
            Set<Label> allSuperRolesPlayed = new HashSet<>();
            superRelationType.sups().forEach(rel -> rel.roles().forEach(roleType -> allSuperRolesPlayed.add(roleType.label())));

            for (Role relate : relates) {
                boolean validRoleTypeFound = SchemaConceptImpl.from(relate).sups().
                        anyMatch(superRole -> allSuperRolesPlayed.contains(superRole.label()));

                if (!validRoleTypeFound) {
                    errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(relate.label(), relationType.label(), "super", "super", superRelationType.label()));
                }
            }
        }

        //Check 2) Every role of superRelationType has a sub role which is in the relates of relationTypes
        for (Role superRelate : superRelates) {
            boolean subRoleNotFoundInRelates = superRelate.subs().noneMatch(sub -> relatesLabels.contains(sub.label()));

            if (subRoleNotFoundInRelates) {
                errorMessages.add(VALIDATION_RELATION_TYPES_ROLES_SCHEMA.getMessage(superRelate.label(), superRelationType.label(), "sub", "sub", relationType.label()));
            }
        }

        return errorMessages;
    }

    /**
     * @param thing The thing to be validated
     * @return An error message if the thing does not have all the required resources
     */
    public static Optional<String> validateInstancePlaysAllRequiredRoles(ConceptManager conceptManager, Thing thing) {
        TypeImpl<?, ?> type = (TypeImpl) thing.type();

        while (type != null) {
            Map<Role, Boolean> rolesAreRequired = type.directPlays();
            for (Map.Entry<Role, Boolean> roleIsRequired : rolesAreRequired.entrySet()) {
                if (roleIsRequired.getValue()) {
                    Role role = roleIsRequired.getKey();
                    Label attributeLabel = Schema.ImplicitType.explicitLabel(role.label());

                    // Assert there is a relation for this type
                    if (!Streams.containsOnly(thing.relations(role), 1)) {
                        return Optional.of(VALIDATION_NOT_EXACTLY_ONE_KEY.getMessage(thing.id(), thing.type().label().getValue(), attributeLabel));
                    }

                    Optional<String> uniquenessViolation = validateKeyUniqueness(conceptManager, thing, role, type, attributeLabel);
                    if (uniquenessViolation.isPresent()) return uniquenessViolation;
                }
            }
            type = (TypeImpl) type.sup();
        }
        return Optional.empty();
    }

    public static Optional<String> validateKeyUniqueness(ConceptManager conceptManager, Thing thing, Role role, Type ownerType, Label attributeLabel){
        //validate key uniqueness
        Relation keyRelation = thing.relations(role).findFirst().get();
        final Role keyValueRole = conceptManager.getRole(Schema.ImplicitType.KEY_VALUE.getLabel(attributeLabel).getValue());
        final Attribute<?> keyValue = keyRelation.rolePlayers(keyValueRole).findFirst().get().asAttribute();
        if (keyValue.owners().filter(owner -> owner.type().sups().anyMatch(t -> t.equals(ownerType))).limit(2).count() > 1) {
            Label resourceTypeLabel = Schema.ImplicitType.explicitLabel(role.label());
            return Optional.of(VALIDATION_MORE_THAN_ONE_USE_OF_KEY.getMessage(ownerType.label(), keyValue.value(), resourceTypeLabel));
        }
        return Optional.empty();
    }

    /**
     * @return Error messages if the rules in the db are not stratifiable (cycles with negation are present)
     */
    public static Set<String> validateRuleStratifiability(ConceptManager conceptManager){
        Set<String> errors = new HashSet<>();
        List<Set<Type>> negativeCycles = RuleUtils.negativeCycles(conceptManager);
        if (!negativeCycles.isEmpty()){
            errors.add(ErrorMessage.VALIDATION_RULE_GRAPH_NOT_STRATIFIABLE.getMessage(negativeCycles));
        }
        return errors;
    }

    /**
     * @param rule the rule to be validated
     * @return Error messages if the rule is not a valid clause (in implication form, conjunction in the body, single-atom conjunction in the head)
     */
    public static Set<String> validateRuleIsValidClause(ReasonerQueryFactory reasonerQueryFactory, Rule rule) {
        Set<String> errors = new HashSet<>();
        Set<Conjunction<Pattern>> patterns = rule.when().getNegationDNF().getPatterns();
        if (patterns.size() > 1) {
            errors.add(ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY.getMessage(rule.label()));
        } else {
            errors.addAll(CompositeQuery.validateAsRuleBody(Iterables.getOnlyElement(patterns), rule, reasonerQueryFactory));
        }

        if (errors.isEmpty()) {
            errors.addAll(validateRuleHead(reasonerQueryFactory, rule));
        }
        return errors;
    }

    /**
     * @param rule  the rule to be cast into a combined conjunction query
     * @return a combined conjunction created from statements from both the body and the head of the rule
     */
    private static ReasonerQuery combinedRuleQuery(ReasonerQueryFactory reasonerQueryFactory, Rule rule) {
        ReasonerQuery bodyQuery = reasonerQueryFactory.create(Graql.and(rule.when().getDisjunctiveNormalForm().getPatterns().stream().flatMap(conj -> conj.getPatterns().stream()).collect(Collectors.toSet())));
        ReasonerQuery headQuery = reasonerQueryFactory.create(Graql.and(rule.then().getDisjunctiveNormalForm().getPatterns().stream().flatMap(conj -> conj.getPatterns().stream()).collect(Collectors.toSet())));
        return headQuery.conjunction(bodyQuery);
    }

    /**
     * NB: this only gets checked if the rule obeys the Horn clause form
     *
     * @param rule  the rule to be validated ontologically
     * @return Error messages if the rule has ontological inconsistencies
     */
    public static Set<String> validateRuleOntologically(ReasonerQueryFactory reasonerQueryFactory, Rule rule) {
        Set<String> errors = new HashSet<>();

        //both body and head refer to the same graph and have to be valid with respect to the schema that governs it
        //as a result the rule can be ontologically validated by combining them into a conjunction
        //this additionally allows to cross check body-head references
        ReasonerQuery combinedQuery = combinedRuleQuery(reasonerQueryFactory, rule);
        errors.addAll(combinedQuery.validateOntologically(rule.label()));
        return errors;
    }

    /**
     * @param rule  the rule to be validated
     * @return Error messages if the rule head is invalid - is not a single-atom conjunction, doesn't contain illegal atomics and is ontologically valid
     */
    private static Set<String> validateRuleHead(ReasonerQueryFactory reasonerQueryFactory, Rule rule) {
        Set<String> errors = new HashSet<>();
        Set<Conjunction<Statement>> headPatterns = rule.then().getDisjunctiveNormalForm().getPatterns();
        Set<Conjunction<Statement>> bodyPatterns = rule.when().getDisjunctiveNormalForm().getPatterns();

        if (headPatterns.size() != 1) {
            errors.add(ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_HEAD.getMessage(rule.label()));
        } else {
            ReasonerQuery bodyQuery = reasonerQueryFactory.create(Iterables.getOnlyElement(bodyPatterns));
            ReasonerQuery headQuery = reasonerQueryFactory.create(Iterables.getOnlyElement(headPatterns));
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
     * @param rule The rule to be validated
     * @return Error messages if the when or then of a rule refers to a non existent type
     */
    public static Set<String> validateRuleSchemaConceptExist(ConceptManager conceptManager, Rule rule) {
        Set<String> errors = new HashSet<>();
        errors.addAll(checkRuleSideInvalid(conceptManager, rule, Schema.VertexProperty.RULE_WHEN, rule.when()));
        errors.addAll(checkRuleSideInvalid(conceptManager, rule, Schema.VertexProperty.RULE_THEN, rule.then()));
        return errors;
    }

    /**
     * @param rule    The rule the pattern was extracted from
     * @param side    The side from which the pattern was extracted
     * @param pattern The pattern from which we will extract the types in the pattern
     * @return A list of errors if the pattern refers to any non-existent types in the graph
     */
    private static Set<String> checkRuleSideInvalid(ConceptManager conceptManager, Rule rule, Schema.VertexProperty side, Pattern pattern) {
        Set<String> errors = new HashSet<>();

        pattern.getNegationDNF().getPatterns().stream()
                .flatMap(conj -> conj.getPatterns().stream())
                .forEach(p -> p.statements().stream()
                        .flatMap(statement -> statement.innerStatements().stream())
                        .flatMap(statement -> statement.getTypes().stream())
                        .forEach(type -> {
                            SchemaConcept schemaConcept = conceptManager.getSchemaConcept(Label.of(type));
                            if(schemaConcept == null){
                                errors.add(ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(side, rule.label(), type));
                            } else {
                                if(Schema.VertexProperty.RULE_WHEN.equals(side)){
                                    if (schemaConcept.isType()){
                                        if (p.isNegation()){
                                            RuleImpl.from(rule).addNegativeHypothesis(schemaConcept.asType());
                                        } else {
                                            RuleImpl.from(rule).addPositiveHypothesis(schemaConcept.asType());
                                        }
                                    }
                                } else if (Schema.VertexProperty.RULE_THEN.equals(side)){
                                    if (schemaConcept.isType()) {
                                        RuleImpl.from(rule).addConclusion(schemaConcept.asType());
                                    }
                                } else {
                                    throw GraknConceptException.invalidPropertyUse(rule, side.toString());
                                }
                            }
                        }));
        return errors;
    }

    /**
     * Checks if a Relation has at least one role player.
     *
     * @param relation The Relation to check
     */
    public static Optional<String> validateRelationHasRolePlayers(Relation relation) {
        if (!relation.rolePlayers().findAny().isPresent()) {
            return Optional.of(ErrorMessage.VALIDATION_RELATION_WITH_NO_ROLE_PLAYERS.getMessage(relation.id(), relation.type().label()));
        }
        return Optional.empty();
    }
}
