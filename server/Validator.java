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

import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.server.cache.TransactionCache;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Ensures each concept undergoes the correct type of validation.
 * Handles calling the relevant validation defined in ValidateGlobalRules depending on the
 * type of the concept.
 */
public class Validator {
    private final ReasonerQueryFactory reasonerQueryFactory;
    private TransactionCache transactionCache;
    private ConceptManager conceptManager;
    private final List<String> errorsFound = new ArrayList<>();

    public Validator(ReasonerQueryFactory reasonerQueryFactory, TransactionCache transactionCache, ConceptManager conceptManager) {
        this.reasonerQueryFactory = reasonerQueryFactory;
        this.transactionCache = transactionCache;
        this.conceptManager = conceptManager;
    }

    /**
     * @return Any errors found during validation
     */
    public List<String> getErrorsFound() {
        return errorsFound;
    }

    /**
     * @return True if the data and schema conforms to our concept.
     */
    public boolean validate() {
        //Validate Things
        for (Thing thing : transactionCache.getModifiedThings()) {
            validateThing(thing);
        }

        //Validate Relations
        transactionCache.getNewRelations().forEach(this::validateRelation);

        //Validate RoleTypes
        transactionCache.getModifiedRoles().forEach(this::validateRole);
        //Validate Role Players
        transactionCache.getModifiedCastings().forEach(this::validateCasting);

        //Validate Relation Types
        transactionCache.getModifiedRelationTypes().forEach(this::validateRelationType);

        //Validate Rules
        transactionCache.getModifiedRules().forEach(rule -> validateRule(rule));

        //Validate rule type graph
        if (!transactionCache.getModifiedRules().isEmpty()) {
            errorsFound.addAll(ValidateGlobalRules.validateRuleStratifiability(conceptManager));
        }

        return errorsFound.size() == 0;
    }

    /**
     * Validation rules exclusive to rules
     * the precedence of validation is: labelValidation -> ontologicalValidation -> clauseValidation
     * each of the validation happens only if the preceding validation yields no errors
     *
     * @param rule  the rule which needs to be validated
     */
    private void validateRule(Rule rule) {
        Set<String> labelErrors = ValidateGlobalRules.validateRuleSchemaConceptExist(conceptManager, rule);
        errorsFound.addAll(labelErrors);
        if (labelErrors.isEmpty()) {
            Set<String> ontologicalErrors = ValidateGlobalRules.validateRuleOntologically(reasonerQueryFactory, rule);
            errorsFound.addAll(ontologicalErrors);
            if (ontologicalErrors.isEmpty()) {
                errorsFound.addAll(ValidateGlobalRules.validateRuleIsValidClause(reasonerQueryFactory, rule));
            }
        }
    }

    /**
     * Validation rules exclusive to role players
     *
     * @param casting The Role player to validate
     */
    private void validateCasting(Casting casting) {
        errorsFound.addAll(ValidateGlobalRules.validatePlaysAndRelatesStructure(casting));
    }

    /**
     * Validation rules exclusive to role
     *
     * @param role The Role to validate
     */
    private void validateRole(Role role) {
        ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).ifPresent(errorsFound::add);
    }

    /**
     * Validation rules exclusive to relation types
     *
     * @param relationType The relationTypes to validate
     */
    private void validateRelationType(RelationType relationType) {
        ValidateGlobalRules.validateHasMinimumRoles(relationType).ifPresent(errorsFound::add);
        errorsFound.addAll(ValidateGlobalRules.validateRelationTypesToRolesSchema(relationType));
    }

    /**
     * Validation rules exclusive to instances
     *
     * @param thing The Thing to validate
     */
    private void validateThing(Thing thing) {
        ValidateGlobalRules.validateInstancePlaysAllRequiredRoles(conceptManager, thing).ifPresent(errorsFound::add);
    }

    /**
     * Validates that Relations can be committed.
     *
     * @param relation The Relation to validate
     */
    private void validateRelation(Relation relation) {
        ValidateGlobalRules.validateRelationHasRolePlayers(relation).ifPresent(errorsFound::add);
    }
}
