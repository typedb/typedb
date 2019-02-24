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

import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.concept.thing.Thing;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.session.TransactionOLTP;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Ensures each concept undergoes the correct type of validation.
 * Handles calling the relevant validation defined in ValidateGlobalRules depending on the
 * type of the concept.
 */
public class Validator {
    private final TransactionOLTP graknGraph;
    private final List<String> errorsFound = new ArrayList<>();

    public Validator(TransactionOLTP graknGraph) {
        this.graknGraph = graknGraph;
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
        graknGraph.cache().getModifiedThings().forEach(this::validateThing);

        //Validate Relationships
        graknGraph.cache().getNewRelationships().forEach(this::validateRelationship);

        //Validate RoleTypes
        graknGraph.cache().getModifiedRoles().forEach(this::validateRole);
        //Validate Role Players
        graknGraph.cache().getModifiedCastings().forEach(this::validateCasting);

        //Validate Relationship Types
        graknGraph.cache().getModifiedRelationshipTypes().forEach(this::validateRelationType);

        //Validate Rules
        graknGraph.cache().getModifiedRules().forEach(rule -> validateRule(graknGraph, rule));

        //Validate rule stratification
        errorsFound.addAll(
                ValidateGlobalRules.validateRuleStratifiability(graknGraph, graknGraph.cache().getModifiedRules())
        );

        return errorsFound.size() == 0;
    }

    /**
     * Validation rules exclusive to rules
     * the precedence of validation is: labelValidation -> ontologicalValidation -> clauseValidation
     * each of the validation happens only if the preceding validation yields no errors
     *
     * @param graph the graph to query against
     * @param rule  the rule which needs to be validated
     */
    private void validateRule(TransactionOLTP graph, Rule rule) {
        Set<String> labelErrors = ValidateGlobalRules.validateRuleSchemaConceptExist(graph, rule);
        errorsFound.addAll(labelErrors);
        if (labelErrors.isEmpty()) {
            Set<String> ontologicalErrors = ValidateGlobalRules.validateRuleOntologically(graph, rule);
            errorsFound.addAll(ontologicalErrors);
            if (ontologicalErrors.isEmpty()) {
                errorsFound.addAll(ValidateGlobalRules.validateRuleIsValidClause(graph, rule));
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
     * @param relationshipType The relationTypes to validate
     */
    private void validateRelationType(RelationType relationshipType) {
        ValidateGlobalRules.validateHasMinimumRoles(relationshipType).ifPresent(errorsFound::add);
        errorsFound.addAll(ValidateGlobalRules.validateRelationTypesToRolesSchema(relationshipType));
    }

    /**
     * Validation rules exclusive to instances
     *
     * @param thing The Thing to validate
     */
    private void validateThing(Thing thing) {
        ValidateGlobalRules.validateInstancePlaysAllRequiredRoles(thing).ifPresent(errorsFound::add);
    }

    /**
     * Validates that Relations can be committed.
     *
     * @param relationship The Relation to validate
     */
    private void validateRelationship(Relation relationship) {
        ValidateGlobalRules.validateRelationshipHasRolePlayers(relationship).ifPresent(errorsFound::add);
    }
}
