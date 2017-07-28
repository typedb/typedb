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

import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Thing;
import ai.grakn.graph.internal.concept.RelationImpl;
import ai.grakn.graph.internal.concept.RelationReified;
import ai.grakn.graph.internal.structure.Casting;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * <p>
 *     Ensures each concept undergoes the correct type of validation.
 * </p>
 *
 * <p>
 *      Handles calling the relevant validation defined in {@link ValidateGlobalRules} depending on the
 *      type of the concept.
 * </p>
 *
 * @author fppt
 *
 */
class Validator {
    private final AbstractGraknGraph<?> graknGraph;
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
        //Validate Entity Types
        //Not Needed
        //Validate Entities
        graknGraph.txCache().getModifiedEntities().forEach(this::validateThing);

        //Validate RoleTypes
        graknGraph.txCache().getModifiedRoles().forEach(this::validateRole);
        //Validate Role Players
        graknGraph.txCache().getModifiedCastings().forEach(this::validateCasting);

        //Validate Relation Types
        graknGraph.txCache().getModifiedRelationTypes().forEach(this::validateRelationType);
        //Validate Relations
        graknGraph.txCache().getModifiedRelations().forEach(relation -> validateRelation(graknGraph, relation));

        //Validate Rule Types
        //Not Needed
        //Validate Rules
        graknGraph.txCache().getModifiedRules().forEach(rule -> validateRule(graknGraph, rule));

        //Validate Resource Types
        //Not Needed
        //Validate Resource
        graknGraph.txCache().getModifiedResources().forEach(this::validateThing);

        return errorsFound.size() == 0;
    }

    /**
     * Validation rules exclusive to rules
     * @param graph the graph to query against
     * @param rule the rule which needs to be validated
     */
    private void validateRule(AbstractGraknGraph<?> graph, Rule rule){
        Set<String> labelErrors = ValidateGlobalRules.validateRuleOntologyElementsExist(graph, rule);
        errorsFound.addAll(labelErrors);
        errorsFound.addAll(ValidateGlobalRules.validateRuleIsValidHornClause(graph, rule));
        if (labelErrors.isEmpty()){
            errorsFound.addAll(ValidateGlobalRules.validateRuleOntologically(graph, rule));
        }
    }

    /**
     * Validation rules exclusive to relations
     * @param relation The relation to validate
     */
    private void validateRelation(AbstractGraknGraph<?> graph, Relation relation){
        validateThing(relation);
        Optional<RelationReified> relationReified = ((RelationImpl) relation).reified();
        //TODO: We need new validation mechanisms for non-reified relations
        relationReified.ifPresent(relationReified1 -> {
            ValidateGlobalRules.validateRelationshipStructure(relationReified1).ifPresent(errorsFound::add);
            ValidateGlobalRules.validateRelationIsUnique(graph, relationReified1).ifPresent(errorsFound::add);
        });
    }

    /**
     * Validation rules exclusive to role players
     * @param casting The Role player to validate
     */
    private void validateCasting(Casting casting){
        ValidateGlobalRules.validatePlaysStructure(casting).ifPresent(errorsFound::add);
    }

    /**
     * Validation rules exclusive to role
     * @param role The {@link Role} to validate
     */
    private void validateRole(Role role){
        ValidateGlobalRules.validateHasSingleIncomingRelatesEdge(role).ifPresent(errorsFound::add);
    }

    /**
     * Validation rules exclusive to relation types
     * @param relationType The relationTypes to validate
     */
    private void validateRelationType(RelationType relationType){
        ValidateGlobalRules.validateHasMinimumRoles(relationType).ifPresent(errorsFound::add);
        errorsFound.addAll(ValidateGlobalRules.validateRelationTypesToRolesSchema(relationType));
    }

    /**
     * Validation rules exclusive to instances
     * @param thing The {@link Thing} to validate
     */
    private void validateThing(Thing thing) {
        ValidateGlobalRules.validateInstancePlaysAllRequiredRoles(thing).ifPresent(errorsFound::add);
    }
}
