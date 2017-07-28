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

package ai.grakn.exception;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.macro.Macro;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;

import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;

import static ai.grakn.util.ErrorMessage.INSERT_ABSTRACT_NOT_TYPE;
import static ai.grakn.util.ErrorMessage.INSERT_INSTANCE_WITH_NAME;
import static ai.grakn.util.ErrorMessage.INSERT_ISA_AND_SUB;
import static ai.grakn.util.ErrorMessage.INSERT_MULTIPLE_VALUES;
import static ai.grakn.util.ErrorMessage.INSERT_NEW_TYPE;
import static ai.grakn.util.ErrorMessage.INSERT_NO_DATATYPE;
import static ai.grakn.util.ErrorMessage.INSERT_RECURSIVE;
import static ai.grakn.util.ErrorMessage.INSERT_RESOURCE_WITHOUT_VALUE;
import static ai.grakn.util.ErrorMessage.INSERT_RULE_WITHOUT_THEN;
import static ai.grakn.util.ErrorMessage.INSERT_RULE_WITHOUT_WHEN;
import static ai.grakn.util.ErrorMessage.INSERT_TYPE_WITHOUT_LABEL;
import static ai.grakn.util.ErrorMessage.INSERT_UNDEFINED_VARIABLE;
import static ai.grakn.util.ErrorMessage.INSERT_UNSUPPORTED_PROPERTY;
import static ai.grakn.util.ErrorMessage.INSERT_WITHOUT_TYPE;
import static ai.grakn.util.ErrorMessage.INVALID_VALUE;
import static ai.grakn.util.ErrorMessage.NEGATIVE_OFFSET;
import static ai.grakn.util.ErrorMessage.NON_POSITIVE_LIMIT;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;

/**
 * <p>
 *     Graql Query Exception
 * </p>
 *
 * <p>
 *     Occurs when the query is syntactically correct but semantically incorrect.
 *     For example limiting the results of a query -1
 * </p>
 *
 * @author fppt
 */
public class GraqlQueryException extends GraknException{

    private GraqlQueryException(String error) {
        super(error);
    }

    private GraqlQueryException(String error, Exception cause) {
        super(error, cause);
    }

    public static GraqlQueryException noPatterns() {
        return new GraqlQueryException(ErrorMessage.NO_PATTERNS.getMessage());
    }

    public static GraqlQueryException incorrectAggregateArgumentNumber(
            String name, int minArgs, int maxArgs, List<Object> args) {
        String expectedArgs = (minArgs == maxArgs) ? Integer.toString(minArgs) : minArgs + "-" + maxArgs;
        String message = ErrorMessage.AGGREGATE_ARGUMENT_NUM.getMessage(name, expectedArgs, args.size());
        return new GraqlQueryException(message);
    }

    public static GraqlQueryException conflictingProperties(
            VarPatternAdmin varPattern, UniqueVarProperty property, UniqueVarProperty other) {
        String message = ErrorMessage.CONFLICTING_PROPERTIES.getMessage(
                varPattern.getPrintableName(), property.graqlString(), other.graqlString()
        );
        return new GraqlQueryException(message);
    }

    public static GraqlQueryException labelNotFound(Label label) {
        return new GraqlQueryException(ErrorMessage.LABEL_NOT_FOUND.getMessage(label));
    }

    public static GraqlQueryException failDelete(VarProperty property) {
        StringBuilder builder = new StringBuilder();
        property.buildString(builder);
        return new GraqlQueryException(ErrorMessage.DELETE_UNSUPPORTED_PROPERTY.getMessage(builder.toString()));
    }

    public static GraqlQueryException mustBeResourceType(Label resourceType) {
        return new GraqlQueryException(ErrorMessage.MUST_BE_RESOURCE_TYPE.getMessage(resourceType));
    }

    public static GraqlQueryException queryInstanceOfRoleType(Label label) {
        return new GraqlQueryException(ErrorMessage.INSTANCE_OF_ROLE_TYPE.getMessage(label));
    }

    public static GraqlQueryException notARelationType(Label label) {
        return new GraqlQueryException(ErrorMessage.NOT_A_RELATION_TYPE.getMessage(label));
    }

    public static GraqlQueryException notARoleType(Label roleId) {
        return new GraqlQueryException(ErrorMessage.NOT_A_ROLE_TYPE.getMessage(roleId, roleId));
    }

    public static GraqlQueryException insertRelationWithoutType() {
        return new GraqlQueryException(ErrorMessage.INSERT_RELATION_WITHOUT_ISA.getMessage());
    }

    public static GraqlQueryException insertUnsupportedProperty(String name, Schema.MetaSchema metaSchema) {
        return new GraqlQueryException(INSERT_UNSUPPORTED_PROPERTY.getMessage(name, metaSchema.getLabel()));
    }

    public static GraqlQueryException insertPredicate() {
        return new GraqlQueryException(ErrorMessage.INSERT_PREDICATE.getMessage());
    }

    public static GraqlQueryException insertIsaAndSub(String printableName) {
        return new GraqlQueryException(INSERT_ISA_AND_SUB.getMessage(printableName));
    }

    public static GraqlQueryException insertRecursive(VarPatternAdmin var) {
        return new GraqlQueryException(INSERT_RECURSIVE.getMessage(var.getPrintableName()));
    }

    public static GraqlQueryException insertInstanceWithLabel(Label label) {
        return new GraqlQueryException(INSERT_INSTANCE_WITH_NAME.getMessage(label));
    }

    public static GraqlQueryException insertWithoutType(ConceptId conceptId) {
        return new GraqlQueryException(INSERT_WITHOUT_TYPE.getMessage(conceptId));
    }

    public static GraqlQueryException insertUndefinedVariable(VarPatternAdmin var) {
        return new GraqlQueryException(INSERT_UNDEFINED_VARIABLE.getMessage(var.getPrintableName()));
    }

    public static GraqlQueryException createInstanceOfMetaConcept(Var var, Type type) {
        return new GraqlQueryException(var + " cannot be an instance of meta-type " + type.getLabel());
    }

    public static GraqlQueryException insertMetaType(Label label, OntologyConcept ontologyConcept) {
        return new GraqlQueryException(ErrorMessage.INSERT_METATYPE.getMessage(label, ontologyConcept.getLabel()));
    }

    public static GraqlQueryException insertMultipleValues(Object value1, Object value2) {
        return new GraqlQueryException(INSERT_MULTIPLE_VALUES.getMessage(value1, value2));
    }

    public static GraqlQueryException insertResourceWithoutValue() {
        return new GraqlQueryException(INSERT_RESOURCE_WITHOUT_VALUE.getMessage());
    }

    public static GraqlQueryException varNotInQuery(Var var) {
        return new GraqlQueryException(VARIABLE_NOT_IN_QUERY.getMessage(var));
    }

    public static GraqlQueryException noGraph() {
        return new GraqlQueryException(ErrorMessage.NO_GRAPH.getMessage());
    }

    public static GraqlQueryException multipleGraphs() {
        return new GraqlQueryException(ErrorMessage.MULTIPLE_GRAPH.getMessage());
    }

    public static GraqlQueryException nonPositiveLimit(long limit) {
        return new GraqlQueryException(NON_POSITIVE_LIMIT.getMessage(limit));
    }

    public static GraqlQueryException negativeOffset(long offset) {
        return new GraqlQueryException(NEGATIVE_OFFSET.getMessage(offset));
    }

    public static GraqlQueryException noSelectedVars() {
        return new GraqlQueryException(ErrorMessage.SELECT_NONE_SELECTED.getMessage());
    }

    public static GraqlQueryException invalidValueClass(Object value) {
        return new GraqlQueryException(INVALID_VALUE.getMessage(value.getClass()));
    }

    public static GraqlQueryException wrongNumberOfMacroArguments(Macro macro, List<Object> values) {
        return new GraqlQueryException("Wrong number of arguments [" + values.size() + "] to macro " + macro.name());
    }

    public static GraqlQueryException wrongMacroArgumentType(Macro macro, String expectedType, String value) {
        return new GraqlQueryException("Value [" + value + "] is not " + expectedType + " in macro " + macro.name());
    }

    public static GraqlQueryException unknownAggregate(String name) {
        return new GraqlQueryException(ErrorMessage.UNKNOWN_AGGREGATE.getMessage(name));
    }

    public static GraqlQueryException maxIterationsReached(Class<?> clazz) {
        return new GraqlQueryException(ErrorMessage.MAX_ITERATION_REACHED
                .getMessage(clazz.toString()));
    }

    public static GraqlQueryException statisticsResourceTypesNotSpecified() {
        return new GraqlQueryException(ErrorMessage.RESOURCE_TYPE_NOT_SPECIFIED.getMessage());
    }

    public static GraqlQueryException noPathDestination() {
        return new GraqlQueryException(ErrorMessage.NO_DESTINATION.getMessage());
    }

    public static GraqlQueryException noPathSource() {
        return new GraqlQueryException(ErrorMessage.NO_SOURCE.getMessage());
    }

    public static GraqlQueryException instanceDoesNotExist() {
        return new GraqlQueryException(ErrorMessage.INSTANCE_DOES_NOT_EXIST.getMessage());
    }

    public static GraqlQueryException resourceMustBeANumber(ResourceType.DataType dataType, Label resourceType) {
        return new GraqlQueryException(resourceType + " must have data type of `long` or `double`, but was " + dataType.getName());
    }

    public static GraqlQueryException resourcesWithDifferentDataTypes(Set<Label> resourceTypes) {
        return new GraqlQueryException("resource types " + resourceTypes + " have different data types");
    }

    public static GraqlQueryException unificationAtomIncompatibility() {
        return new GraqlQueryException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());
    }

    public static GraqlQueryException nonAtomicQuery(ReasonerQuery reasonerQuery) {
        return new GraqlQueryException(ErrorMessage.NON_ATOMIC_QUERY.getMessage(reasonerQuery));
    }

    public static GraqlQueryException ruleCreationArityMismatch() {
        return new GraqlQueryException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());
    }

    public static GraqlQueryException valuePredicateAtomWithMultiplePredicates() {
        return new GraqlQueryException("Attempting creation of ValuePredicate atom with more than single predicate");
    }

    public static GraqlQueryException getUnifierOfNonAtomicQuery() {
        return new GraqlQueryException("Attempted to obtain unifiers on non-atomic queries.");
    }

    public static GraqlQueryException noAtomsSelected(ReasonerQuery reasonerQuery) {
        return new GraqlQueryException(ErrorMessage.NO_ATOMS_SELECTED.getMessage(reasonerQuery.toString()));
    }

    public static GraqlQueryException cannotParseDateFormat(String originalFormat) {
        return new GraqlQueryException("Cannot parse date format " + originalFormat + ". See DateTimeFormatter#ofPattern");
    }

    public static GraqlQueryException cannotParseDateString(String originalDate, String originalFormat, DateTimeParseException cause) {
        throw new GraqlQueryException("Cannot parse date value " + originalDate + " with format " + originalFormat, cause);
    }

    public static GraqlQueryException noLabelSpecifiedForHas() {
        return new GraqlQueryException(ErrorMessage.NO_LABEL_SPECIFIED_FOR_HAS.getMessage());
    }

    public static GraqlQueryException insertRolePlayerWithoutRoleType() {
        return new GraqlQueryException(ErrorMessage.INSERT_RELATION_WITHOUT_ROLE_TYPE.getMessage());
    }

    public static GraqlQueryException insertRuleWithoutLhs(VarPatternAdmin var) {
        return new GraqlQueryException(INSERT_RULE_WITHOUT_WHEN.getMessage(var));
    }

    public static GraqlQueryException insertRuleWithoutRhs(VarPatternAdmin var) {
        return new GraqlQueryException(INSERT_RULE_WITHOUT_THEN.getMessage(var));
    }

    public static GraqlQueryException insertTypeWithoutLabel() {
        return new GraqlQueryException(INSERT_TYPE_WITHOUT_LABEL.getMessage());
    }

    public static GraqlQueryException insertAbstractOnNonType(OntologyConcept concept){
        return new GraqlQueryException(INSERT_ABSTRACT_NOT_TYPE.getMessage(concept.getLabel()));
    }

    public static GraqlQueryException insertResourceTypeWithoutDataType(VarPatternAdmin var) {
        return new GraqlQueryException(INSERT_NO_DATATYPE.getMessage(var.getPrintableName()));
    }

    public static GraqlQueryException insertNewType(Thing thing, Type type) {
        return new GraqlQueryException(INSERT_NEW_TYPE.getMessage(thing, type));
    }
}
