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

package grakn.core.kb.graql.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.INSERT_ABSTRACT_NOT_TYPE;
import static grakn.core.common.exception.ErrorMessage.INSERT_RECURSIVE;
import static grakn.core.common.exception.ErrorMessage.INSERT_UNDEFINED_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.INVALID_VALUE;
import static grakn.core.common.exception.ErrorMessage.NEGATIVE_OFFSET;
import static grakn.core.common.exception.ErrorMessage.NON_POSITIVE_LIMIT;
import static grakn.core.common.exception.ErrorMessage.UNEXPECTED_RESULT;

/**
 * Graql Semantic Exception
 * Occurs when the query is syntactically correct but semantically incorrect.
 * For example limiting the results of a query -1
 */
public class GraqlSemanticException extends GraknException {

    private final String NAME = "GraqlSemanticException";

    private GraqlSemanticException(String error) {
        super(error, null, false, false);
    }

    private GraqlSemanticException(String error, Exception cause) {
        super(error, cause, false, false);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static GraqlSemanticException create(String formatString, Object... args) {
        return new GraqlSemanticException(String.format(formatString, args));
    }

    public static GraqlSemanticException labelNotFound(Label label) {
        return new GraqlSemanticException(ErrorMessage.LABEL_NOT_FOUND.getMessage(label));
    }

    public static GraqlSemanticException attributeWithNonAttributeType(Label attributeType) {
        return new GraqlSemanticException(ErrorMessage.MUST_BE_ATTRIBUTE_TYPE.getMessage(attributeType));
    }

    public static GraqlSemanticException relationWithNonRelationType(Label label) {
        return new GraqlSemanticException(ErrorMessage.NOT_A_RELATION_TYPE.getMessage(label));
    }

    public static GraqlSemanticException invalidRoleLabel(Label label) {
        return new GraqlSemanticException(ErrorMessage.NOT_A_ROLE_TYPE.getMessage(label, label));
    }

    public static GraqlSemanticException matchWithoutAnyProperties(Statement statement) {
        return create("Require statement to have at least one property: `%s`", statement);
    }

    public static GraqlSemanticException unboundComparisonVariables(Set<Variable> unboundVariables) {
        return GraqlSemanticException.create("Variables used in comparisons cannot be unbounded %s", unboundVariables.toString());
    }

    public static GraqlSemanticException kCoreOnRelationType(Label label) {
        return create("cannot compute coreness of relation type %s.", label.getValue());
    }

    public static GraqlSemanticException deleteSchemaConcept(SchemaConcept schemaConcept) {
        return create("cannot delete schema concept %s. Use `undefine` instead.", schemaConcept);
    }

    public static GraqlSemanticException insertUnsupportedProperty(String propertyName) {
        return GraqlSemanticException.create("inserting property '%s' is not supported, try `define`", propertyName);
    }

    public static GraqlSemanticException defineUnsupportedProperty(String propertyName) {
        return GraqlSemanticException.create("defining property '%s' is not supported, try `insert`", propertyName);
    }

    public static GraqlSemanticException mustBeAttributeType(Label attributeType) {
        return new GraqlSemanticException(ErrorMessage.MUST_BE_ATTRIBUTE_TYPE.getMessage(attributeType));
    }

    public static GraqlSemanticException conceptDowncast(Type existingType, Type newType) {
        return create("Downcasting concepts from type `%s` to type `%s` is not allowed", existingType, newType);
    }

    public static GraqlSemanticException cannotGetInstancesOfNonType(Label label) {
        return GraqlSemanticException.create("%s is not a type and so does not have instances", label);
    }

    public static GraqlSemanticException insertPredicate() {
        return new GraqlSemanticException(ErrorMessage.INSERT_PREDICATE.getMessage());
    }

    public static GraqlSemanticException insertRecursive(Statement var) {
        return new GraqlSemanticException(INSERT_RECURSIVE.getMessage(var.getPrintableName()));
    }

    public static GraqlSemanticException insertUndefinedVariable(Statement var) {
        return new GraqlSemanticException(INSERT_UNDEFINED_VARIABLE.getMessage(var.getPrintableName()));
    }

    public static GraqlSemanticException createInstanceOfMetaConcept(Variable var, Type type) {
        return new GraqlSemanticException(var + " cannot be an instance of meta-type " + type.label());
    }

    /**
     * Thrown when a concept is inserted with multiple properties when it can only have one.
     * <p>
     * For example: {@code insert $x isa movie; $x isa person;}
     * </p>
     */
    public static GraqlSemanticException insertMultipleProperties(
            Statement varPattern, String property, Object value1, Object value2
    ) {
        String message = "A concept `%s` cannot have multiple properties `%s` and `%s` for `%s`";
        return create(message, varPattern, value1, value2, property);
    }

    /**
     * Thrown when a property is inserted on a concept that already exists and that property can't be overridden.
     * <p>
     * For example: {@code match $x isa name; insert $x val "Bob";}
     * </p>
     */
    public static GraqlSemanticException insertPropertyOnExistingConcept(String property, Object value, Concept concept) {
        return create("Cannot insert property `%s %s` on existing concept `%s`", property, value, concept);
    }

    /**
     * Thrown when a property is inserted on a concept that doesn't support that property.
     * <p>
     * For example, an entity with a value: {@code insert $x isa movie, val "The Godfather";}
     * </p>
     */
    public static GraqlSemanticException insertUnexpectedProperty(String property, Object value, Concept concept) {
        return create("Unexpected property `%s %s` for concept `%s`", property, value, concept);
    }

    /**
     * Thrown when a concept does not have all expected properties required to insert it.
     * <p>
     * For example, an attribute without a value: {@code insert $x isa name;}
     * </p>
     */
    public static GraqlSemanticException insertNoExpectedProperty(String property, Statement var) {
        return create("Missing an expected property `%s` in `%s`", property, var);
    }


    /**
     * Thrown when attempting to insert a concept that already exists.
     * <p>
     * For example: {@code match $x isa movie; insert $x isa name, val "Bob";}
     * </p>
     */
    public static GraqlSemanticException insertExistingConcept(Statement pattern, Concept concept) {
        return create("Cannot overwrite properties `%s` on  concept `%s`", pattern, concept);
    }

    public static GraqlSemanticException nonPositiveLimit(long limit) {
        return new GraqlSemanticException(NON_POSITIVE_LIMIT.getMessage(limit));
    }

    public static GraqlSemanticException negativeOffset(long offset) {
        return new GraqlSemanticException(NEGATIVE_OFFSET.getMessage(offset));
    }

    public static GraqlSemanticException invalidValueClass(Object value) {
        return new GraqlSemanticException(INVALID_VALUE.getMessage(value.getClass()));
    }

    public static GraqlSemanticException unknownAggregate(String name) {
        return new GraqlSemanticException(ErrorMessage.UNKNOWN_AGGREGATE.getMessage(name));
    }

    public static GraqlSemanticException statisticsAttributeTypesNotSpecified() {
        return new GraqlSemanticException(ErrorMessage.ATTRIBUTE_TYPE_NOT_SPECIFIED.getMessage());
    }

    public static GraqlSemanticException instanceDoesNotExist() {
        return new GraqlSemanticException(ErrorMessage.INSTANCE_DOES_NOT_EXIST.getMessage());
    }

    public static GraqlSemanticException kValueSmallerThanTwo() {
        return new GraqlSemanticException(ErrorMessage.K_SMALLER_THAN_TWO.getMessage());
    }

    public static GraqlSemanticException incompatibleAttributeValue(AttributeType.DataType dataType, Object value) {
        return new GraqlSemanticException("Value " + value + " is not compatible with attribute datatype: " + dataType.name());
    }

    public static GraqlSemanticException attributeMustBeANumber(AttributeType.DataType dataType, Label attributeType) {
        return new GraqlSemanticException(attributeType + " must have data type of `long` or `double`, but was " + dataType.name());
    }

    public static GraqlSemanticException attributesWithDifferentDataTypes(Collection<String> attributeTypes) {
        return new GraqlSemanticException("resource types " + attributeTypes + " have different data types");
    }

    public static GraqlSemanticException usingNegationWithReasoningOff(Pattern pattern) {
        return new GraqlSemanticException(ErrorMessage.USING_NEGATION_WITH_REASONING_OFF.getMessage(pattern));
    }

    public static GraqlSemanticException disjunctiveNegationBlock() {
        return new GraqlSemanticException(ErrorMessage.DISJUNCTIVE_NEGATION_BLOCK.getMessage());
    }

    public static GraqlSemanticException cannotParseDateFormat(String originalFormat) {
        return new GraqlSemanticException("Cannot parse date format " + originalFormat + ". See DateTimeFormatter#ofPattern");
    }

    public static GraqlSemanticException cannotParseDateString(String originalDate, String originalFormat, DateTimeParseException cause) {
        throw new GraqlSemanticException("Cannot parse date value " + originalDate + " with format " + originalFormat, cause);
    }

    public static GraqlSemanticException noLabelSpecifiedForHas(Variable var) {
        return create("'has' argument '%s' requires a label", var);
    }

    public static GraqlSemanticException insertRolePlayerWithoutRoleType() {
        return new GraqlSemanticException(ErrorMessage.INSERT_RELATION_WITHOUT_ROLE_TYPE.getMessage());
    }

    public static GraqlSemanticException insertAbstractOnNonType(SchemaConcept concept) {
        return new GraqlSemanticException(INSERT_ABSTRACT_NOT_TYPE.getMessage(concept.label()));
    }

    public static GraqlSemanticException unexpectedResult(Variable var) {
        return new GraqlSemanticException(UNEXPECTED_RESULT.getMessage(var.name()));
    }

}
