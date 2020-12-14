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

package grakn.core.test.behaviour.concept.type.attributetype;

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.ThingType;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.test.behaviour.connection.ConnectionSteps.tx;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Behaviour Steps specific to AttributeSteps
 */
public class AttributeTypeSteps {

    @When("put attribute type: {type_label}, with value type: {value_type}")
    public void put_attribute_type_with_value_type(String typeLabel, AttributeType.ValueType valueType) {
        tx().concepts().putAttributeType(typeLabel, valueType);
    }

    @Then("attribute\\( ?{type_label} ?) get value type: {value_type}")
    public void attribute_type_get_value_type(String typeLabel, AttributeType.ValueType valueType) {
        assertEquals(valueType, tx().concepts().getAttributeType(typeLabel).getValueType());
    }

    @Then("attribute\\( ?{type_label} ?) get supertype value type: {value_type}")
    public void attribute_type_get_supertype_value_type(String typeLabel, AttributeType.ValueType valueType) {
        final AttributeType supertype = tx().concepts().getAttributeType(typeLabel).getSupertype();
        assertEquals(valueType, supertype.getValueType());
    }

    private AttributeType attribute_type_as_value_type(String typeLabel, AttributeType.ValueType valueType) {
        final AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);

        switch (valueType) {
            case OBJECT:
                return attributeType;
            case BOOLEAN:
                return attributeType.asBoolean();
            case LONG:
                return attributeType.asLong();
            case DOUBLE:
                return attributeType.asDouble();
            case STRING:
                return attributeType.asString();
            case DATETIME:
                return attributeType.asDateTime();
            default:
                throw GraknException.of(UNRECOGNISED_VALUE);
        }
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) get subtypes contain:")
    public void attribute_type_as_value_type_get_subtypes_contain(String typeLabel, AttributeType.ValueType valueType, List<String> subLabels) {
        final AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        final Set<String> actuals = attributeType.getSubtypes().map(ThingType::getLabel).map(Label::toString).collect(toSet());
        assertTrue(actuals.containsAll(subLabels));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) get subtypes do not contain:")
    public void attribute_type_as_value_type_get_subtypes_do_not_contain(String typeLabel, AttributeType.ValueType valueType, List<String> subLabels) {
        final AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        final Set<String> actuals = attributeType.getSubtypes().map(ThingType::getLabel).map(Label::toString).collect(toSet());
        for (String subLabel : subLabels) {
            assertFalse(actuals.contains(subLabel));
        }
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) set regex: {}")
    public void attribute_type_as_value_type_set_regex(String typeLabel, AttributeType.ValueType valueType, String regex) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        final AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        attributeType.asString().setRegex(Pattern.compile(regex));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) unset regex")
    public void attribute_type_as_value_type_unset_regex(String typeLabel, AttributeType.ValueType valueType) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        final AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        attributeType.asString().unsetRegex();
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) get regex: {}")
    public void attribute_type_as_value_type_get_regex(String typeLabel, AttributeType.ValueType valueType, String regex) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        final AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        assertEquals(regex, attributeType.asString().getRegex().pattern());
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) does not have any regex")
    public void attribute_type_as_value_type_does_not_have_any_regex(String typeLabel, AttributeType.ValueType valueType) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        final AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        assertNull(attributeType.asString().getRegex());
    }

    @Then("attribute\\( ?{type_label} ?) get key owners contain:")
    public void attribute_type_get_owners_as_key_contains(String typeLabel, List<String> ownerLabels) {
        final AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        final Set<String> actuals = attributeType.getOwners(true).map(ThingType::getLabel).map(Label::toString).collect(toSet());
        assertTrue(actuals.containsAll(ownerLabels));
    }

    @Then("attribute\\( ?{type_label} ?) get key owners do not contain:")
    public void attribute_type_get_owners_as_key_do_not_contains(String typeLabel, List<String> ownerLabels) {
        final AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        final Set<String> actuals = attributeType.getOwners(true).map(ThingType::getLabel).map(Label::toString).collect(toSet());
        for (String ownerLabel : ownerLabels) {
            assertFalse(actuals.contains(ownerLabel));
        }
    }

    @Then("attribute\\( ?{type_label} ?) get attribute owners contain:")
    public void attribute_type_get_owners_as_attribute_contains(String typeLabel, List<String> ownerLabels) {
        final AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        final Set<String> actuals = attributeType.getOwners(false).map(ThingType::getLabel).map(Label::toString).collect(toSet());
        assertTrue(actuals.containsAll(ownerLabels));
    }

    @Then("attribute\\( ?{type_label} ?) get attribute owners do not contain:")
    public void attribute_type_get_owners_as_attribute_do_not_contains(String typeLabel, List<String> ownerLabels) {
        final AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        final Set<String> actuals = attributeType.getOwners(false).map(ThingType::getLabel).map(Label::toString).collect(toSet());
        for (String ownerLabel : ownerLabels) {
            assertFalse(actuals.contains(ownerLabel));
        }
    }
}
