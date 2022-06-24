/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.concept.type.attributetype;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.ThingType;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.tx;
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
        AttributeType supertype = tx().concepts().getAttributeType(typeLabel).getSupertype().asAttributeType();
        assertEquals(valueType, supertype.getValueType());
    }

    private AttributeType attribute_type_as_value_type(String typeLabel, AttributeType.ValueType valueType) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);

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
                throw TypeDBException.of(UNRECOGNISED_VALUE);
        }
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) get subtypes contain:")
    public void attribute_type_as_value_type_get_subtypes_contain(String typeLabel, AttributeType.ValueType valueType, List<String> subLabels) {
        AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        Set<String> actuals = attributeType.getSubtypes().map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(subLabels));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) get subtypes do not contain:")
    public void attribute_type_as_value_type_get_subtypes_do_not_contain(String typeLabel, AttributeType.ValueType valueType, List<String> subLabels) {
        AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        Set<String> actuals = attributeType.getSubtypes().map(ThingType::getLabel).map(Label::toString).toSet();
        for (String subLabel : subLabels) {
            assertFalse(actuals.contains(subLabel));
        }
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) set regex: {}")
    public void attribute_type_as_value_type_set_regex(String typeLabel, AttributeType.ValueType valueType, String regex) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        attributeType.asString().setRegex(Pattern.compile(regex));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) unset regex")
    public void attribute_type_as_value_type_unset_regex(String typeLabel, AttributeType.ValueType valueType) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        attributeType.asString().unsetRegex();
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) get regex: {}")
    public void attribute_type_as_value_type_get_regex(String typeLabel, AttributeType.ValueType valueType, String regex) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        assertEquals(regex, attributeType.asString().getRegex().pattern());
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?{value_type} ?) does not have any regex")
    public void attribute_type_as_value_type_does_not_have_any_regex(String typeLabel, AttributeType.ValueType valueType) {
        if (!valueType.equals(AttributeType.ValueType.STRING)) fail();
        AttributeType attributeType = attribute_type_as_value_type(typeLabel, valueType);
        assertNull(attributeType.asString().getRegex());
    }

    @Then("attribute\\( ?{type_label} ?) get key owners contain:")
    public void attribute_type_get_owners_as_key_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwners(true).map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(ownerLabels));
    }

    @Then("attribute\\( ?{type_label} ?) get key owners do not contain:")
    public void attribute_type_get_owners_as_key_do_not_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwners(true).map(ThingType::getLabel).map(Label::toString).toSet();
        for (String ownerLabel : ownerLabels) {
            assertFalse(actuals.contains(ownerLabel));
        }
    }

    @Then("attribute\\( ?{type_label} ?) get key owners explicit contain:")
    public void attribute_type_get_owners_explicit_as_key_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwnersExplicit(true).map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(ownerLabels));
    }

    @Then("attribute\\( ?{type_label} ?) get key owners explicit do not contain:")
    public void attribute_type_get_owners_explicit_as_key_do_not_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwnersExplicit(true).map(ThingType::getLabel).map(Label::toString).toSet();
        for (String ownerLabel : ownerLabels) {
            assertFalse(actuals.contains(ownerLabel));
        }
    }

    @Then("attribute\\( ?{type_label} ?) get attribute owners contain:")
    public void attribute_type_get_owners_as_attribute_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwners(false).map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(ownerLabels));
    }

    @Then("attribute\\( ?{type_label} ?) get attribute owners do not contain:")
    public void attribute_type_get_owners_as_attribute_do_not_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwners(false).map(ThingType::getLabel).map(Label::toString).toSet();
        for (String ownerLabel : ownerLabels) {
            assertFalse(actuals.contains(ownerLabel));
        }
    }

    @Then("attribute\\( ?{type_label} ?) get attribute owners explicit contain:")
    public void attribute_type_get_owners_explicit_as_attribute_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwnersExplicit(false).map(ThingType::getLabel).map(Label::toString).toSet();
        assertTrue(actuals.containsAll(ownerLabels));
    }

    @Then("attribute\\( ?{type_label} ?) get attribute owners explicit do not contain:")
    public void attribute_type_get_owners_explicit_as_attribute_do_not_contain(String typeLabel, List<String> ownerLabels) {
        AttributeType attributeType = tx().concepts().getAttributeType(typeLabel);
        Set<String> actuals = attributeType.getOwnersExplicit(false).map(ThingType::getLabel).map(Label::toString).toSet();
        for (String ownerLabel : ownerLabels) {
            assertFalse(actuals.contains(ownerLabel));
        }
    }
}
