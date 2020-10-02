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

package grakn.core.test.behaviour.concept.thing.attribute;

import grakn.core.concept.type.AttributeType;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.LocalDateTime;

import static grakn.core.test.behaviour.concept.thing.ThingSteps.get;
import static grakn.core.test.behaviour.concept.thing.ThingSteps.put;
import static grakn.core.test.behaviour.connection.ConnectionSteps.tx;
import static grakn.core.test.behaviour.util.Util.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AttributeSteps {

    @When("attribute\\( ?{type_label} ?) get instances contain: {var}")
    public void attribute_type_get_instances_contain(final String typeLabel, final String var) {
        assertTrue(tx().concepts().getAttributeType(typeLabel).getInstances().anyMatch(i -> i.equals(get(var))));
    }

    @Then("attribute {var} get owners contain: {var}")
    public void attribute_get_owners_contain(final String var1, final String var2) {
        assertTrue(get(var1).asAttribute().getOwners().anyMatch(o -> o.equals(get(var2))));
    }

    @Then("attribute {var} get owners do not contain: {var}")
    public void attribute_get_owners_do_not_contain(final String var1, final String var2) {
        assertTrue(get(var1).asAttribute().getOwners().noneMatch(o -> o.equals(get(var2))));
    }

    @Then("attribute {var} has value type: {value_type}")
    public void attribute_has_value_type(final String var, final AttributeType.ValueType valueType) {
        assertEquals(valueType, get(var).asAttribute().getType().getValueType());
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?boolean ?) put: {bool}")
    public void attribute_type_as_boolean_put(final String var, final String typeLabel, final boolean value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asBoolean().put(value));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?boolean ?) put: {bool}; throws exception")
    public void attribute_type_as_boolean_put_throws_exception(final String typeLabel, final boolean value) {
        assertThrows(() -> tx().concepts().getAttributeType(typeLabel).asBoolean().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?long ?) put: {int}")
    public void attribute_type_as_long_put(final String var, final String typeLabel, final long value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asLong().put(value));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?long ?) put: {int}; throws exception")
    public void attribute_type_as_long_put_throws_exception(final String typeLabel, final long value) {
        assertThrows(() -> tx().concepts().getAttributeType(typeLabel).asLong().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?double ?) put: {double}")
    public void attribute_type_as_double_put(final String var, final String typeLabel, final double value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDouble().put(value));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?double ?) put: {double}; throws exception")
    public void attribute_type_as_double_put_throws_exception(final String typeLabel, final double value) {
        assertThrows(() -> tx().concepts().getAttributeType(typeLabel).asDouble().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?string ?) put: {word}")
    public void attribute_type_as_string_put(final String var, final String typeLabel, final String value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asString().put(value));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?string ?) put: {word}; throws exception")
    public void attribute_type_as_string_put_throws_exception(final String typeLabel, final String value) {
        assertThrows(() -> tx().concepts().getAttributeType(typeLabel).asString().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?datetime ?) put: {datetime}")
    public void attribute_type_as_datetime_put(final String var, final String typeLabel, final LocalDateTime value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDateTime().put(value));
    }

    @Then("attribute\\( ?{type_label} ?) as\\( ?datetime ?) put: {datetime}; throws exception")
    public void attribute_type_as_datetime_put_throws_exception(final String typeLabel, final LocalDateTime value) {
        assertThrows(() -> tx().concepts().getAttributeType(typeLabel).asDateTime().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?boolean ?) get: {bool}")
    public void attribute_type_as_boolean_get(final String var, final String typeLabel, final boolean value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asBoolean().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?long ?) get: {int}")
    public void attribute_type_as_long_get(final String var, final String typeLabel, final long value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asLong().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?double ?) get: {double}")
    public void attribute_type_as_double_get(final String var, final String typeLabel, final double value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDouble().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?string ?) get: {word}")
    public void attribute_type_as_string_get(final String var, final String typeLabel, final String value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asString().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?datetime ?) get: {datetime}")
    public void attribute_type_as_datetime_get(final String var, final String typeLabel, final LocalDateTime value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDateTime().get(value));
    }

    @Then("attribute {var} has boolean value: {bool}")
    public void attribute_has_boolean_value(final String var, final boolean value) {
        assertEquals(value, get(var).asAttribute().asBoolean().getValue());
    }

    @Then("attribute {var} has long value: {long}")
    public void attribute_has_long_value(final String var, final long value) {
        assertEquals(value, get(var).asAttribute().asLong().getValue().longValue());
    }

    @Then("attribute {var} has double value: {double}")
    public void attribute_has_double_value(final String var, final double value) {
        assertEquals(value, get(var).asAttribute().asDouble().getValue(), 0.0001);
    }

    @Then("attribute {var} has string value: {word}")
    public void attribute_has_string_value(final String var, final String value) {
        assertEquals(value, get(var).asAttribute().asString().getValue());
    }

    @Then("attribute {var} has datetime value: {datetime}")
    public void attribute_has_datetime_value(final String var, final LocalDateTime value) {
        assertEquals(value, get(var).asAttribute().asDateTime().getValue());
    }
}
