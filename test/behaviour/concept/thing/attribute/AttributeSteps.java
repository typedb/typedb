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

package hypergraph.test.behaviour.concept.thing.attribute;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.LocalDateTime;

import static hypergraph.test.behaviour.concept.thing.ThingSteps.get;
import static hypergraph.test.behaviour.concept.thing.ThingSteps.put;
import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AttributeSteps {

    @When("attribute\\( ?{type_label} ?) instances contain: {var}")
    public void attribute_instances_contain(String typeLabel, String var) {
        assertTrue(tx().concepts().getAttributeType(typeLabel).instances().anyMatch(i -> i.equals(get(var))));
    }

    @Then("attribute {var} has value type: {value_type}")
    public void attribute_has_value_type(String var, Class<?> valueType) {
        assertEquals(valueType, get(var).asAttribute().type().valueType());
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?boolean ?) put: {bool}")
    public void attribute_type_as_boolean_put(String var, String typeLabel, boolean value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asBoolean().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?long ?) put: {int}")
    public void attribute_type_as_long_put(String var, String typeLabel, long value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asLong().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?double ?) put: {double}")
    public void attribute_type_as_double_put(String var, String typeLabel, double value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDouble().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?string ?) put: {word}")
    public void attribute_type_as_string_put(String var, String typeLabel, String value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asString().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?datetime ?) put: {datetime}")
    public void attribute_type_as_datetime_put(String var, String typeLabel, LocalDateTime value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDateTime().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?boolean ?) get: {bool}")
    public void attribute_type_as_boolean_get(String var, String typeLabel, boolean value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asBoolean().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?long ?) get: {int}")
    public void attribute_type_as_long_get(String var, String typeLabel, long value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asLong().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?double ?) get: {double}")
    public void attribute_type_as_double_get(String var, String typeLabel, double value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDouble().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?string ?) get: {word}")
    public void attribute_type_as_string_get(String var, String typeLabel, String value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asString().get(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?datetime ?) get: {datetime}")
    public void attribute_type_as_datetime_get(String var, String typeLabel, LocalDateTime value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDateTime().get(value));
    }

    @Then("attribute {var} has boolean value: {bool}")
    public void attribute_has_boolean_value(String var, boolean value) {
        assertEquals(value, get(var).asAttribute().asBoolean().value());
    }

    @Then("attribute {var} has long value: {long}")
    public void attribute_has_long_value(String var, long value) {
        assertEquals(value, get(var).asAttribute().asLong().value().longValue());
    }

    @Then("attribute {var} has double value: {double}")
    public void attribute_has_double_value(String var, double value) {
        assertEquals(value, get(var).asAttribute().asDouble().value(), 0.0001);
    }

    @Then("attribute {var} has string value: {word}")
    public void attribute_has_boolean_value(String var, String value) {
        assertEquals(value, get(var).asAttribute().asString().value());
    }

    @Then("attribute {var} has datetime value: {datetime}")
    public void attribute_has_datetime_value(String var, LocalDateTime value) {
        assertEquals(value, get(var).asAttribute().asDateTime().value());
    }
}
