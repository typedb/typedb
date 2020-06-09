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

import static hypergraph.test.behaviour.concept.thing.util.ThingMap.get;
import static hypergraph.test.behaviour.concept.thing.util.ThingMap.put;
import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;

public class AttributeSteps {

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?boolean ?) put: {bool}")
    public void attribute_as_put(String var, String typeLabel, boolean value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asBoolean().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?long ?) put: {int}")
    public void attribute_as_put(String var, String typeLabel, long value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asLong().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?double ?) put: {double}")
    public void attribute_as_put(String var, String typeLabel, double value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDouble().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?string ?) put: {word}")
    public void attribute_as_put(String var, String typeLabel, String value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asString().put(value));
    }

    @When("{var} = attribute\\( ?{type_label} ?) as\\( ?datetime ?) put: {datetime}")
    public void attribute_as_put(String var, String typeLabel, LocalDateTime value) {
        put(var, tx().concepts().getAttributeType(typeLabel).asDateTime().put(value));
    }

    @Then("attribute {var} is null: {bool}")
    public void attribute_is_null(String var, boolean isNull) {
        assertEquals(isNull, isNull(get(var).asAttribute()));
    }

    @Then("attribute {var} has type: {type_label}")
    public void attribute_has_type(String var, String typeLabel) {
        assertEquals(tx().concepts().getAttributeType(typeLabel), get(var).asAttribute().type());
    }

    @Then("attribute {var} has value type: {value_type}")
    public void attribute_has_type(String var, Class<?> valueType) {
        assertEquals(valueType, get(var).asAttribute().type().valueType());
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
