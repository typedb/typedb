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

package hypergraph.test.behaviour.concept.type.attributetype;

import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.type.AttributeTypeInt;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Behaviour Steps specific to AttributeSteps
 */
public class AttributeTypeSteps {

    @When("put attribute type: {type_label}, value class: {value_class}")
    public void put_attribute_type_value_class(String typeLabel, Class<?> valueClass) {
        tx().concepts().putAttributeType(typeLabel, valueClass);
    }

    @Then("attribute\\( ?{type_label} ?) get value class: {value_class}")
    public void attribute_get_value_class(String typeLabel, Class<?> valueClass) {
        assertEquals(valueClass, tx().concepts().getAttributeType(typeLabel).valueClass());
    }

    @Then("attribute\\( ?{type_label} ?) fails at setting supertype: {type_label}")
    public void thing_fails_at_setting_key_attribute(String typeLabel, String superLabel) {
        AttributeTypeInt superType = tx().concepts().getAttributeType(superLabel);
        try {
            tx().concepts().getAttributeType(typeLabel).sup(superType);
            fail();
        } catch (HypergraphException ignored) {
            assertTrue(true);
        }
    }
}
