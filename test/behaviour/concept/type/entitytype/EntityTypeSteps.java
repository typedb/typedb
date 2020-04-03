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

package hypergraph.test.behaviour.concept.type.entitytype;

import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.ThingType;
import hypergraph.concept.type.Type;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static grakn.common.util.Collections.list;
import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EntityTypeSteps {

    @When("put entity type: {word}")
    public void put_entity_type(String label) {
        tx().concepts().putEntityType(label);
    }

    @Then("entity\\( ?{word} ?) is null: {bool}")
    public void entity_is_null(String label, boolean isNull) {
        assertEquals(isNull, isNull(tx().concepts().getEntityType(label)));
    }

    @When("entity\\( ?{word} ?) set label: {word}")
    public void entity_set_label(String label, String newLabel) {
        tx().concepts().getEntityType(label).label(newLabel);
    }

    @Then("entity\\( ?{word} ?) get label: {word}")
    public void entity_get_label(String label, String getLabel) {
        assertEquals(getLabel, tx().concepts().getEntityType(label).label());
    }

    @When("entity\\( ?{word} ?) set abstract: {bool}")
    public void entity_set_abstract(String label, boolean isAbstract) {
        tx().concepts().getEntityType(label).setAbstract(isAbstract);
    }

    @Then("entity\\( ?{word} ?) is abstract: {bool}")
    public void entity_is_abstract(String label, boolean isAbstract) {
        assertEquals(isAbstract, tx().concepts().getEntityType(label).isAbstract());
    }

    @When("entity\\( ?{word} ?) set supertype: {word}")
    public void entity_set_supertype(String label, String superLabel) {
        EntityType supertype = tx().concepts().getEntityType(superLabel);
        tx().concepts().getEntityType(label).sup(supertype);
    }

    @Then("entity\\( ?{word} ?) get supertype: {word}")
    public void entity_get_supertype(String label, String superLabel) {
        EntityType supertype = tx().concepts().getEntityType(superLabel);
        assertEquals(supertype, tx().concepts().getEntityType(label).sup());
    }

    @Then("entity\\( ?{word} ?) get supertypes contain: {word}")
    public void entity_get_supertypes_contain(String label, String superLabel) {
        entity_get_supertypes_contain(label, list(superLabel));
    }

    @Then("entity\\( ?{word} ?) get supertypes contain:")
    public void entity_get_supertypes_contain(String label, List<String> superLabels) {
        Set<String> actuals = tx().concepts().getEntityType(label).sups().map(ThingType::label).collect(toSet());
        assertTrue(actuals.containsAll(superLabels));
    }

    @Then("entity\\( ?{word} ?) get subtypes contain: {word}")
    public void entity_get_subtypes_contain(String label, String subLabel) {
        entity_get_subtypes_contain(label, list(subLabel));
    }

    @Then("entity\\( ?{word} ?) get subtypes contain:")
    public void entity_get_subtypes_contain(String label, List<String> subLabels) {
        Set<String> actuals = tx().concepts().getEntityType(label).subs().map(ThingType::label).collect(toSet());
        assertTrue(actuals.containsAll(subLabels));
    }

    @When("entity\\( ?{word} ?) set key attribute: {word}")
    public void entity_set_key_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getEntityType(label).key(attributeType);
    }

    @When("entity\\( ?{word} ?) remove key attribute: {word}")
    public void entity_remove_key_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getEntityType(label).unkey(attributeType);
    }

    @Then("entity\\( ?{word} ?) get key attributes contain: {word}")
    public void entity_get_key_attributes_contain(String label, String attributeLabel) {
        entity_get_key_attributes_contain(label, list(attributeLabel));
    }

    @Then("entity\\( ?{word} ?) get key attributes contain:")
    public void entity_get_key_attributes_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getEntityType(label).keys().map(Type::label).collect(toSet());
        assertTrue(actuals.containsAll(attributeLabels));
    }

    @Then("entity\\( ?{word} ?) get key attributes does not contain: {word}")
    public void entity_get_key_attributes_does_not_contain(String label, String attributeLabel) {
        entity_get_key_attributes_does_not_contain(label, list(attributeLabel));
    }

    @Then("entity\\( ?{word} ?) get key attributes does not contain:")
    public void entity_get_key_attributes_does_not_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getEntityType(label).keys().map(Type::label).collect(toSet());
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @When("entity\\( ?{word} ?) set has attribute: {word}")
    public void entity_set_has_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getEntityType(label).has(attributeType);
    }

    @When("entity\\( ?{word} ?) remove has attribute: {word}")
    public void entity_remove_has_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getEntityType(label).unhas(attributeType);
    }

    @Then("entity\\( ?{word} ?) get has attributes contain: {word}")
    public void entity_get_has_attributes_contain(String label, String attributeLabel) {
        entity_get_has_attributes_contain(label, list(attributeLabel));
    }

    @Then("entity\\( ?{word} ?) get has attributes contain:")
    public void entity_get_has_attributes_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getEntityType(label).attributes().map(Type::label).collect(toSet());
    }

    @Then("entity\\( ?{word} ?) get has attributes does not contain: {word}")
    public void entity_get_has_attributes_does_not_contain(String label, String attributeLabel) {
        entity_get_has_attributes_does_not_contain(label, list(attributeLabel));
    }

    @Then("entity\\( ?{word} ?) get has attributes does not contain:")
    public void entity_get_has_attributes_does_not_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getEntityType(label).keys().map(Type::label).collect(toSet());
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @When("entity\\( ?{word} ?) set plays role: {word}")
    public void entity_set_plays_role(String label, String roleLabel) {

    }

    @When("entity\\( ?{word} ?) remove plays role: {word}")
    public void entity_remove_plays_role(String label, String roleLabel) {

    }

    @Then("entity\\( ?{word} ?) get playing roles contain: {word}")
    public void entity_get_playing_roles_contain(String label, String roleLabel) {
        entity_get_playing_roles_contain(label, list(roleLabel));
    }

    @Then("entity\\( ?{word} ?) get playing roles contain:")
    public void entity_get_playing_roles_contain(String label, List<String> roleLabels) {

    }

    @Then("entity\\( ?{word} ?) creates instance successfully: {bool}")
    public void entity_creates_instance_successfully(String label, boolean isSuccessful) {
        // TODO: implement this
    }
}
