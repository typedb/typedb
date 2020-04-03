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

package hypergraph.test.behaviour.concept.type.relationtype;

import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.RelationType;
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

public class RelationTypeSteps {


    @When("put relation type: {word}")
    public void put_relation_type(String label) {
        tx().concepts().putRelationType(label);
    }

    @Then("relation\\( ?{word} ?) is null: {bool}")
    public void relation_is_null(String label, boolean isNull) {
        assertEquals(isNull, isNull(tx().concepts().getRelationType(label)));
    }

    @When("relation\\( ?{word} ?) set label: {word}")
    public void relation_set_label(String label, String newLabel) {
        tx().concepts().getRelationType(label).label(newLabel);
    }

    @Then("relation\\( ?{word} ?) get label: {word}")
    public void relation_get_label(String label, String getLabel) {
        assertEquals(getLabel, tx().concepts().getRelationType(label).label());
    }

    @When("relation\\( ?{word} ?) set abstract: {bool}")
    public void relation_set_abstract(String label, boolean isAbstract) {
        tx().concepts().getRelationType(label).setAbstract(isAbstract);
    }

    @Then("relation\\( ?{word} ?) is abstract: {bool}")
    public void relation_is_abstract(String label, boolean isAbstract) {
        assertEquals(isAbstract, tx().concepts().getRelationType(label).isAbstract());
    }

    @When("relation\\( ?{word} ?) set supertype: {word}")
    public void relation_set_supertype(String label, String superLabel) {
        RelationType supertype = tx().concepts().getRelationType(superLabel);
        tx().concepts().getRelationType(label).sup(supertype);
    }

    @Then("relation\\( ?{word} ?) get supertype: {word}")
    public void relation_get_supertype(String label, String superLabel) {
        RelationType supertype = tx().concepts().getRelationType(superLabel);
        assertEquals(supertype, tx().concepts().getRelationType(label).sup());
    }

    @Then("relation\\( ?{word} ?) get supertypes contain: {word}")
    public void relation_get_supertypes_contain(String label, String superLabel) {
        relation_get_supertypes_contain(label, list(superLabel));
    }

    @Then("relation\\( ?{word} ?) get supertypes contain:")
    public void relation_get_supertypes_contain(String label, List<String> superLabels) {
        Set<String> actuals = tx().concepts().getRelationType(label).sups().map(ThingType::label).collect(toSet());
        assertTrue(actuals.containsAll(superLabels));
    }

    @Then("relation\\( ?{word} ?) get subtypes contain: {word}")
    public void relation_get_subtypes_contain(String label, String subLabel) {
        relation_get_subtypes_contain(label, list(subLabel));
    }

    @Then("relation\\( ?{word} ?) get subtypes contain:")
    public void relation_get_subtypes_contain(String label, List<String> subLabels) {
        Set<String> actuals = tx().concepts().getRelationType(label).subs().map(ThingType::label).collect(toSet());
        assertTrue(actuals.containsAll(subLabels));
    }

    @When("relation\\( ?{word} ?) set key attribute: {word}")
    public void relation_set_key_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getRelationType(label).key(attributeType);
    }

    @When("relation\\( ?{word} ?) remove key attribute: {word}")
    public void relation_remove_key_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getRelationType(label).unkey(attributeType);
    }

    @Then("relation\\( ?{word} ?) get key attributes contain: {word}")
    public void relation_get_key_attributes_contain(String label, String attributeLabel) {
        relation_get_key_attributes_contain(label, list(attributeLabel));
    }

    @Then("relation\\( ?{word} ?) get key attributes contain:")
    public void relation_get_key_attributes_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getRelationType(label).keys().map(Type::label).collect(toSet());
        assertTrue(actuals.containsAll(attributeLabels));
    }

    @Then("relation\\( ?{word} ?) get key attributes does not contain: {word}")
    public void relation_get_key_attributes_does_not_contain(String label, String attributeLabel) {
        relation_get_key_attributes_does_not_contain(label, list(attributeLabel));
    }

    @Then("relation\\( ?{word} ?) get key attributes does not contain:")
    public void relation_get_key_attributes_does_not_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getRelationType(label).keys().map(Type::label).collect(toSet());
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @When("relation\\( ?{word} ?) set has attribute: {word}")
    public void relation_set_has_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getRelationType(label).has(attributeType);
    }

    @When("relation\\( ?{word} ?) remove has attribute: {word}")
    public void relation_remove_has_attribute(String label, String attributeLabel) {
        AttributeType attributeType = tx().concepts().getAttributeType(attributeLabel);
        tx().concepts().getRelationType(label).unhas(attributeType);
    }

    @Then("relation\\( ?{word} ?) get has attributes contain: {word}")
    public void relation_get_has_attributes_contain(String label, String attributeLabel) {
        relation_get_has_attributes_contain(label, list(attributeLabel));
    }

    @Then("relation\\( ?{word} ?) get has attributes contain:")
    public void relation_get_has_attributes_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getRelationType(label).attributes().map(Type::label).collect(toSet());
        assertTrue(actuals.containsAll(attributeLabels));
    }

    @Then("relation\\( ?{word} ?) get has attributes does not contain: {word}")
    public void relation_get_has_attributes_does_not_contain(String label, String attributeLabel) {
        relation_get_has_attributes_does_not_contain(label, list(attributeLabel));
    }

    @Then("relation\\( ?{word} ?) get has attributes does not contain:")
    public void relation_get_has_attributes_does_not_contain(String label, List<String> attributeLabels) {
        Set<String> actuals = tx().concepts().getRelationType(label).keys().map(Type::label).collect(toSet());
        for (String attributeLabel : attributeLabels) {
            assertFalse(actuals.contains(attributeLabel));
        }
    }

    @When("relation\\( ?{word} ?) set plays role: {word}")
    public void relation_set_plays_role(String label, String roleLabel) {

    }

    @When("relation\\( ?{word} ?) remove plays role: {word}")
    public void relation_remove_plays_role(String label, String roleLabel) {

    }

    @Then("relation\\( ?{word} ?) get playing roles contain: {word}")
    public void relation_get_playing_roles_contain(String label, String roleLabel) {
        relation_get_playing_roles_contain(label, list(roleLabel));
    }

    @Then("relation\\( ?{word} ?) get playing roles contain:")
    public void relation_get_playing_roles_contain(String label, List<String> roleLabels) {

    }

    @Then("relation\\( ?{word} ?) creates instance successfully: {bool}")
    public void relation_creates_instance_successfully(String label, boolean isSuccessful) {
        // TODO: implement this
    }
}
