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

import hypergraph.Hypergraph;
import hypergraph.concept.type.EntityType;
import hypergraph.concept.type.Type;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static grakn.common.util.Collections.list;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessions;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessionsToTransactions;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EntityTypeSteps {

    private static Hypergraph.Transaction tx() {
        return sessionsToTransactions.get(sessions.get(0)).get(0);
    }

    @When("put entity type: {word}")
    public void put_entity_type(String type) {
        tx().concepts().putEntityType(type);
    }

    @When("entity\\( ?{word} ?) set label: {word}")
    public void entity_set_label(String label, String newLabel) {
        tx().concepts().getEntityType(label).label(newLabel);
    }

    @Then("entity\\( ?{word} ?) get label: {word}")
    public void entity_get_label(String label, String getLabel) {
        assertEquals(getLabel, tx().concepts().getEntityType(label).label());
    }

    @Then("entity\\( ?{word} ?) is null: {bool}")
    public void entity_is_null(String label, boolean isNull) {
        assertEquals(isNull, isNull(tx().concepts().getEntityType(label)));
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
        Set<String> actuals = tx().concepts().getEntityType(label).sups().map(Type::label).collect(toSet());
        assertTrue(actuals.containsAll(superLabels));
    }

    @Then("entity\\( ?{word} ?) get subtypes contain: {word}")
    public void entity_get_subtypes_contain(String label, String subLabel) {
        entity_get_subtypes_contain(label, list(subLabel));
    }

    @Then("entity\\( ?{word} ?) get subtypes contain:")
    public void entity_get_subtypes_contain(String label, List<String> subLabels) {
        Set<String> actuals = tx().concepts().getEntityType(label).subs().map(Type::label).collect(toSet());
        assertTrue(actuals.containsAll(subLabels));
    }

    @When("entity\\( ?{word} ?) set abstract: {bool}")
    public void entity_set_abstract(String label, boolean isAbstract) {

    }

    @Then("entity\\( ?{word} ?) is abstract: {bool}")
    public void entity_is_abstract(String label, boolean isAbstract) {

    }

    @When("entity\\( ?{word} ?) set key attribute: {word}")
    public void entity_set_key_attribute(String label, String attributeLabel) {

    }

    @When("entity\\( ?{word} ?) remove key attribute: {word}")
    public void entity_remove_key_attribute(String label, String attributeLabe) {

    }

    @Then("entity\\( ?{word} ?) get key attributes contain: {word}")
    public void entity_get_key_attributes_contain(String label, String attributeLabel) {
        entity_get_has_attributes_contain(label, list(attributeLabel));
    }

    @Then("entity\\( ?{word} ?) get key attributes contain:")
    public void entity_get_key_attributes_contain(String label, List<String> attributeLabels) {

    }

    @When("entity\\( ?{word} ?) set has attribute: {word}")
    public void entity_set_has_attribute(String label, String attributeLabel) {

    }

    @When("entity\\( ?{word} ?) remove has attribute: {word}")
    public void entity_remove_has_attribute(String label, String attributeLabel) {

    }

    @Then("entity\\( ?{word} ?) get has attributes contain: {word}")
    public void entity_get_has_attributes_contain(String label, String attributeLabel) {
        entity_get_has_attributes_contain(label, list(attributeLabel));
    }

    @Then("entity\\( ?{word} ?) get has attributes contain:")
    public void entity_get_has_attributes_contain(String label, List<String> attributeLabels) {

    }

    @When("entity\\( ?{word} ?) set plays role: {word}")
    public void entity_set_plays_role(String label, String roleLabel) {

    }

    @When("entity\\( ?{word} ?) remove plays role: {word}")
    public void entity_remove_plays_role(String label, String roleLabel) {

    }

    @When("entity\\( ?{word} ?) get playing roles contain: {word}")
    public void entity_get_playing_roles_contain(String label, String roleLabel) {
        entity_get_playing_roles_contain(label, list(roleLabel));
    }

    @When("entity\\( ?{word} ?) get playing roles contain:")
    public void entity_get_playing_roles_contain(String label, List<String> roleLabels) {

    }
}
