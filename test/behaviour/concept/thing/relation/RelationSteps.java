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

package hypergraph.test.behaviour.concept.thing.relation;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import static hypergraph.test.behaviour.concept.thing.ThingSteps.get;
import static hypergraph.test.behaviour.concept.thing.ThingSteps.put;
import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RelationSteps {

    @When("{var} = relation\\( ?{type_label} ?) create new instance")
    public void relation_type_create_new_instance(String var, String typeLabel) {
        put(var, tx().concepts().getRelationType(typeLabel).create());
    }

    @When("{var} = relation\\( ?{type_label} ?) get first instance")
    public void relation_type_get_first_instance(String var, String typeLabel) {
        put(var, tx().concepts().getRelationType(typeLabel).instances().findFirst().orElse(null));
    }

    @Then("relation\\( ?{type_label} ?) get instances contain: {var}")
    public void relation_type_get_instances_contain(String typeLabel, String var) {
        assertTrue(tx().concepts().getRelationType(typeLabel).instances().anyMatch(i -> i.equals(get(var))));
    }

    @Then("relation\\( ?{type_label} ?) get instances do not contain: {var}")
    public void relation_type_get_instances_do_not_contain(String typeLabel, String var) {
        assertTrue(tx().concepts().getRelationType(typeLabel).instances().noneMatch(i -> i.equals(get(var))));
    }

    @Then("relation\\( ?{type_label} ?) get instances is empty")
    public void relation_type_get_instances_is_empty(String typeLabel) {
        assertEquals(0, tx().concepts().getRelationType(typeLabel).instances().count());
    }

    @When("relation {var} set player for role\\( ?{type_label} ?): {var}")
    public void relation_set_player_for_role(String var1, String roleTypeLabel, String var2) {
        get(var1).asRelation().relate(get(var1).asRelation().type().role(roleTypeLabel), get(var2));
    }

    @When("relation {var} remove player for role\\( ?{type_label} ?): {var}")
    public void relation_remove_player_for_role(String var1, String roleTypeLabel, String var2) {
        get(var1).asRelation().unrelate(get(var1).asRelation().type().role(roleTypeLabel), get(var2));
    }

    @Then("relation {var} get players contain: {var}")
    public void relation_get_players_contain(String var1, String var2) {
        assertTrue(get(var1).asRelation().players().anyMatch(p -> p.equals(get(var2))));
    }

    @Then("relation {var} get players do not contain: {var}")
    public void relation_get_players_do_not_contain(String var1, String var2) {
        assertTrue(get(var1).asRelation().players().noneMatch(p -> p.equals(get(var2))));
    }

    @Then("relation {var} get players for role\\( ?{type_label} ?) contain: {var}")
    public void relation_get_player_for_role_contain(String var1, String roleTypeLabel, String var2) {
        assertTrue(get(var1).asRelation().players(get(var1).asRelation().type().role(roleTypeLabel)).anyMatch(p -> p.equals(get(var2))));
    }

    @Then("relation {var} get players for role\\( ?{type_label} ?) do not contain: {var}")
    public void relation_get_player_for_role_do_not_contain(String var1, String roleTypeLabel, String var2) {
        assertTrue(get(var1).asRelation().players(get(var1).asRelation().type().role(roleTypeLabel)).noneMatch(p -> p.equals(get(var2))));
    }
}
