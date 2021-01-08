/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.test.behaviour.concept.thing.relation;

import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static grakn.core.test.behaviour.concept.thing.ThingSteps.get;
import static grakn.core.test.behaviour.concept.thing.ThingSteps.put;
import static grakn.core.test.behaviour.connection.ConnectionSteps.tx;
import static grakn.core.test.behaviour.util.Util.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RelationSteps {

    @When("{var} = relation\\( ?{type_label} ?) create new instance")
    public void relation_type_create_new_instance(String var, String typeLabel) {
        put(var, tx().concepts().getRelationType(typeLabel).create());
    }

    @Then("relation\\( ?{type_label} ?) create new instance; throws exception")
    public void relation_type_create_new_instance_throws_exception(String typeLabel) {
        assertThrows(() -> tx().concepts().getRelationType(typeLabel).create());
    }

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {int}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, int keyValue) {
        final Attribute.Long key = tx().concepts().getAttributeType(keyType).asLong().put(keyValue);
        final Relation relation = tx().concepts().getRelationType(type).create();
        relation.setHas(key);
        put(var, relation);
    }

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {word}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, String keyValue) {
        final Attribute.String key = tx().concepts().getAttributeType(keyType).asString().put(keyValue);
        final Relation relation = tx().concepts().getRelationType(type).create();
        relation.setHas(key);
        put(var, relation);
    }

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {datetime}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, LocalDateTime keyValue) {
        final Attribute.DateTime key = tx().concepts().getAttributeType(keyType).asDateTime().put(keyValue);
        final Relation relation = tx().concepts().getRelationType(type).create();
        relation.setHas(key);
        put(var, relation);
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {long}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, long keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asLong().get(keyValue).getOwners()
                .filter(owner -> owner.getType().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {word}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, String keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asString().get(keyValue).getOwners()
                .filter(owner -> owner.getType().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {datetime}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, LocalDateTime keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asDateTime().get(keyValue).getOwners()
                .filter(owner -> owner.getType().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }

    @Then("relation\\( ?{type_label} ?) get instances contain: {var}")
    public void relation_type_get_instances_contain(String typeLabel, String var) {
        assertTrue(tx().concepts().getRelationType(typeLabel).getInstances().anyMatch(i -> i.equals(get(var))));
    }

    @Then("relation\\( ?{type_label} ?) get instances do not contain: {var}")
    public void relation_type_get_instances_do_not_contain(String typeLabel, String var) {
        assertTrue(tx().concepts().getRelationType(typeLabel).getInstances().noneMatch(i -> i.equals(get(var))));
    }

    @Then("relation\\( ?{type_label} ?) get instances is empty")
    public void relation_type_get_instances_is_empty(String typeLabel) {
        assertEquals(0, tx().concepts().getRelationType(typeLabel).getInstances().count());
    }

    @When("relation {var} add player for role\\( ?{type_label} ?): {var}")
    public void relation_add_player_for_role(String var1, String roleTypeLabel, String var2) {
        get(var1).asRelation().addPlayer(get(var1).asRelation().getType().getRelates(roleTypeLabel), get(var2));
    }

    @When("relation {var} remove player for role\\( ?{type_label} ?): {var}")
    public void relation_remove_player_for_role(String var1, String roleTypeLabel, String var2) {
        get(var1).asRelation().removePlayer(get(var1).asRelation().getType().getRelates(roleTypeLabel), get(var2));
    }

    @Then("relation {var} get players contain:")
    public void relation_get_players_contain(String var, Map<String, String> players) {
        final Relation relation = get(var).asRelation();
        players.forEach((rt, var2) -> assertTrue(relation.getPlayersByRoleType().get(relation.getType().getRelates(rt)).contains(get(var2.substring(1)))));
    }

    @Then("relation {var} get players do not contain:")
    public void relation_get_players_do_not_contain(String var, Map<String, String> players) {
        final Relation relation = get(var).asRelation();
        players.forEach((rt, var2) -> {
            final List<? extends Thing> p;
            if ((p = relation.getPlayersByRoleType().get(relation.getType().getRelates(rt))) != null) {
                assertFalse(p.contains(get(var2.substring(1))));
            }
        });
    }

    @Then("relation {var} get players contain: {var}")
    public void relation_get_players_contain(String var1, String var2) {
        assertTrue(get(var1).asRelation().getPlayers().anyMatch(p -> p.equals(get(var2))));
    }

    @Then("relation {var} get players do not contain: {var}")
    public void relation_get_players_do_not_contain(String var1, String var2) {
        assertTrue(get(var1).asRelation().getPlayers().noneMatch(p -> p.equals(get(var2))));
    }

    @Then("relation {var} get players for role\\( ?{type_label} ?) contain: {var}")
    public void relation_get_player_for_role_contain(String var1, String roleTypeLabel, String var2) {
        assertTrue(get(var1).asRelation().getPlayers(get(var1).asRelation().getType().getRelates(roleTypeLabel)).anyMatch(p -> p.equals(get(var2))));
    }

    @Then("relation {var} get players for role\\( ?{type_label} ?) do not contain: {var}")
    public void relation_get_player_for_role_do_not_contain(String var1, String roleTypeLabel, String var2) {
        assertTrue(get(var1).asRelation().getPlayers(get(var1).asRelation().getType().getRelates(roleTypeLabel)).noneMatch(p -> p.equals(get(var2))));
    }
}
