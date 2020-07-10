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

package grakn.test.behaviour.concept.thing.relation;

import grakn.concept.thing.Attribute;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.LocalDateTime;

import static grakn.test.behaviour.concept.thing.ThingSteps.get;
import static grakn.test.behaviour.concept.thing.ThingSteps.put;
import static grakn.test.behaviour.connection.ConnectionSteps.tx;
import static grakn.test.behaviour.util.Util.assertThrows;
import static org.junit.Assert.assertEquals;
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

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {bool}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, boolean keyValue) {
        Attribute.Boolean key = tx().concepts().getAttributeType(keyType).asBoolean().put(keyValue);
        put(var, tx().concepts().getRelationType(type).create().has(key));
    }

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {int}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, int keyValue) {
        Attribute.Long key = tx().concepts().getAttributeType(keyType).asLong().put(keyValue);
        put(var, tx().concepts().getRelationType(type).create().has(key));
    }

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {double}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, double keyValue) {
        Attribute.Double key = tx().concepts().getAttributeType(keyType).asDouble().put(keyValue);
        put(var, tx().concepts().getRelationType(type).create().has(key));
    }

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {word}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, String keyValue) {
        Attribute.String key = tx().concepts().getAttributeType(keyType).asString().put(keyValue);
        put(var, tx().concepts().getRelationType(type).create().has(key));
    }

    @When("{var} = relation\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {datetime}")
    public void relation_type_create_new_instance_with_key(String var, String type, String keyType, LocalDateTime keyValue) {
        Attribute.DateTime key = tx().concepts().getAttributeType(keyType).asDateTime().put(keyValue);
        put(var, tx().concepts().getRelationType(type).create().has(key));
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key: {var}")
    public void relation_type_get_instance_with_key(String var1, String type, String var2) {
        put(var1, get(var2).asAttribute().owners()
                .filter(owner -> owner.type().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {bool}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, boolean keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asBoolean().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {long}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, long keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asLong().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {double}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, double keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asDouble().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }


    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {word}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, String keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asString().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = relation\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {datetime}")
    public void relation_type_get_instance_with_key(String var1, String type, String keyType, LocalDateTime keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asDateTime().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getRelationType(type)))
                .findFirst().orElse(null));
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
