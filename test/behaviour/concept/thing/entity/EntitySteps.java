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

package hypergraph.test.behaviour.concept.thing.entity;

import hypergraph.concept.thing.Attribute;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.LocalDateTime;

import static hypergraph.test.behaviour.concept.thing.ThingSteps.get;
import static hypergraph.test.behaviour.concept.thing.ThingSteps.put;
import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static hypergraph.test.behaviour.util.Util.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EntitySteps {

    @When("{var} = entity\\( ?{type_label} ?) create new instance")
    public void entity_type_create_new_instance(String var, String typeLabel) {
        put(var, tx().concepts().getEntityType(typeLabel).create());
    }

    @When("entity\\( ?{type_label} ?) create new instance; throws exception")
    public void entity_type_create_new_instance_throws_exception(String typeLabel) {
        assertThrows(() -> tx().concepts().getEntityType(typeLabel).create());
    }

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {bool}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, boolean keyValue) {
        Attribute.Boolean key = tx().concepts().getAttributeType(keyType).asBoolean().put(keyValue);
        put(var, tx().concepts().getEntityType(type).create().has(key));
    }

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {int}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, int keyValue) {
        Attribute.Long key = tx().concepts().getAttributeType(keyType).asLong().put(keyValue);
        put(var, tx().concepts().getEntityType(type).create().has(key));
    }

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {double}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, double keyValue) {
        Attribute.Double key = tx().concepts().getAttributeType(keyType).asDouble().put(keyValue);
        put(var, tx().concepts().getEntityType(type).create().has(key));
    }

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {word}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, String keyValue) {
        Attribute.String key = tx().concepts().getAttributeType(keyType).asString().put(keyValue);
        put(var, tx().concepts().getEntityType(type).create().has(key));
    }

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {datetime}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, LocalDateTime keyValue) {
        Attribute.DateTime key = tx().concepts().getAttributeType(keyType).asDateTime().put(keyValue);
        put(var, tx().concepts().getEntityType(type).create().has(key));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key: {var}")
    public void entity_type_get_instance_with_key(String var1, String type, String var2) {
        put(var1, get(var1).asAttribute().owners()
                .filter(owner -> owner.type().equals(tx().concepts().getEntityType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {bool}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, boolean keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asBoolean().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getEntityType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {long}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, long keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asLong().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getEntityType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {double}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, double keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asDouble().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getEntityType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {word}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, String keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asString().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getEntityType(type)))
                .findFirst().orElse(null));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {datetime}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, LocalDateTime keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asDateTime().get(keyValue).owners()
                .filter(owner -> owner.type().equals(tx().concepts().getEntityType(type)))
                .findFirst().orElse(null));
    }

    @Then("entity\\( ?{type_label} ?) get instances contain: {var}")
    public void entity_type_get_instances_contain(String typeLabel, String var) {
        assertTrue(tx().concepts().getEntityType(typeLabel).instances().anyMatch(i -> i.equals(get(var))));
    }

    @Then("entity\\( ?{type_label} ?) get instances is empty")
    public void entity_type_get_instances_is_empty(String typeLabel) {
        assertEquals(0, tx().concepts().getEntityType(typeLabel).instances().count());
    }
}
