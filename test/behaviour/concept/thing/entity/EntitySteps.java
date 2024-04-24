/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.concept.thing.entity;

import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.LocalDateTime;

import static com.vaticle.typedb.core.common.test.Util.assertThrows;
import static com.vaticle.typedb.core.test.behaviour.concept.thing.ThingSteps.get;
import static com.vaticle.typedb.core.test.behaviour.concept.thing.ThingSteps.put;
import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.tx;
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

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {int}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, int keyValue) {
        Attribute.Long key = tx().concepts().getAttributeType(keyType).asLong().put(keyValue);
        Entity entity = tx().concepts().getEntityType(type).create();
        entity.setHas(key);
        put(var, entity);
    }

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {word}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, String keyValue) {
        Attribute.String key = tx().concepts().getAttributeType(keyType).asString().put(keyValue);
        Entity entity = tx().concepts().getEntityType(type).create();
        entity.setHas(key);
        put(var, entity);
    }

    @When("{var} = entity\\( ?{type_label} ?) create new instance with key\\( ?{type_label} ?): {datetime}")
    public void entity_type_create_new_instance_with_key(String var, String type, String keyType, LocalDateTime keyValue) {
        Attribute.DateTime key = tx().concepts().getAttributeType(keyType).asDateTime().put(keyValue);
        Entity entity = tx().concepts().getEntityType(type).create();
        entity.setHas(key);
        put(var, entity);
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {long}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, long keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asLong().get(keyValue).getOwners()
                .filter(owner -> owner.getType().equals(tx().concepts().getEntityType(type)))
                .first().orElse(null));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {word}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, String keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asString().get(keyValue).getOwners()
                .filter(owner -> owner.getType().equals(tx().concepts().getEntityType(type)))
                .first().orElse(null));
    }

    @When("{var} = entity\\( ?{type_label} ?) get instance with key\\( ?{type_label} ?): {datetime}")
    public void entity_type_get_instance_with_key(String var1, String type, String keyType, LocalDateTime keyValue) {
        put(var1, tx().concepts().getAttributeType(keyType).asDateTime().get(keyValue).getOwners()
                .filter(owner -> owner.getType().equals(tx().concepts().getEntityType(type)))
                .first().orElse(null));
    }

    @Then("entity\\( ?{type_label} ?) get instances contain: {var}")
    public void entity_type_get_instances_contain(String typeLabel, String var) {
        assertTrue(tx().concepts().getEntityType(typeLabel).getInstances().anyMatch(i -> i.equals(get(var))));
    }

    @Then("entity\\( ?{type_label} ?) get instances is empty")
    public void entity_type_get_instances_is_empty(String typeLabel) {
        assertEquals(0, tx().concepts().getEntityType(typeLabel).getInstances().count());
    }
}
