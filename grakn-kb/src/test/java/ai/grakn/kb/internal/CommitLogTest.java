/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.kb.internal;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import mjson.Json;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommitLogTest extends TxTestBase {

    @Test
    public void whenNoOp_EnsureLogWellFormed() {
        Json expected = Json.read("{\"" + REST.Request.COMMIT_LOG_COUNTING + "\":[]}");
        Json log = tx.commitLog().getFormattedLog();
        assertEquals("Unexpected graph logs", expected, log);
        assertFalse(log.has(REST.Request.COMMIT_LOG_FIXING));
    }

    @Test
    public void whenAddingEntitiesAndNoAttributes_EnsureOnlyCountSectionOfLogIsFilled() {
        EntityType entityType = tx.putEntityType("My Type");
        entityType.addEntity();
        entityType.addEntity();
        Json emptyLog = Json.read("{\"" + REST.Request.COMMIT_LOG_COUNTING + "\":[]}");
        assertEquals("Logs are not empty", emptyLog, tx.commitLog().getFormattedLog());

        tx.commit();

        Json filledLog = Json.read("{\"" + REST.Request.COMMIT_LOG_COUNTING  +
                "\":[{\"" + REST.Request.COMMIT_LOG_CONCEPT_ID +
                "\":\"" + entityType.getId() + "\",\"" + REST.Request.COMMIT_LOG_SHARDING_COUNT + "\":2}]}");
        assertEquals("Logs are empty", filledLog, tx.commitLog().getFormattedLog());
    }

    @Test
    public void whenAddingAttributes_EnsureCountSectionAndAttributeSectionOfLogIsFilled() {
        AttributeType<String> attributeType = tx.putAttributeType("My Type", AttributeType.DataType.STRING);
        attributeType.putAttribute("bob");
        attributeType.putAttribute("alice");

        Json logs = tx.commitLog().getFormattedLog();
        assertFalse("Contains fixing section when it should not", logs.has(REST.Request.COMMIT_LOG_FIXING));
        assertTrue("Does not contain count section when it should", logs.has(REST.Request.COMMIT_LOG_COUNTING));

        tx.commit();

        logs = tx.commitLog().getFormattedLog();

        assertTrue("Does not contain fixing section when it should", logs.has(REST.Request.COMMIT_LOG_FIXING));
        assertTrue("Does not contain count section when it should", logs.has(REST.Request.COMMIT_LOG_COUNTING));
        assertTrue(logs.at(REST.Request.COMMIT_LOG_FIXING).has(Schema.BaseType.ATTRIBUTE.name()));
        assertFalse(logs.at(REST.Request.COMMIT_LOG_FIXING).has(Schema.BaseType.RELATIONSHIP.name()));
    }

    @Test
    public void whenAddingRolePlayerToRelationship_EnsureRelationshipSectionOfLogIsFilled(){
        Role role1 = tx.putRole("role 1");
        Role role2 = tx.putRole("role 2");
        tx.putEntityType("An entity Type").plays(role1).plays(role2);
        tx.putRelationshipType("Relationship Type Thing").relates(role1).relates(role2);

        tx.commit();

        Json logs = tx.commitLog().getFormattedLog();
        assertFalse("Contains fixing section when it should not", logs.has(REST.Request.COMMIT_LOG_FIXING));


        //Switch to batch which should result in the logs filling
        tx = switchToBatchTx();
        EntityType entityType = tx.getEntityType("An entity Type");
        RelationshipType relationshipType = tx.getRelationshipType("Relationship Type Thing");
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();
        relationshipType.addRelationship().addRolePlayer(role1, e1).addRolePlayer(role2, e2);

        tx.commit();
        logs = tx.commitLog().getFormattedLog();
        assertTrue("Does not contain fixing section when it should", logs.has(REST.Request.COMMIT_LOG_FIXING));
        assertTrue("Does not contain count section when it should", logs.has(REST.Request.COMMIT_LOG_COUNTING));
        assertTrue(logs.at(REST.Request.COMMIT_LOG_FIXING).has(Schema.BaseType.RELATIONSHIP.name()));
        assertFalse(logs.at(REST.Request.COMMIT_LOG_FIXING).has(Schema.BaseType.ATTRIBUTE.name()));
    }
}
