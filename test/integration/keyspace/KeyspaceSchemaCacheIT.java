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

package grakn.core.keyspace;

import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertTrue;

/**
 * Tests expected behavior of shared JanusGraph and caching between sessions to the same keyspace
 * as well as synchronizing the KeyspaceCache on commits
 */
@SuppressWarnings("Duplicates")
public class KeyspaceSchemaCacheIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();
    private Session session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void tearDown() {
        session.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithSameLocalSession(){
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (Transaction tx = session.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
    }


    /**
     * Check if schema is accessible in new local session.
     * The new session in created AFTER the schema has been defined
     */
    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionAfterSchemaIsDefined(){
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        Session testSession = server.sessionFactory().session(session.keyspace());
        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    /**
     * Check if schema is accessible in new local session.
     * The new session in created BEFORE the schema has been defined
     */
    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionBeforeSchemaIsDefined(){
        Session testSession = server.sessionFactory().session(session.keyspace());
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getSchemaConcept(grakn.core.kb.concept.api.Label.of("entity")).subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionClosingPreviousOne(){
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        session.close();
        Session testSession = server.sessionFactory().session(session.keyspace());
        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

}
