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

import grakn.client.GraknClient;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
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
    private Session localSession;
    private GraknClient.Session remoteSession;
    private GraknClient graknClient;

    @Before
    public void setUp() {
        localSession = server.sessionWithNewKeyspace();
        graknClient = new GraknClient(server.grpcUri());
        remoteSession = graknClient.session(localSession.keyspace().name());
    }

    @After
    public void tearDown() {
        localSession.close();
        remoteSession.close();
        graknClient.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithSameLocalSession(){
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (Transaction tx = localSession.readTransaction()) {
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
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        Session testSession = server.sessionFactory().session(localSession.keyspace());
        try (Transaction tx = testSession.readTransaction()) {
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
        Session testSession = server.sessionFactory().session(localSession.keyspace());
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (Transaction tx = testSession.readTransaction()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionClosingPreviousOne(){
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        localSession.close();
        Session testSession = server.sessionFactory().session(localSession.keyspace());
        try (Transaction tx = testSession.readTransaction()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithRemoteSession(){
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (GraknClient.Transaction tx = remoteSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
            assertTrue(entityTypeSubs.contains("animal"));
        }
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewRemoteSession(){
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        GraknClient.Session testSession = new GraknClient(server.grpcUri()).session(localSession.keyspace().name());
        try (GraknClient.Transaction tx = testSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewRemoteSessionClosingPreviousOne(){
        try (Transaction tx = localSession.writeTransaction()) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        remoteSession.close();
        GraknClient.Session testSession = graknClient.session(localSession.keyspace().name());
        try (GraknClient.Transaction tx = testSession.transaction().read()) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }
}
