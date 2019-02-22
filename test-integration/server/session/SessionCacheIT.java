package grakn.core.server.session;

import grakn.core.client.GraknClient;
import grakn.core.graql.concept.Role;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertTrue;

public class SessionCacheIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();
    private SessionImpl localSession;
    private GraknClient.Session remoteSession;

    @Before
    public void setUp() {
        localSession = server.sessionWithNewKeyspace();
        remoteSession = new GraknClient(server.grpcUri().toString()).session(localSession.keyspace().getName());
    }

    @After
    public void tearDown() {
        localSession.close();
        remoteSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithSameLocalSession(){
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (Transaction tx = localSession.transaction(Transaction.Type.READ)) {
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
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        SessionImpl testSession = server.sessionFactory().session(localSession.keyspace());
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
        SessionImpl testSession = server.sessionFactory().session(localSession.keyspace());
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionClosingPreviousOne(){
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        localSession.close();
        SessionImpl testSession = server.sessionFactory().session(localSession.keyspace());
        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithRemoteSession(){
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        System.out.println("done with local");
        try (Transaction tx = remoteSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
            assertTrue(entityTypeSubs.contains("animal"));
        }
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewRemoteSession(){
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        GraknClient.Session testSession = new GraknClient(server.grpcUri().toString()).session(localSession.keyspace().getName());
        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }

    @Test
    public void addEntityWithLocalSession_possibleToRetrieveItWithNewRemoteSessionClosingPreviousOne(){
        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
            tx.putEntityType("animal");
            Role role1 = tx.putRole("role1");
            Role role2 = tx.putRole("role2");
            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
            tx.commit();
        }
        remoteSession.close();
        GraknClient.Session testSession = new GraknClient(server.grpcUri().toString()).session(localSession.keyspace().getName());
        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(entityTypeSubs.contains("animal"));
            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
            assertTrue(relationshipTypeSubs.contains("test-relationship"));
        }
        testSession.close();
    }


//    @Test
//    public void addEntityWithLocalSession_possibleToRetrieveItWithNewLocalSessionClosingPreviousOne(){
//        try (Transaction tx = localSession.transaction(Transaction.Type.WRITE)) {
//            tx.putEntityType("animal");
//            Role role1 = tx.putRole("role1");
//            Role role2 = tx.putRole("role2");
//            tx.putRelationshipType("test-relationship").relates(role1).relates(role2);
//            tx.commit();
//        }
//        localSession.close();
//        SessionImpl testSession = server.sessionFactory().session(localSession.keyspace());
//        try (Transaction tx = testSession.transaction(Transaction.Type.READ)) {
//            Set<String> entityTypeSubs = tx.getMetaEntityType().subs().map(et -> et.label().getValue()).collect(toSet());
//            assertTrue(entityTypeSubs.contains("animal"));
//            Set<String> relationshipTypeSubs = tx.getMetaRelationType().subs().map(et -> et.label().getValue()).collect(toSet());
//            assertTrue(relationshipTypeSubs.contains("test-relationship"));
//        }
//        testSession.close();
//    }
//



}
