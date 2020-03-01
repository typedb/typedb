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
 */

package grakn.core.server.session;

import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.SessionException;
import grakn.core.kb.server.exception.TransactionException;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.keyspace.KeyspaceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SessionIT {
    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private Session session;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
    }

    @After
    public void closeSession() {
        session.close();
    }

    /**
     * When requesting 2 transactions from the same Session we expect to receive
     * 2 different objects
     */
    @Test
    public void sessionProducesDifferentTransactionObjects() {
        Transaction tx1 = session.writeTransaction();
        tx1.close();
        Transaction tx2 = session.writeTransaction();
        assertNotEquals(tx1, tx2);
    }

    /**
     * It is not possible to have multiple transactions per thread.
     */
    @Test
    public void tryingToOpenTwoTransactionsInSameThread_throwsException() {
        Transaction tx1 = session.writeTransaction();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("A transaction is already open on this thread for graph [" + session.keyspace() + "]. Close the current transaction before opening a new one in the same thread.");
        Transaction tx2 = session.writeTransaction();
    }


    /**
     * Transactions are thread bound, it's not possible to share the same transaction between multiple threads
     */
    @Test
    public void sharingSameTransactionInDifferentThread_transactionIsNotUsable() throws InterruptedException {
        Transaction tx1 = session.writeTransaction();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(() -> {
                SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            }).get();
        } catch (ExecutionException e) {
            Throwable transactionException = e.getCause();
            assertThat(transactionException, instanceOf(TransactionException.class));
            assertEquals("The transaction is no longer on the thread it was spawned on, this is not allowed", transactionException.getMessage());
        }
    }

    /**
     * A session can be shared between multiple threads so that each thread can use its own local transaction.
     */
    @Test
    public void sessionOpeningTransactionsInDifferentThreads_transactionsAreUsable() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            Transaction tx1 = session.writeTransaction();
            SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            assertEquals("thing", concept.label().toString());
            tx1.close();
        }).get();
        executor.submit(() -> {
            Transaction tx1 = session.writeTransaction();
            SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            assertEquals("thing", concept.label().toString());
            tx1.close();
        }).get();
    }


    /**
     * Using different sessions in different threads it should be possible to access the same keyspace.
     */
    @Test
    public void sessionsInDifferentThreadsShouldBeAbleToAccessSameKeyspace() throws ExecutionException, InterruptedException {
        Transaction tx1 = session.writeTransaction();
        tx1.putEntityType("person");
        tx1.commit();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(() -> {
            Session localSession = server.sessionFactory().session(session.keyspace());
            Transaction tx2 = localSession.writeTransaction();
            SchemaConcept concept = tx2.getSchemaConcept(Label.of("person"));
            assertEquals("person", concept.label().toString());
            tx2.close();
            localSession.close();
        }).get();

        executor.submit(() -> {
            Session localSession = server.sessionFactory().session(session.keyspace());
            Transaction tx2 = localSession.writeTransaction();
            SchemaConcept concept = tx2.getSchemaConcept(Label.of("person"));
            assertEquals("person", concept.label().toString());
            tx2.close();
            localSession.close();
        }).get();
    }


    @Test
    public void whenClosingSession_transactionIsAlsoClosed() {
        Session localSession = server.sessionFactory().session(new KeyspaceImpl("test"));
        Transaction tx1 = localSession.writeTransaction();
        assertTrue(tx1.isOpen());
        localSession.close();
        assertFalse(tx1.isOpen());
    }

    @Test
    public void whenClosingSession_tryingToUseTransactionThrowsException() {
        Session localSession = server.sessionFactory().session(new KeyspaceImpl("test"));
        Transaction tx1 = localSession.writeTransaction();
        assertTrue(tx1.isOpen());
        localSession.close();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("The session for graph [test] is closed. Create a new session to interact with the graph.");
        SchemaConcept thing = tx1.getSchemaConcept(Label.of("thing"));
    }

    /**
     * Once a session it's closed it should not be possible to use it to get new transactions.
     */
    @Test
    public void whenSessionIsClosed_itIsNotPossibleToCreateNewTransactions(){
        session.close();
        expectedException.expect(SessionException.class);
        expectedException.expectMessage("The session for graph [" + session.keyspace() + "] is closed. Create a new session to interact with the graph.");
        Transaction tx1 = session.writeTransaction();

        SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
        assertEquals("thing", concept.label().toString());
    }

    @Test
    public void whenTransactionIsClosed_notUsable(){
        Transaction tx1 = session.writeTransaction();
        tx1.close();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("The transaction for keyspace [" + session.keyspace() + "] is closed.");
        SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
        assertEquals("thing", concept.label().toString());
    }

    @Test
    public void transactionRead_checkMutationsAllowedThrows(){
        TransactionImpl tx1 = (TransactionImpl) session.readTransaction();
        expectedException.expect(TransactionException.class);
        tx1.checkMutationAllowed();
        tx1.close();
        TransactionImpl tx2 = (TransactionImpl) session.writeTransaction();
        tx2.checkMutationAllowed();
        tx2.close();
        TransactionImpl tx3 = (TransactionImpl) session.readTransaction();
        expectedException.expect(TransactionException.class);
        tx3.checkMutationAllowed();
        tx3.close();
    }
}
