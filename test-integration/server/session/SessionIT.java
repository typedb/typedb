package grakn.core.server.session;

import grakn.core.common.config.Config;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.keyspace.Keyspace;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static grakn.core.server.Transaction.Type.WRITE;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SessionIT {
    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private SessionImpl session;
    private Config config;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        config = server.config();
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
        Transaction tx1 = session.transaction(WRITE);
        tx1.close();
        Transaction tx2 = session.transaction(WRITE);
        assertNotEquals(tx1, tx2);
    }

    /**
     * It is not possible to have multiple transactions per thread.
     */
    @Test
    public void tryingToOpenTwoTransactionsInSameThread_throwsException() {
        Transaction tx1 = session.transaction(WRITE);
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("A transaction is already open on this thread for graph [" + session.keyspace() + "]. Close the current transaction before opening a new one in the same thread.");
        Transaction tx2 = session.transaction(WRITE);
    }


    /**
     * Transactions are thread bound, it's not possible to share the same transaction between multiple threads
     */
    @Test
    public void sharingSameTransactionInDifferentThread_transactionIsNotUsable() throws InterruptedException {
        Transaction tx1 = session.transaction(WRITE);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(() -> {
                SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            }).get();
        } catch (ExecutionException e) {
            Throwable transactionException = e.getCause();
            assertThat(transactionException, instanceOf(TransactionException.class));
            assertEquals("The transaction for keyspace [" + session.keyspace() + "] is closed. Use the session to get a new transaction for the graph.", transactionException.getMessage());
        }
    }

    /**
     * A session can be shared between multiple threads so that each thread can use its own local transaction.
     */
    @Test
    public void sessionOpeningTransactionsInDifferentThreads_transactionsAreUsable() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            Transaction tx1 = session.transaction(WRITE);
            SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
            assertEquals("thing", concept.label().toString());
            tx1.close();
        }).get();
        executor.submit(() -> {
            Transaction tx1 = session.transaction(WRITE);
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
        Transaction tx1 = session.transaction(WRITE);
        tx1.putEntityType("person");
        tx1.commit();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(() -> {
            Transaction tx2 = new SessionImpl(session.keyspace(), config).transaction(WRITE);
            SchemaConcept concept = tx2.getSchemaConcept(Label.of("person"));
            assertEquals("person", concept.label().toString());
        }).get();
        executor.submit(() -> {
            Transaction tx2 = new SessionImpl(session.keyspace(), config).transaction(WRITE);
            SchemaConcept concept = tx2.getSchemaConcept(Label.of("person"));
            assertEquals("person", concept.label().toString());
        }).get();
    }


    @Test
    public void whenClosingSession_transactionIsAlsoClosed() {
        SessionImpl localSession = new SessionImpl(Keyspace.of("test"), config);
        Transaction tx1 = localSession.transaction(WRITE);
        assertFalse(tx1.isClosed());
        localSession.close();
        assertTrue(tx1.isClosed());
    }

    /**
     * Once a session it's closed it should not be possible to use it to get new transactions.
     */
    @Test
    public void whenSessionIsClosed_itIsNotPossibleToCreateNewTransactions(){
        session.close();
        expectedException.expect(Exception.class);
        Transaction tx1 = session.transaction(WRITE);

        SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
        assertEquals("thing", concept.label().toString());
    }

    @Test
    public void whenTransactionIsClosed_notUsable(){
        Transaction tx1 = session.transaction(WRITE);
        tx1.close();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("The transaction for keyspace [" + session.keyspace() + "] is closed.");
        SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
        assertEquals("thing", concept.label().toString());
    }
}
