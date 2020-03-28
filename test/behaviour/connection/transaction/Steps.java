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

package hypergraph.test.behaviour.connection.transaction;

import hypergraph.Hypergraph;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static grakn.common.util.Collections.list;
import static hypergraph.test.behaviour.connection.ConnectionSteps.THREAD_POOL_SIZE;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessions;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessionsParallel;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessionsParallelToTransactionsParallel;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessionsToTransactions;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessionsToTransactionsParallel;
import static hypergraph.test.behaviour.connection.ConnectionSteps.threadPool;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Steps {

    // =============================================//
    // sequential sessions, sequential transactions //
    // =============================================//

    @When("session open transaction of type: {transaction_type}")
    public void session_opens_transaction_of_type(Hypergraph.Transaction.Type type) {
        for_each_session_open_transactions_of_type(list(type));
    }

    @When("for each session, open transaction(s) of type:")
    public void for_each_session_open_transactions_of_type(List<Hypergraph.Transaction.Type> types) {
        for (Hypergraph.Session session : sessions) {
            List<Hypergraph.Transaction> transactions = new ArrayList<>();
            for (Hypergraph.Transaction.Type type : types) {
                Hypergraph.Transaction transaction = session.transaction(type);
                transactions.add(transaction);
            }
            sessionsToTransactions.put(session, transactions);
        }
    }

    @Then("for each session, transaction(s) is/are null: {bool}")
    public void for_each_session_transactions_are_null(boolean isNull) {
        for_each_session_transactions_are(transaction -> assertEquals(isNull, isNull(transaction)));
    }

    @Then("for each session, transaction(s) is/are open: {bool}")
    public void for_each_session_transactions_are_open(boolean isOpen) {
        for_each_session_transactions_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
    }

    @Then("for each session, transaction commits successfully: {bool}")
    public void for_each_session_transaction_commit(boolean successfully) {
        for (Hypergraph.Session session : sessions) {
            for (Hypergraph.Transaction transaction : sessionsToTransactions.get(session)) {
                boolean hasException = false;
                try {
                    transaction.commit();
                } catch (RuntimeException commitException) {
                    hasException = true;
                }
                if (successfully) {
                    assertFalse(hasException);
                } else {
                    assertTrue(hasException);
                }
            }
        }
    }

    @Then("for each session, transaction close")
    public void for_each_session_transaction_close() {
        for (Hypergraph.Session session : sessions) {
            for (Hypergraph.Transaction transaction : sessionsToTransactions.get(session)) {
                transaction.close();
            }
        }
    }

    private void for_each_session_transactions_are(Consumer<Hypergraph.Transaction> assertion) {
        for (Hypergraph.Session session : sessions) {
            for (Hypergraph.Transaction transaction : sessionsToTransactions.get(session)) {
                assertion.accept(transaction);
            }
        }
    }

    @Then("for each session, transaction(s) has/have type:")
    public void for_each_session_transactions_have_type(List<Hypergraph.Transaction.Type> types) {
        for (Hypergraph.Session session : sessions) {
            List<Hypergraph.Transaction> transactions = sessionsToTransactions.get(session);
            assertEquals(types.size(), transactions.size());

            Iterator<Hypergraph.Transaction.Type> typesIterator = types.iterator();
            Iterator<Hypergraph.Transaction> transactionIterator = transactions.iterator();
            while (typesIterator.hasNext()) {
                assertEquals(typesIterator.next(), transactionIterator.next().type());
            }
        }
    }

    // ===========================================//
    // sequential sessions, parallel transactions //
    // ===========================================//

    @When("for each session, open transaction(s) in parallel of type:")
    public void for_each_session_open_transactions_in_parallel_of_type(List<Hypergraph.Transaction.Type> types) {
        assertTrue(THREAD_POOL_SIZE >= types.size());
        for (Hypergraph.Session session : sessions) {
            List<CompletableFuture<Hypergraph.Transaction>> transactionsParallel = new ArrayList<>();
            for (Hypergraph.Transaction.Type type : types) {
                transactionsParallel.add(CompletableFuture.supplyAsync(() -> session.transaction(type), threadPool));
            }
            sessionsToTransactionsParallel.put(session, transactionsParallel);
        }
    }

    @Then("for each session, transactions in parallel are null: {bool}")
    public void for_each_session_transactions_in_parallel_are_null(boolean isNull) {
        for_each_session_transactions_in_parallel_are(transaction -> assertEquals(isNull, isNull(transaction)));
    }

    @Then("for each session, transactions in parallel are open: {bool}")
    public void for_each_session_transactions_in_parallel_are_open(boolean isOpen) {
        for_each_session_transactions_in_parallel_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
    }

    private void for_each_session_transactions_in_parallel_are(Consumer<Hypergraph.Transaction> assertion) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (Hypergraph.Session session : sessions) {
            for (CompletableFuture<Hypergraph.Transaction> futureTransaction :
                    sessionsToTransactionsParallel.get(session)) {

                assertions.add(futureTransaction.thenApply(transaction -> {
                    assertion.accept(transaction);
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
    }

    @Then("for each session, transactions in parallel have type:")
    public void for_each_session_transactions_in_parallel_have_type(List<Hypergraph.Transaction.Type> types) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (Hypergraph.Session session : sessions) {
            List<CompletableFuture<Hypergraph.Transaction>> futureTxs =
                    sessionsToTransactionsParallel.get(session);

            assertEquals(types.size(), futureTxs.size());

            Iterator<Hypergraph.Transaction.Type> typesIter = types.iterator();
            Iterator<CompletableFuture<Hypergraph.Transaction>> futureTxsIter = futureTxs.iterator();

            while (typesIter.hasNext()) {
                Hypergraph.Transaction.Type type = typesIter.next();
                futureTxsIter.next().thenApplyAsync(tx -> {
                    assertEquals(type, tx.type());
                    return null;
                });
            }
        }

        CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
    }

    // =========================================//
    // parallel sessions, parallel transactions //
    // =========================================//

    @Then("for each session in parallel, transactions in parallel are null: {bool}")
    public void for_each_session_in_parallel_transactions_in_parallel_are_null(boolean isNull) {
        for_each_session_in_parallel_transactions_in_parallel_are(transaction -> assertEquals(isNull, isNull(transaction)));
    }

    @Then("for each session in parallel, transactions in parallel are open: {bool}")
    public void for_each_session_in_parallel_transactions_in_parallel_are_open(boolean isOpen) {
        for_each_session_in_parallel_transactions_in_parallel_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
    }

    private void for_each_session_in_parallel_transactions_in_parallel_are(Consumer<Hypergraph.Transaction> assertion) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (CompletableFuture<Hypergraph.Session> futureSession : sessionsParallel) {
            for (CompletableFuture<Hypergraph.Transaction> futureTransaction : sessionsParallelToTransactionsParallel.get(futureSession)) {
                assertions.add(futureTransaction.thenApply(transaction -> {
                    assertion.accept(transaction);
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
    }
}
