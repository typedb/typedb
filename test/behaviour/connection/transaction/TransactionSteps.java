/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.test.behaviour.connection.transaction;

import grakn.core.Grakn;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.test.Util.assertThrows;
import static grakn.core.common.test.Util.assertThrowsWithMessage;
import static grakn.core.test.behaviour.connection.ConnectionSteps.THREAD_POOL_SIZE;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessions;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsParallel;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsParallelToTransactionsParallel;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsToTransactions;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsToTransactionsParallel;
import static grakn.core.test.behaviour.connection.ConnectionSteps.threadPool;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionSteps {

    // =============================================//
    // sequential sessions, sequential transactions //
    // =============================================//

    @When("(for each )session(,) open(s) transaction(s) of type: {transaction_type}")
    public void session_opens_transaction_of_type(Arguments.Transaction.Type type) {
        for_each_session_open_transactions_of_type(list(type));
    }

    @When("(for each )session(,) open transaction(s) of type:")
    public void for_each_session_open_transactions_of_type(List<Arguments.Transaction.Type> types) {
        for (Grakn.Session session : sessions) {
            List<Grakn.Transaction> transactions = new ArrayList<>();
            for (Arguments.Transaction.Type type : types) {
                Grakn.Transaction transaction = session.transaction(type, (new Options.Transaction()));
                transactions.add(transaction);
            }
            sessionsToTransactions.put(session, transactions);
        }
    }

    @When("(for each )session(,) open transaction(s) of type; throws exception: {transaction_type}")
    public void for_each_session_open_transactions_of_type_throws_exception(Arguments.Transaction.Type type) {
        for_each_session_open_transactions_of_type_throws_exception(list(type));
    }

    @Then("(for each )session(,) open transaction(s) of type; throws exception")
    public void for_each_session_open_transactions_of_type_throws_exception(List<Arguments.Transaction.Type> types) {
        for (Grakn.Session session : sessions) {
            for (Arguments.Transaction.Type type : types) {
                assertThrows(() -> session.transaction(type));
            }
        }
    }

    @Then("(for each )session(,) transaction(s) is/are null: {bool}")
    public void for_each_session_transactions_are_null(boolean isNull) {
        for_each_session_transactions_are(transaction -> assertEquals(isNull, isNull(transaction)));
    }

    @Then("(for each )session(,) transaction(s) is/are open: {bool}")
    public void for_each_session_transactions_are_open(boolean isOpen) {
        for_each_session_transactions_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
    }

    @Then("transaction commits")
    public void transaction_commits() {
        sessionsToTransactions.get(sessions.get(0)).get(0).commit();
    }

    @Then("transaction commits; throws exception")
    public void transaction_commits_throws_exception() {
        assertThrows(() -> sessionsToTransactions.get(sessions.get(0)).get(0).commit());
    }

    @Then("transaction commits; throws exception containing {string}")
    public void transaction_commits_throws_exception(String exception) {
        assertThrowsWithMessage(() -> sessionsToTransactions.get(sessions.get(0)).get(0).commit(), exception);
    }

    @Then("(for each )session(,) transaction(s) commit(s)")
    public void for_each_session_transactions_commit() {
        for (Grakn.Session session : sessions) {
            for (Grakn.Transaction transaction : sessionsToTransactions.get(session)) {
                transaction.commit();
            }
        }
    }

    @Then("(for each )session(,) transaction(s) commit(s); throws exception")
    public void for_each_session_transactions_commits_throws_exception() {
        for (Grakn.Session session : sessions) {
            for (Grakn.Transaction transaction : sessionsToTransactions.get(session)) {
                assertThrows(transaction::commit);
            }
        }
    }

    @Then("(for each )session(,) transaction close(s)")
    public void for_each_session_transaction_closes() {
        for (Grakn.Session session : sessions) {
            for (Grakn.Transaction transaction : sessionsToTransactions.get(session)) {
                transaction.close();
            }
        }
    }

    private void for_each_session_transactions_are(Consumer<Grakn.Transaction> assertion) {
        for (Grakn.Session session : sessions) {
            for (Grakn.Transaction transaction : sessionsToTransactions.get(session)) {
                assertion.accept(transaction);
            }
        }
    }

    @Then("(for each )session(,) transaction(s) has/have type: {transaction_type}")
    public void for_each_session_transactions_have_type(Arguments.Transaction.Type type) {
        for_each_session_transactions_have_type(list(type));
    }

    @Then("(for each )session(,) transaction(s) has/have type:")
    public void for_each_session_transactions_have_type(List<Arguments.Transaction.Type> types) {
        for (Grakn.Session session : sessions) {
            List<Grakn.Transaction> transactions = sessionsToTransactions.get(session);
            assertEquals(types.size(), transactions.size());

            Iterator<Arguments.Transaction.Type> typesIterator = types.iterator();
            Iterator<Grakn.Transaction> transactionIterator = transactions.iterator();
            while (typesIterator.hasNext()) {
                assertEquals(typesIterator.next(), transactionIterator.next().type());
            }
        }
    }

    // ===========================================//
    // sequential sessions, parallel transactions //
    // ===========================================//

    @When("(for each )session(,) open transaction(s) in parallel of type:")
    public void for_each_session_open_transactions_in_parallel_of_type(List<Arguments.Transaction.Type> types) {
        assertTrue(THREAD_POOL_SIZE >= types.size());
        for (Grakn.Session session : sessions) {
            List<CompletableFuture<Grakn.Transaction>> transactionsParallel = new ArrayList<>();
            for (Arguments.Transaction.Type type : types) {
                transactionsParallel.add(CompletableFuture.supplyAsync(() -> session.transaction(type), threadPool));
            }
            sessionsToTransactionsParallel.put(session, transactionsParallel);
        }
    }

    @Then("(for each )session(,) transactions in parallel are null: {bool}")
    public void for_each_session_transactions_in_parallel_are_null(boolean isNull) {
        for_each_session_transactions_in_parallel_are(transaction -> assertEquals(isNull, isNull(transaction)));
    }

    @Then("(for each )session(,) transactions in parallel are open: {bool}")
    public void for_each_session_transactions_in_parallel_are_open(boolean isOpen) {
        for_each_session_transactions_in_parallel_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
    }

    private void for_each_session_transactions_in_parallel_are(Consumer<Grakn.Transaction> assertion) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (Grakn.Session session : sessions) {
            for (CompletableFuture<Grakn.Transaction> futureTransaction :
                    sessionsToTransactionsParallel.get(session)) {

                assertions.add(futureTransaction.thenApply(transaction -> {
                    assertion.accept(transaction);
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
    }

    @Then("(for each )session(,) transactions in parallel have type:")
    public void for_each_session_transactions_in_parallel_have_type(List<Arguments.Transaction.Type> types) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (Grakn.Session session : sessions) {
            List<CompletableFuture<Grakn.Transaction>> futureTxs =
                    sessionsToTransactionsParallel.get(session);

            assertEquals(types.size(), futureTxs.size());

            Iterator<Arguments.Transaction.Type> typesIter = types.iterator();
            Iterator<CompletableFuture<Grakn.Transaction>> futureTxsIter = futureTxs.iterator();

            while (typesIter.hasNext()) {
                Arguments.Transaction.Type type = typesIter.next();
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

    private void for_each_session_in_parallel_transactions_in_parallel_are(Consumer<Grakn.Transaction> assertion) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (CompletableFuture<Grakn.Session> futureSession : sessionsParallel) {
            for (CompletableFuture<Grakn.Transaction> futureTransaction : sessionsParallelToTransactionsParallel.get(futureSession)) {
                assertions.add(futureTransaction.thenApply(transaction -> {
                    assertion.accept(transaction);
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
    }
}
