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

package grakn.core.test.behaviour.connection.transaction;

import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static grakn.common.util.Collections.list;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessions;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsParallel;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsParallelToTransactionsParallel;
import static grakn.core.test.behaviour.connection.ConnectionSteps.sessionsToTransactionsParallel;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;

public class TransactionSteps {

    // =============================================//
    // sequential sessions, sequential transactions //
    // =============================================//

    @When("session open transaction of type: {transaction_type}")
    public void session_opens_transaction_of_type(Transaction.Type type) {
        for_each_session_open_transactions_of_type(list(type));
    }

    @When("for each session, open transaction(s) of type:")
    public void for_each_session_open_transactions_of_type(List<Transaction.Type> types) {
//        for (Session session : sessions) {
//            List<Transaction> transactions = new ArrayList<>();
//            for (Transaction.Type type : types) {
//                Transaction transaction = session.transaction(type);
//                transactions.add(transaction);
//            }
//            sessionsToTransactions.put(session, transactions);
//        }

        // cannot open multiple tx per session when not parallel in core
    }

    @Then("for each session, transaction(s) is/are null: {bool}")
    public void for_each_session_transactions_are_null(boolean isNull) {
//        for_each_session_transactions_are(transaction -> assertEquals(isNull, isNull(transaction)));
    }

    @Then("for each session, transaction(s) is/are open: {bool}")
    public void for_each_session_transactions_are_open(boolean isOpen) {
//        for_each_session_transactions_are(transaction -> assertEquals(isOpen, transaction.isOpen()));
    }

    @Then("transaction commits")
    public void transaction_commits() {
//        sessionsToTransactions.get(sessions.get(0)).get(0).commit();
    }

    @Then("for each session, transaction(s) commit(s)")
    public void for_each_session_transactions_commit() {
//        for (Session session : sessions) {
//            for (Transaction transaction : sessionsToTransactions.get(session)) {
//                transaction.commit();
//            }
//        }
    }

    @Then("for each session, transaction(s) commit(s) successfully: {bool}")
    public void for_each_session_transactions_commit(boolean successfully) {
//        for (Session session : sessions) {
//            for (Transaction transaction : sessionsToTransactions.get(session)) {
//                boolean hasException = false;
//                try {
//                    transaction.commit();
//                } catch (RuntimeException commitException) {
//                    hasException = true;
//                }
//                assertEquals(successfully, !hasException);
//            }
//        }
    }

    @Then("for each session, transaction close")
    public void for_each_session_transaction_close() {
//        for (Session session : sessions) {
//            for (Transaction transaction : sessionsToTransactions.get(session)) {
//                transaction.close();
//            }
//        }
    }

    private void for_each_session_transactions_are(Consumer<Transaction> assertion) {
//        for (Session session : sessions) {
//            for (Transaction transaction : sessionsToTransactions.get(session)) {
//                assertion.accept(transaction);
//            }
//        }
    }

    @Then("for each session, transaction(s) has/have type:")
    public void for_each_session_transactions_have_type(List<Transaction.Type> types) {
//        for (Session session : sessions) {
//            List<Transaction> transactions = sessionsToTransactions.get(session);
//            assertEquals(types.size(), transactions.size());
//
//            Iterator<Transaction.Type> typesIterator = types.iterator();
//            Iterator<Transaction> transactionIterator = transactions.iterator();
//            while (typesIterator.hasNext()) {
//                assertEquals(typesIterator.next(), transactionIterator.next().type());
//            }
//        }
    }

    // ===========================================//
    // sequential sessions, parallel transactions //
    // ===========================================//

    @When("for each session, open transaction(s) in parallel of type:")
    public void for_each_session_open_transactions_in_parallel_of_type(List<Transaction.Type> types) throws InterruptedException {
        for (Session session : sessions) {
            List<CompletableFuture<Transaction>> transactionsParallel = new ArrayList<>();
            for (Transaction.Type type : types) {
                transactionsParallel.add(
                        CompletableFuture.supplyAsync(() ->  session.transaction(type),
                        Executors.newSingleThreadExecutor())
                );
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

    private void for_each_session_transactions_in_parallel_are(Consumer<Transaction> assertion) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (Session session : sessions) {
            for (CompletableFuture<Transaction> futureTransaction :
                    sessionsToTransactionsParallel.get(session)) {

                assertions.add(futureTransaction.thenAccept(transaction -> assertion.accept(transaction)));
            }
        }
        CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
    }

    @Then("for each session, transactions in parallel have type:")
    public void for_each_session_transactions_in_parallel_have_type(List<Transaction.Type> types) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (Session session : sessions) {
            List<CompletableFuture<Transaction>> futureTxs =
                    sessionsToTransactionsParallel.get(session);

            assertEquals(types.size(), futureTxs.size());

            Iterator<Transaction.Type> typesIter = types.iterator();
            Iterator<CompletableFuture<Transaction>> futureTxsIter = futureTxs.iterator();

            while (typesIter.hasNext()) {
                Transaction.Type type = typesIter.next();
                assertions.add(
                        futureTxsIter.next().thenAccept(tx -> assertEquals(type, tx.type()))
                );
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

    private void for_each_session_in_parallel_transactions_in_parallel_are(Consumer<Transaction> assertion) {
        List<CompletableFuture<Void>> assertions = new ArrayList<>();
        for (CompletableFuture<Session> futureSession : sessionsParallel) {
            for (CompletableFuture<Transaction> futureTransaction : sessionsParallelToTransactionsParallel.get(futureSession)) {
                assertions.add(futureTransaction.thenAccept(assertion::accept));
            }
        }
        CompletableFuture.allOf(assertions.toArray(new CompletableFuture[0])).join();
    }
}
