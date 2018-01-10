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
 *
 */

package ai.grakn.engine.rpc;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.util.CommonUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents a transaction and a thread. All operations on the transaction are executed within the thread.
 */
class TxThread implements AutoCloseable {

    private final ExecutorService executor;
    private final GraknTx tx;

    private TxThread(ExecutorService executor, GraknTx tx) {
        this.executor = executor;
        this.tx = tx;
    }

    public static TxThread open(EngineGraknTxFactory txFactory, Keyspace keyspace) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        GraknTx tx = runAndAssumeSafe(executor, () -> txFactory.tx(keyspace, GraknTxType.WRITE));
        return new TxThread(executor, tx);
    }

    public void run(Consumer<GraknTx> task) {
        run(tx -> {
            task.accept(tx);
            return null;
        });
    }

    public <T> T run(Function<GraknTx, T> task) {
        return runAndAssumeSafe(executor, () -> task.apply(tx));
    }

    @Override
    public void close() {
        run(GraknTx::close);
        executor.shutdown();
    }

    private static <T> T runAndAssumeSafe(ExecutorService executor, Supplier<T> task) {
        Future<T> future = executor.submit(task::get);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw CommonUtil.unreachableStatement(
                    "This should never be interrupted, cancelled or throw a checked exception", e
            );
        }
    }
}
