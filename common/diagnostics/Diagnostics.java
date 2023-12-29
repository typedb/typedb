/*
 * Copyright (C) 2023 Vaticle
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

package com.vaticle.typedb.core.common.diagnostics;

import com.vaticle.typedb.common.collection.Pair;
import io.sentry.ITransaction;
import io.sentry.NoOpTransaction;
import io.sentry.Sentry;
import io.sentry.TransactionContext;
import io.sentry.protocol.User;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;

public class Diagnostics {

    public static void initialise(String serverID, String distributionName, String version, String diagnosticsURI) {
        Sentry.init(options -> {
            options.setDsn(diagnosticsURI);
            options.setEnableTracing(true);
            options.setSendDefaultPii(false);
            options.setRelease(releaseName(distributionName, version));
        });
        User user = new User();
        user.setUsername(serverID);
        Sentry.setUser(user);
    }

    private static String releaseName(String distributionName, String version) {
        return distributionName + "@" + version;
    }

    public static ScheduledDiagnosticProvider scheduledProvider(long initialDelayMillis, long delayMillis, String name,
                                                                String operation, @Nullable String description) {
        return new ScheduledDiagnosticProvider(initialDelayMillis, delayMillis, transactionContext(name, operation, description));
    }

    public static ScheduledFuture<?> scheduledRunner(long initialDelayMillis, long delayMillis, String name, String operation,
                                                     @Nullable String description, Consumer<TransactionContext> run,
                                                     ScheduledThreadPoolExecutor executor) {
        TransactionContext transactionContext = transactionContext(name, operation, description);
        return executor.scheduleWithFixedDelay(
                () -> run.accept(transactionContext),
                initialDelayMillis, delayMillis, TimeUnit.MILLISECONDS
        );
    }

    private static TransactionContext transactionContext(String name, String operation, @Nullable String description) {
        TransactionContext context = new TransactionContext(name, operation);
        if (description != null) context.setDescription(description);
        return context;
    }

    /**
     * Poll-based scheduled diagnostics provider.
     * Given an initial delay and subsequent period, many threads can compete to get the 'real' diagnostic
     * transaction in the current time window. Only 1 'real' diagnostic transaction is used per time window,
     * and the remainder receive a No-op diagnostic transaction.
     */
    public static class ScheduledDiagnosticProvider {

        private final long initialDelayMillis;
        private final long delayMillis;
        private final TransactionContext context;
        private final long initialTime;
        private final AtomicLong lastWindow;

        private ScheduledDiagnosticProvider(long initialDelayMillis, long delayMillis, TransactionContext transactionContext) {
            this.initialDelayMillis = initialDelayMillis;
            this.delayMillis = delayMillis;
            this.context = transactionContext;
            this.initialTime = System.currentTimeMillis();
            this.lastWindow = new AtomicLong(-1); // last consumed delay windows after initial delay window
        }

        /**
         * Fetch a diagnostic transaction which is a no-op if the time window has already been taken previously.
         *
         * @return A real or no-op diagnostic transaction
         */
        public ITransaction get(BiFunction<ITransaction, Long, ITransaction> mayTransform) {
            long time = System.currentTimeMillis();
            ITransaction txn;
            long timeSinceLast;
            if (time < initialTime + initialDelayMillis) {
                // initial delay window
                txn = NoOpTransaction.getInstance();
                timeSinceLast = time - initialTime;
            } else {
                // number of current delay window since initial delay window ended
                long currentWindow = (time - (initialTime + initialDelayMillis)) / delayMillis;
                long lastWindowValue = this.lastWindow.get();

                // if the current window is equal to the last window (or last window has moved forward by another thread)
                // then the current thread should not sample
                if (currentWindow <= lastWindowValue) {
                    txn = NoOpTransaction.getInstance();
                    timeSinceLast = 0;
                } else {
                    // one thread will be allowed to populate the lastWindow with the current one
                    if (this.lastWindow.compareAndSet(lastWindowValue, currentWindow)) {
                        txn = Sentry.startTransaction(context);
                        timeSinceLast = (currentWindow - lastWindowValue) * delayMillis;
                    } else {
                        txn = NoOpTransaction.getInstance();
                        timeSinceLast = 0;
                    }
                }
            }
            if (!txn.isNoOp()) return mayTransform.apply(txn, timeSinceLast);
            else return txn;
        }
    }
}
