// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.diskstorage.util;

import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.PermanentBackendException;
import grakn.core.graph.diskstorage.TemporaryBackendException;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;

public class BackendOperation {

    private static final Logger LOG = LoggerFactory.getLogger(BackendOperation.class);
    private static final Random random = new Random();

    private static final Duration BASE_REATTEMPT_TIME = Duration.ofMillis(50);
    private static final double PERTURBATION_PERCENTAGE = 0.2;

    private static Duration pertubTime(Duration duration) {
        return duration.dividedBy((int) (2.0 / (1 + (random.nextDouble() * 2 - 1.0) * PERTURBATION_PERCENTAGE)));
    }

    public static <V> V execute(Callable<V> exe, Duration totalWaitTime) throws JanusGraphException {
        try {
            return executeDirect(exe, totalWaitTime);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not execute operation due to backend exception", e);
        }
    }

    public static <V> V executeDirect(Callable<V> exe, Duration totalWaitTime) throws BackendException {
        long maxTime = System.currentTimeMillis() + totalWaitTime.toMillis();
        Duration waitTime = pertubTime(BASE_REATTEMPT_TIME);
        BackendException lastException;
        while (true) {
            try {
                return exe.call();
            } catch (Throwable e) {
                //Find inner-most StorageException
                Throwable ex = e;
                BackendException storeEx = null;
                do {
                    if (ex instanceof BackendException) storeEx = (BackendException) ex;
                } while ((ex = ex.getCause()) != null);


                if (storeEx instanceof TemporaryBackendException) {
                    lastException = storeEx; // if this is a temporary exception, don't throw immediately but retry for a totalWaitTime time before throwing
                } else if (e instanceof BackendException) {
                    throw (BackendException) e;
                } else {
                    throw new PermanentBackendException("Permanent exception while executing backend operation " + exe.toString(), e);
                }
            }
            //Wait and retry
            if (System.currentTimeMillis() + waitTime.toMillis() < maxTime) {
                LOG.info("Temporary exception during backend operation [" + exe.toString() + "]. Attempting backoff retry.", lastException);
                try {
                    Thread.sleep(waitTime.toMillis());
                } catch (InterruptedException r) {
                    // added thread interrupt signal to support traversal interruption
                    Thread.currentThread().interrupt();
                    throw new PermanentBackendException("Interrupted while waiting to retry failed backend operation", r);
                }
            } else {
                break; // TODO: super lol to while(true) and else break;, refactor this beauty at some point
            }
            waitTime = pertubTime(waitTime.multipliedBy(2));
        }
        throw new TemporaryBackendException("Could not successfully complete backend operation due to repeated temporary exceptions after " + totalWaitTime, lastException);
    }

    public static <R> R execute(Transactional<R> exe, TransactionalProvider provider, TimestampProvider times) throws BackendException {
        StoreTransaction txh = null;
        try {
            txh = provider.openTx();
            if (!txh.getConfiguration().hasCommitTime()) txh.getConfiguration().setCommitTime(times.getTime());
            return exe.call(txh);
        } catch (BackendException e) {
            if (txh != null) txh.rollback();
            txh = null;
            throw e;
        } finally {
            if (txh != null) txh.commit();
        }
    }

    /**
     * Method used by KCVSLog and KCVSConfiguration to run transactional operations on DB
     *
     * @param exe      Transactional operation on the Database that needs to happen inside a transaction
     * @param provider Transactions provider, will provide transaction on which execute the above operation
     * @param times    Provider of timestamp, it is used to get the Time (Timestamp) to set on the transaction which will execute the operation
     * @param maxTime  maxTime for which an operation will be retried, this is because sometimes the Database might need some time to startup or reply to havy workload
     * @throws JanusGraphException if the operation fails
     */
    public static <R> R execute(Transactional<R> exe, TransactionalProvider provider, TimestampProvider times, Duration maxTime) throws JanusGraphException {
        return execute(new Callable<R>() {
            @Override
            public R call() throws Exception {
                return execute(exe, provider, times);
            }

            @Override
            public String toString() {
                return exe.toString();
            }
        }, maxTime);
    }


    public interface Transactional<R> {
        R call(StoreTransaction txh) throws BackendException;
    }

    public interface TransactionalProvider {

        StoreTransaction openTx() throws BackendException;

        void close() throws BackendException;

    }

}
