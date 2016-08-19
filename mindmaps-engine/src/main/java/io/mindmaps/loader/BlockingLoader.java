/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.loader;

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Var;
import io.mindmaps.postprocessing.Cache;
import io.mindmaps.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class BlockingLoader {

/**
 * RESTLoader that submits tasks to locally running engine and performs basic load balancing.
 */
public class BlockingLoader extends Loader {

    private final Logger LOG = LoggerFactory.getLogger(BlockingLoader.class);

    private ExecutorService executor;
    private Cache cache;
    private static Semaphore transactionsSemaphore;
    private static int repeatCommits;
    private String graphName;

    public BlockingLoader(String graphNameInit) {

        ConfigProperties prop = ConfigProperties.getInstance();
        numThreads = prop.getPropertyAsInt(ConfigProperties.NUM_THREADS_PROPERTY);
        batchSize = prop.getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY);
        repeatCommits = prop.getPropertyAsInt(ConfigProperties.LOADER_REPEAT_COMMITS);
        graphName = graphNameInit;
        cache = Cache.getInstance();
        executor = Executors.newFixedThreadPool(numThreads);
        transactionsSemaphore = new Semaphore(numThreads * 3);
        batch = new HashSet<>();
    }

    public void setExecutorSize(int size) {
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            executor = Executors.newFixedThreadPool(size);
        }
    }

    protected void submitBatch(Collection<Var> vars) {
        try {
            transactionsSemaphore.acquire();
            Collection<Var> deepCopy = new ArrayList<>(vars);
            executor.submit(() -> loadData(graphName, deepCopy));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void waitToFinish() {
        flush();

        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
            LOG.info("All tasks submitted, waiting for termination..");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            LOG.info("ALL TASKS DONE!");
            executor = Executors.newFixedThreadPool(numThreads);
        }
    }

    private void loadData(String name, Collection<Var> batch) {

        for (int i = 0; i < repeatCommits; i++) {
            MindmapsTransactionImpl transaction = (MindmapsTransactionImpl) GraphFactory.getInstance().getGraphBatchLoading(name).getTransaction();
            try {

                insert(batch).withTransaction(transaction).execute();
                transaction.commit();
                cache.addJobCasting(graphName, transaction.getModifiedCastingIds());
                transactionsSemaphore.release();
                return;

            } catch (MindmapsValidationException e) {
                //If it's a validation exception there is no point in re-trying
                LOG.error(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
                transactionsSemaphore.release();
            } catch (Exception e) {
                //If it's not a validation exception we need to remain in the for loop
                handleError(e, 1);
            } finally {
                try {
                    transaction.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        transactionsSemaphore.release();
        LOG.error(ErrorMessage.FAILED_TRANSACTION.getMessage(repeatCommits));

        //TODO: set a proper file appender to log all exceptions to file.
    }

    private void handleError(Exception e, int i) {
        LOG.error("Caught exception ", e);
        e.printStackTrace();

        try {
            Thread.sleep((i + 2) * 1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
    }
}
