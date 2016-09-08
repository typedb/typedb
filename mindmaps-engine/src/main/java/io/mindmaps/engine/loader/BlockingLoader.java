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

package io.mindmaps.engine.loader;

import io.mindmaps.util.ErrorMessage;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.engine.postprocessing.Cache;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.Var;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static io.mindmaps.graql.Graql.insert;


/**
 * RESTLoader that submits tasks to locally running engine and performs basic load balancing.
 */
public class BlockingLoader extends Loader {

    private ExecutorService executor;
    private Cache cache;
    private static Semaphore transactionsSemaphore;
    private static int repeatCommits;
    private String graphName;

    public BlockingLoader(String graphNameInit) {

        ConfigProperties prop = ConfigProperties.getInstance();
        threadsNumber = prop.getAvailableThreads();
        batchSize = prop.getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY);
        repeatCommits = prop.getPropertyAsInt(ConfigProperties.LOADER_REPEAT_COMMITS);
        graphName = graphNameInit;
        cache = Cache.getInstance();
        executor = Executors.newFixedThreadPool(threadsNumber);
        transactionsSemaphore = new Semaphore(threadsNumber * 3);
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Collection<Var> deepCopy = new ArrayList<>(vars);
        try {
            executor.submit(() -> loadData(graphName, deepCopy));
        } catch (Exception e) {
            e.printStackTrace();
            transactionsSemaphore.release();
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
            LOG.info("All tasks done!");
            executor = Executors.newFixedThreadPool(threadsNumber);
        }
    }

    private void loadData(String name, Collection<Var> batch) {

        try {
            for (int i = 0; i < repeatCommits; i++) {
                AbstractMindmapsGraph graph = (AbstractMindmapsGraph) GraphFactory.getInstance().getGraphBatchLoading(name);
                try {

                    insert(batch).withGraph(graph).execute();
                    graph.commit();
                    cache.addJobCasting(graphName, graph.getModifiedCastingIds());
                    return;

                } catch (MindmapsValidationException e) {
                    //If it's a validation exception there is no point in re-trying
                    LOG.error(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
                    return;
                } catch (Exception e) {
                    //If it's not a validation exception we need to remain in the for loop
                    handleError(e, 1);
                }
            }
        } finally {
            transactionsSemaphore.release();
        }
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
