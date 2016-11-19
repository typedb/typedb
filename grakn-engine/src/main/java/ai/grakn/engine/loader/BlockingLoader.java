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
 */

package ai.grakn.engine.loader;

import ai.grakn.engine.postprocessing.Cache;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.GraknGraph;
import ai.grakn.graql.InsertQuery;
import ai.grakn.util.ErrorMessage;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.GraphFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * RESTLoader that submits tasks to locally running engine and performs basic load balancing.
 */
public class BlockingLoader extends Loader {

    private static ConfigProperties prop = ConfigProperties.getInstance();

    private static int repeatCommits = prop.getPropertyAsInt(ConfigProperties.LOADER_REPEAT_COMMITS);
    private static Cache cache = Cache.getInstance();

    private static Semaphore transactionsSemaphore;
    private ExecutorService executor;
    private String graphName;

    public BlockingLoader(String graphName) {
        setBatchSize(prop.getPropertyAsInt(ConfigProperties.BATCH_SIZE_PROPERTY));
        setThreadsNumber(prop.getAvailableThreads());
        initExecutor();
        initSemaphore();

        this.graphName = graphName;
    }

    public void setExecutorSize(int size){
        shutdownExecutor();
        setThreadsNumber(size);
        initExecutor();
        initSemaphore();
    }

    public void waitToFinish() {
        flush();
        shutdownExecutor();
        initExecutor();
    }

    /**
     *
     * @param queries
     */
    protected void sendQueriesToLoader(Collection<InsertQuery> queries) {
        try {
            transactionsSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Collection<InsertQuery> deepCopy = new ArrayList<>(queries);
        try {
            executor.submit(() -> insertQueriesInOneTransaction(graphName, deepCopy));
        } catch (Exception e) {
            LOG.error("Exception ",e);
            transactionsSemaphore.release();
        }
    }

    /**
     *
     * @param name
     * @param queries
     */
    private void insertQueriesInOneTransaction(String name, Collection<InsertQuery> queries) {

        try( GraknGraph graph = GraphFactory.getInstance().getGraphBatchLoading(name)){
            for (int i = 0; i < repeatCommits; i++) {
                try {

                    // execute each of the insert queries
                    queries.forEach(q -> q.withGraph(graph).execute());

                    // commit the transaction
                    graph.commit();

                    cache.addJobCasting(graphName, ((AbstractGraknGraph) graph).getModifiedCastingIds());
                    cache.addJobResource(graphName, ((AbstractGraknGraph) graph).getModifiedCastingIds());
                    return;
                } catch (GraknValidationException e) {
                    //If it's a validation exception there is no point in re-trying
                    LOG.error(ErrorMessage.FAILED_VALIDATION.getMessage(e.getMessage()));
                    return;
                } catch (Exception e) {
                    //If it's not a validation exception we need to remain in the for loop
                    handleError(e, 1);
                }
            }
        } catch (Throwable e){
            LOG.error(e.getMessage() + ErrorMessage.FAILED_TRANSACTION.getMessage(repeatCommits));
        } finally {
            transactionsSemaphore.release();
        }
    }

    private void shutdownExecutor(){
        if(executor == null){
            return;
        }

        try {
            executor.shutdown();

            LOG.info("All tasks submitted, waiting for termination..");
            boolean finished = executor.awaitTermination(5, TimeUnit.MINUTES);
            if(finished){
                LOG.info("All tasks done.");
            } else {
                LOG.warn("Loading exceeded timeout.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initExecutor(){
        executor = Executors.newFixedThreadPool(threadsNumber);
    }

    private void initSemaphore(){
        transactionsSemaphore = new Semaphore(threadsNumber * 3);
    }
}
