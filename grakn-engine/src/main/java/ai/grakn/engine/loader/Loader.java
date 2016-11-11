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

import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.admin.InsertQueryAdmin;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RESTLoader to perform bulk loading into the graph
 */
public abstract class Loader {

    protected AtomicInteger enqueuedJobs;
    protected AtomicInteger loadingJobs;
    protected AtomicInteger finishedJobs;
    protected AtomicInteger errorJobs;

    protected Collection<InsertQuery> queries;
    protected int batchSize;
    protected int threadsNumber;

    final Logger LOG = LoggerFactory.getLogger(Loader.class);


    public Loader(){
        enqueuedJobs = new AtomicInteger();
        loadingJobs = new AtomicInteger();
        errorJobs = new AtomicInteger();
        finishedJobs = new AtomicInteger();
        queries = new HashSet<>();
    }

    /**
     * Method to load data into the graph. Implementation depends on the type of the loader.
     */
    protected abstract void sendQueriesToLoader(Collection<InsertQuery> batch);

    /**
     * Wait for all loading to terminate.
     */
    public abstract void waitToFinish();

    /**
     * Add an insert query to the queue
     * @param query insert query to be executed
     */
    public void add(InsertQuery query){
        queries.add(query);
        if(queries.size() >= batchSize){
            sendQueriesToLoader(queries);
            queries.clear();
        }
    }

    /**
     * Set the size of the each transaction in terms of number of vars.
     * @param size number of vars in each transaction
     */
    public Loader setBatchSize(int size){
        this.batchSize = size;
        return this;
    }

    /**
     * @return the current batch size - minimum number of vars to be loaded in a transaction
     */
    public int getBatchSize(){
        return this.batchSize;
    }

    /**
     * Set the number of thread that will execute insert transactions at a time
     */
    public Loader setThreadsNumber(int number){
        this.threadsNumber = number;
        return this;
    }

    /**
     * Load any remaining batches in the queue.
     */
    public void flush(){
        if(queries.size() > 0){
            sendQueriesToLoader(queries);
            queries.clear();
        }
    }

    /**
     * Method that logs the current state of loading transactions
     */
    public void printLoaderState(){
        LOG.info(Json.object().set(TransactionState.State.QUEUED.name(), enqueuedJobs.get())
                .set(TransactionState.State.LOADING.name(), loadingJobs.get())
                .set(TransactionState.State.ERROR.name(), errorJobs.get())
                .set(TransactionState.State.FINISHED.name(), finishedJobs.get()).toString());
    }

    protected void handleError(Exception e, int i) {
        LOG.error("Caught exception ", e);
        try {
            Thread.sleep((i + 2) * 1000);
        } catch (InterruptedException e1) {
            LOG.error("Caught exception ", e1);
        }
    }
}
