package io.mindmaps.engine.loader;

import io.mindmaps.graql.QueryParser;
import io.mindmaps.graql.Var;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RESTLoader to perform bulk loading into the graph
 */
public abstract class Loader {

    protected AtomicInteger enqueuedJobs;
    protected AtomicInteger loadingJobs;
    protected AtomicInteger finishedJobs;
    protected AtomicInteger errorJobs;

    protected Collection<Var> batch;
    protected int batchSize;
    protected int threadsNumber;

    final Logger LOG = LoggerFactory.getLogger(Loader.class);


    public Loader(){
        enqueuedJobs = new AtomicInteger();
        loadingJobs = new AtomicInteger();
        errorJobs = new AtomicInteger();
        finishedJobs = new AtomicInteger();
    }

    /**
     * Method to load data into the graph. Implementation depends on the type of the loader.
     */
    protected abstract void submitBatch(Collection<Var> batch);

    /**
     * Wait for all loading to terminate.
     */
    public abstract void waitToFinish();

    /**
     * Add a single var to the queue
     * @param var to be loaded
     */
    public void addToQueue(Var var){
        addToQueue(Collections.singleton(var));
    }

    /**
     * Add the given query to the queue to load
     * @param vars to be loaded
     */
    public void addToQueue(String vars){
        try {
            addToQueue(QueryParser.create().parseInsertQuery(vars).admin().getVars());
        }
        catch (IllegalArgumentException e){
            System.out.println(vars);
            e.printStackTrace();
        }
    }

    /**
     * Add multiple vars to the queue. These should be inserted in one transaction.
     * @param vars to be loaded
     */
    public void addToQueue(Collection<? extends Var> vars){
        batch.addAll(vars);
        if(batch.size() > batchSize){
            submitBatch(batch);
            batch.clear();
        }
    }

    /**
     * Set the size of the each transaction in terms of number of vars.
     * @param size number of vars in each transaction
     */
    public void setBatchSize(int size){
        this.batchSize = size;
    }

    public void setThreadsNumber(int number){
        this.threadsNumber = number;
    }

    /**
     * Load any remaining batches in the queue.
     */
    public void flush(){
        if(batch.size() > 0){
            submitBatch(batch);
            batch.clear();
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

    public void markAsQueued(String transaction){
        enqueuedJobs.incrementAndGet();
    }

    public void markAsLoading(String transaction){
        loadingJobs.incrementAndGet();
    }

    public void markAsFinished(String transaction){
        loadingJobs.decrementAndGet();
        finishedJobs.incrementAndGet();
    }

    public void markAsError(String transaction){
        loadingJobs.decrementAndGet();
        errorJobs.incrementAndGet();
    }
}
