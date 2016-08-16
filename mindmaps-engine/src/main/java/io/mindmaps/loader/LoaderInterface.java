package io.mindmaps.loader;

import io.mindmaps.graql.Var;

import java.util.Collection;

/**
 * Loader to perform bulk loading into the graph
 */
public interface LoaderInterface {

    /**
     * Add a single var to the queue
     * @param var to be loaded
     */
    public void addToQueue(Var var);

    /**
     * Add multiple vars to the queue. These should be inserted in one transaction.
     * @param vars to be loaded
     */
    public void addToQueue(Collection<Var> vars);

    /**
     * Add the given query to the queue to load
     * @param vars to be loaded
     */
    public void addToQueue(String vars);

    /**
     * Set the size of the each transaction in terms of number of vars.
     * @param size number of vars in each transaction
     */
    public void setBatchSize(int size);



}
