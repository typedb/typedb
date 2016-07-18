package io.mindmaps.core.dao;

import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * A mindmaps graph which produces new transactions to work with
 */
public interface MindmapsGraph {
    /**
     *
     * @return A new transaction with a snapshot of the graph at the time of creation
     */
    MindmapsTransaction newTransaction();

    /**
     * Closes the graph making it unusable
     */
    void close();

    /**
     * Clears the graph completely. WARNING: This will invalidate any open transactions.
     */
    void clear();

    /**
     *
     * @return Returns the underlaying gremlin graph.
     */
    Graph getGraph();
}
