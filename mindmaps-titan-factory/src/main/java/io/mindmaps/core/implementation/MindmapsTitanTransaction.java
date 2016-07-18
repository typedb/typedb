package io.mindmaps.core.implementation;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.exceptions.MindmapsValidationException;

public class MindmapsTitanTransaction extends MindmapsTransactionImpl {
    private MindmapsTitanGraph rootGraph;

    public MindmapsTitanTransaction(MindmapsTitanGraph graph) {
        super(graph.getTitanGraph().newTransaction());
        rootGraph = graph;
    }

    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();
        LOG.info("Graph is valid. Committing graph . . . ");
        getTinkerPopGraph().tx().commit();
        try {
            refreshTransaction();
        } catch (Exception e) {
            LOG.error("Failed to create new transaction after committing", e);
            e.printStackTrace();
        }
        LOG.info("Graph committed.");
    }

    @Override
    public void refresh() throws Exception {
        close();
        refreshTransaction();
    }

    @Override
    public void close() throws Exception {
        getTinkerPopGraph().close();
        setTinkerPopGraph(null);
    }

    private void refreshTransaction() throws Exception {
        getTinkerPopGraph().close();
        setTinkerPopGraph(rootGraph.getTitanGraph().newTransaction());
        getTransaction().clearTransaction();
    }

    @Override
    public MindmapsGraph getRootGraph() {
        return rootGraph;
    }
}
