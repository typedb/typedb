package io.mindmaps.core.implementation;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.MindmapsValidationException;

public class MindmapsTinkerTransaction extends MindmapsTransactionImpl {
    MindmapsTinkerGraph rootGraph;

    public MindmapsTinkerTransaction(MindmapsTinkerGraph graph) {
        super(graph.getGraph());
        this.rootGraph = graph;
        initialiseMetaConcepts();
    }

    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();
        getTransaction().clearTransaction();
        LOG.warn(ErrorMessage.TINKERGRAPH_WARNING.getMessage());
        getTransaction().clearTransaction();
    }

    @Override
    public void refresh() throws Exception {
        throw new UnsupportedOperationException(ErrorMessage.NOT_SUPPORTED.getMessage("Tinkergraph"));
    }

    @Override
    public void close() throws Exception {
        getTransaction().clearTransaction();
        setTinkerPopGraph(null);
    }

    @Override
    public MindmapsGraph getRootGraph() {
        return rootGraph;
    }
}
