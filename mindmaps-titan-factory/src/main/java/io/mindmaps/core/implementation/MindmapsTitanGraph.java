package io.mindmaps.core.implementation;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.exceptions.GraphRuntimeException;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MindmapsTitanGraph implements MindmapsGraph {
    protected final Logger LOG = LoggerFactory.getLogger(MindmapsTitanGraph.class);

    private TitanGraph rootGraph;

    public MindmapsTitanGraph(TitanGraph graph){
        rootGraph = graph;
        checkMetaOntology();
    }

    TitanGraph getTitanGraph(){
        if(rootGraph == null){
            throw new GraphRuntimeException(ErrorMessage.CLOSED.getMessage(this.getClass().getName()));
        }
        return rootGraph;
    }

    @Override
    public MindmapsTransaction newTransaction() {
        return new MindmapsTitanTransaction(this);
    }

    @Override
    public void close() {
        getTitanGraph().close();
        rootGraph = null;
    }

    @Override
    public void clear() {
        getTitanGraph().close();
        TitanCleanup.clear(rootGraph);
    }

    @Override
    public Graph getGraph() {
        return getTitanGraph();
    }

    private void checkMetaOntology(){
        MindmapsTransactionImpl metaOntologyCreator = new MindmapsTitanTransaction(this);
        if(!metaOntologyCreator.isMetaOntologyInitialised()){
            metaOntologyCreator.initialiseMetaConcepts();
            try {
                metaOntologyCreator.commit();
            } catch (MindmapsValidationException e) {
                LOG.error("Error creating new meta ontology . . .", e);
                e.printStackTrace();
            }
        } else {
            try {
                metaOntologyCreator.close();
            } catch (Exception e) {
                LOG.error("Could not close rootGraph after finding ontology.", e);
                e.printStackTrace();
            }
        }
    }
}
