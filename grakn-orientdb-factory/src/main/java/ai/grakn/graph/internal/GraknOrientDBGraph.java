package ai.grakn.graph.internal;

import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class GraknOrientDBGraph extends AbstractGraknGraph<OrientGraph> {
    public GraknOrientDBGraph(OrientGraph graph, String name, String engineUrl, boolean batchLoading){
        super(graph, name, engineUrl, batchLoading);
    }

    @Override
    protected void commitTx(){
        getTinkerPopGraph().commit();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> getTinkerTraversal(){
        Schema.BaseType[] baseTypes = Schema.BaseType.values();
        String [] labels = new String [baseTypes.length];

        for(int i = 0; i < labels.length; i ++){
            labels[i] = baseTypes[i].name();
        }

        return getTinkerPopGraph().traversal().withStrategies(ReadOnlyStrategy.instance()).V().hasLabel(labels);
    }

    @Override
    public void rollback(){
        throw new UnsupportedOperationException(ErrorMessage.UNSUPPORTED_GRAPH.getMessage(getTinkerPopGraph().getClass().getName(), "rollback"));
    }
}
