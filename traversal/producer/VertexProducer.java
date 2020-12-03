package grakn.core.traversal.producer;

import grakn.core.graph.GraphManager;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.procedure.Procedure;
import graql.lang.pattern.variable.Reference;

import java.util.Map;

public class VertexProducer implements TraversalProducer {

    private final GraphManager graphMgr;
    private final Procedure procedure;
    private final int parallelisation;

    public VertexProducer(GraphManager graphMgr, Procedure procedure, int parallelisation) {
        this.graphMgr = graphMgr;
        this.procedure = procedure;
        this.parallelisation = parallelisation;
    }

    @Override
    public void produce(Sink<Map<Reference, Vertex<?, ?>>> sink, int count) {

    }

    @Override
    public void recycle() {

    }
}
