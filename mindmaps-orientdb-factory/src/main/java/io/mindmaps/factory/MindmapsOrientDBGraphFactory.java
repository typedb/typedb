package io.mindmaps.factory;

import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import io.mindmaps.graph.internal.MindmapsOrientDBGraph;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MindmapsOrientDBGraphFactory extends AbstractMindmapsGraphFactory<MindmapsOrientDBGraph, OrientGraph>{
    private final Logger LOG = LoggerFactory.getLogger(MindmapsOrientDBGraphFactory.class);
    private final Map<String, OrientGraphFactory> openFactories;

    public MindmapsOrientDBGraphFactory(){
        super();
        openFactories = new HashMap<>();
    }

    @Override
    boolean isClosed(OrientGraph innerGraph) {
        return innerGraph.isClosed();
    }

    @Override
    MindmapsOrientDBGraph buildMindmapsGraphFromTinker(OrientGraph graph, String name, String engineUrl, boolean batchLoading) {
        return new MindmapsOrientDBGraph(graph, name, engineUrl, batchLoading);
    }

    @Override
    OrientGraph buildTinkerPopGraph(String name, String address, String pathToConfig) {
        LOG.warn(ErrorMessage.CONFIG_IGNORED.getMessage("pathToConfig", pathToConfig));
        return configureGraph(name, address);
    }

    private OrientGraph configureGraph(String name, String address){
        boolean schemaDefinitionRequired = false;
        OrientGraphFactory factory = getFactory(name, address);
        OrientGraph graph = factory.getNoTx();

        //Check if the schema has been created
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            try {
                graph.database().browseClass(OImmutableClass.VERTEX_CLASS_NAME + "_" + baseType);
            } catch (IllegalArgumentException e){
                schemaDefinitionRequired = true;
                break;
            }
        }

        //Create the schema if needed
        if(schemaDefinitionRequired){
            graph = createGraphWithSchema(factory, graph);
        }

        return graph;
    }

    private OrientGraph createGraphWithSchema(OrientGraphFactory factory, OrientGraph graph){
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            graph.createVertexClass(baseType.name());
        }

        for (Schema.EdgeLabel edgeLabel : Schema.EdgeLabel.values()) {
            graph.createEdgeClass(edgeLabel.name());
        }

        graph.commit();

        return factory.getNoTx();
    }

    private OrientGraphFactory getFactory(String name, String address){
        String key = name + address;
        if(!openFactories.containsKey(key)){
            openFactories.put(key, new OrientGraphFactory(address + ":" + name));
        }
        return openFactories.get(key);
    }
}
