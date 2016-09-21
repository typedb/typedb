package io.mindmaps.factory;

import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import io.mindmaps.graph.internal.MindmapsOrientDBGraph;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

public class MindmapsOrientDBGraphFactory extends AbstractMindmapsGraphFactory<MindmapsOrientDBGraph, OrientGraph>{
    private final Logger LOG = LoggerFactory.getLogger(MindmapsOrientDBGraphFactory.class);
    private final Map<String, OrientGraphFactory> openFactories;
    private static final String KEY_TYPE = "keytype";
    private static final String UNIQUE = "type";

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

        graph = createIndicesVertex(graph);

        graph.commit();

        return factory.getNoTx();
    }

    private OrientGraph createIndicesVertex(OrientGraph graph){
        ResourceBundle keys = ResourceBundle.getBundle("indices-vertices");
        Set<String> keyString = keys.keySet();

        for(String conceptProperty : keyString){
            String[] configs = keys.getString(conceptProperty).split(",");

            BaseConfiguration indexConfig = new BaseConfiguration();
            OType otype = OType.STRING;
            switch (configs[0]){
                case "Long":
                    otype = OType.LONG;
                    break;
                case "Double":
                    otype = OType.DOUBLE;
                    break;
                case "Boolean":
                    otype = OType.BOOLEAN;
                    break;
            }

            indexConfig.setProperty(KEY_TYPE, otype);

            if(Boolean.valueOf(configs[1])){
                indexConfig.setProperty(UNIQUE, "UNIQUE");
            }

            for (Schema.BaseType baseType : Schema.BaseType.values()) {
                graph.createVertexIndex(conceptProperty, baseType.name(), indexConfig);
            }
        }


        return graph;
    }

    private OrientGraphFactory getFactory(String name, String address){
        String key = name + address;
        if(!openFactories.containsKey(key)){
            openFactories.put(key, new OrientGraphFactory(address + ":" + name));
        }
        return openFactories.get(key);
    }
}
