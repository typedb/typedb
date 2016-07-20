package io.mindmaps.factory;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.TitanIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.implementation.MindmapsTitanGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

class MindmapsTitanGraphFactory implements MindmapsGraphFactory{
    private final Map<String, TitanGraph> instances;
    private final static String SEARCH_KEY = "search";

    public MindmapsTitanGraphFactory(){
        instances = new HashMap<>();
    }

    @Override
    public MindmapsGraph newGraph(String... config){
        if(config.length != 1){
            throw new IllegalArgumentException("Exactly one configuration file must be provided when creating a Titan Graph Instance");
        }
        return new MindmapsTitanGraph(getTitanGraph(config[0]));
    }

    private synchronized TitanGraph getTitanGraph(String config) {
        String key = generateKey(config);
        TitanGraph instance = instances.get(key);
        if(instance == null || !instance.isOpen()) {
            TitanGraph titanGraph = TitanFactory.open(config);
            buildTitanIndexes(titanGraph);
            titanGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
            instance = titanGraph;
            instances.put(key, instance);

            // We must perfectly align things!
            String extraBars = "";
            for (int i = 0; i < Integer.toString(instances.size()).length(); i ++) {
                extraBars += "|";
            }

            System.out.println("=================================================================================================");
            System.out.println("||||||||||||||||||||      " + instances.size() + " TitanGraph(s) are instantiated for Mindmaps      |||||||||||||||||||" + extraBars);
            System.out.println("=================================================================================================");
        }
        return instance;
    }

    private static String generateKey(String config){
        try {
            FileInputStream fis = new FileInputStream(config);
            PropertyResourceBundle prop = new PropertyResourceBundle(fis);
            String graphKey = "";
            Set<String> keys = prop.keySet();

            graphKey = expandKey(prop, keys, graphKey, "storage.hostname");
            graphKey = expandKey(prop, keys, graphKey, "storage.cassandra.keyspace");
            graphKey = expandKey(prop, keys, graphKey, "storage.batch-loading");
            graphKey = expandKey(prop, keys, graphKey, "index.search.backend");

            return graphKey;
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot find config file [" + config +"]");
        }
    }
    private static String expandKey(PropertyResourceBundle prop, Set<String> keys, String graphKey, String property){
        if(keys.contains(property))
            return graphKey + prop.getString(property);
        else
            return graphKey;
    }


    private static void buildTitanIndexes(TitanGraph graph) {
        TitanManagement management = graph.openManagement();

        makeVertexLabels(management);
        makePropertyKeys(management);
        makeEdges(management);
        makeIndicesComposite(management);
        makeIndicesMixed(management);

        management.commit();
    }

    private static void makeVertexLabels(TitanManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("base-types");
        Set<String> vertexLabels = keys.keySet();
        for (String vertexLabel : vertexLabels) {
            VertexLabel foundLabel = management.getVertexLabel(vertexLabel);
            if(foundLabel == null)
                management.makeVertexLabel(vertexLabel).make();
        }
    }

    private static void makeEdges(TitanManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("indices-edges");
        Set<String> edgeLabels = keys.keySet();
        for(String edgeLabel : edgeLabels){
            EdgeLabel label = management.getEdgeLabel(edgeLabel);
            if(label == null)
                label = management.makeEdgeLabel(edgeLabel).make();

            if(label == null)
                throw new RuntimeException("Trying to create edge index on label [" + edgeLabel + "] but label does not exist");

            String properties = keys.getString(edgeLabel);
            if(properties.length() > 0){
                String[] propertyKey = keys.getString(edgeLabel).split(",");
                for(int i = 0; i < propertyKey.length; i ++){
                    PropertyKey key = management.getPropertyKey(propertyKey[i]);
                    if(key==null)
                        throw new RuntimeException("Trying to create edge index on label [" + edgeLabel + "] but the property [" + propertyKey[i] + "] does not exist");

                    RelationType relationType = management.getRelationType(edgeLabel);
                    if(management.getRelationIndex(relationType, edgeLabel + "by" + propertyKey[i]) == null)
                        management.buildEdgeIndex(label, edgeLabel + "by" + propertyKey[i], Direction.OUT, Order.decr, key);
                 }
            }
        }
    }

    private static void makePropertyKeys(TitanManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("property-keys");
        Set<String> keyString = keys.keySet();
        for(String propertyKey : keyString){
            try {
                if (management.getPropertyKey(propertyKey) == null) {
                    String type = keys.getString(propertyKey);
                    if (management.getPropertyKey(propertyKey) == null) {
                        management.makePropertyKey(propertyKey).dataType(Class.forName(type)).make();
                    }
                }
            } catch(ClassNotFoundException e){
                System.out.println("Cannot create index due to unknown java primitive type");
                e.printStackTrace();
            }
        }
    }

    private static void makeIndicesComposite(TitanManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("indices-composite");
        Set<String> keyString = keys.keySet();
        for(String indexLabel : keyString){
            TitanIndex index = management.getGraphIndex(indexLabel);
            if(index == null) {
                String[] indexComponents = keys.getString(indexLabel).split(",");
                String propertyKey = indexComponents[0];
                boolean isUnique = Boolean.parseBoolean(indexComponents[1]);

                PropertyKey key = management.getPropertyKey(propertyKey);
                TitanManagement.IndexBuilder indexBuilder = management.buildIndex(indexLabel, Vertex.class).addKey(key);
                if (isUnique)
                    indexBuilder.unique();
                indexBuilder.buildCompositeIndex();
            }
        }
    }

    private static void makeIndicesMixed(TitanManagement management){
        ResourceBundle comboIndexConfig = ResourceBundle.getBundle("indices-mixed");
        Set<String> comboIndexNames = comboIndexConfig.keySet();
        for(String comboIndexName : comboIndexNames){
            TitanIndex index = management.getGraphIndex(comboIndexName);
            if(index == null) {
                TitanManagement.IndexBuilder indexBuilder;
                String [] indexArray = comboIndexConfig.getString(comboIndexName).split(":");
                String indexType = indexArray[0];
                String[] propertyKeys = indexArray[1].split(",");

                switch (indexType) {
                    case "Vertex":
                        indexBuilder = management.buildIndex(comboIndexName, Vertex.class);
                        break;
                    case "Edge":
                        indexBuilder = management.buildIndex(comboIndexName, Edge.class);
                        break;
                    default:
                        throw new IllegalArgumentException("Indexing element of type [" + indexType + "] is not supported.");
                }

                for (String propertyKey : propertyKeys) {
                    PropertyKey key = management.getPropertyKey(propertyKey);
                    if(key.dataType().equals(String.class)){
                        indexBuilder.addKey(key, Mapping.STRING.asParameter());
                    } else {
                        indexBuilder.addKey(key);
                    }
                }
                indexBuilder.buildMixedIndex(SEARCH_KEY);
            }
        }
    }

}

