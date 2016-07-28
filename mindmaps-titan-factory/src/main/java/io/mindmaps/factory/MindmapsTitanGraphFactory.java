/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.factory;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.TitanIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.implementation.MindmapsTitanGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

class MindmapsTitanGraphFactory implements MindmapsGraphFactory{
    protected final Logger LOG = LoggerFactory.getLogger(MindmapsTitanGraphFactory.class);
    private final Map<String, MindmapsTitanGraph> openGraphs;
    private final static String SEARCH_KEY = "search";
    private final static String DEFAULT_ADDRESS = "localhost";
    private final static String DEAFULT_CONFIG = "backend-default";

    public MindmapsTitanGraphFactory(){
        openGraphs = new HashMap<>();
    }

    @Override
    public MindmapsGraph getGraph(String name, String address, String pathToConfig){
        if(address == null)
            address = DEFAULT_ADDRESS;

        String key = name + "_" + address;
        if(openGraphs.containsKey(key)){
            MindmapsTitanGraph mindmapsTitanGraph = openGraphs.get(key);
            TitanGraph graph = (TitanGraph) mindmapsTitanGraph.getGraph();
            if(graph.isOpen()){
                return mindmapsTitanGraph;
            }
        }

        MindmapsTitanGraph mindmapsTitanGraph = new MindmapsTitanGraph(newTitanGraph(name, address, pathToConfig));
        openGraphs.put(key, mindmapsTitanGraph);

        System.out.println("=================================================================================================");
        System.out.println("||||||||||||||||||||      " + openGraphs.size() + " TitanGraph(s) are instantiated for Mindmaps      |||||||||||||||||||");
        System.out.println("=================================================================================================");

        return mindmapsTitanGraph;
    }

    private synchronized TitanGraph newTitanGraph(String name, String address, String pathToConfig){
        TitanGraph titanGraph = configureGraph(name, address, pathToConfig);
        buildTitanIndexes(titanGraph);
        titanGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        return titanGraph;
    }

    private TitanGraph configureGraph(String name, String address, String pathToConfig){
        ResourceBundle defaultConfig;
        if(pathToConfig == null) {
            defaultConfig = ResourceBundle.getBundle(DEAFULT_CONFIG);
        } else {
            try {
                FileInputStream fis = new FileInputStream(pathToConfig);
                defaultConfig = new PropertyResourceBundle(fis);
            } catch (IOException e) {
                LOG.error(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(pathToConfig), e);
                throw new IllegalArgumentException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(pathToConfig));
            }
        }

        TitanFactory.Builder builder = TitanFactory.build().
                set("storage.hostname", address).
                set("storage.cassandra.keyspace", name);

        defaultConfig.keySet().forEach(key -> builder.set(key, defaultConfig.getString(key)));
        return builder.open();
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

