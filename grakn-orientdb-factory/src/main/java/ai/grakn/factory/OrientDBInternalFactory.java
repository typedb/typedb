/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.factory;

import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import ai.grakn.graph.internal.GraknOrientDBGraph;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

public class OrientDBInternalFactory extends AbstractInternalFactory<GraknOrientDBGraph, OrientGraph> {
    private final Logger LOG = LoggerFactory.getLogger(OrientDBInternalFactory.class);
    private final Map<String, OrientGraphFactory> openFactories;
    private static final String KEY_TYPE = "keytype";
    private static final String UNIQUE = "type";
    private static final String SPECIAL_IN_MEMORY = "memory";

    public OrientDBInternalFactory(String keyspace, String engineUrl, String config) {
        super(keyspace, engineUrl, config);
        openFactories = new HashMap<>();
    }

    @Override
    boolean isClosed(OrientGraph innerGraph) {
        return innerGraph.isClosed();
    }

    @Override
    GraknOrientDBGraph buildGraknGraphFromTinker(OrientGraph graph, boolean batchLoading) {
        String engineUrl = super.engineUrl;
        if(engineUrl.equals(SPECIAL_IN_MEMORY))
            engineUrl = null;
        return new GraknOrientDBGraph(graph, super.keyspace, engineUrl, batchLoading);
    }

    @Override
    OrientGraph buildTinkerPopGraph() {
        LOG.warn(ErrorMessage.CONFIG_IGNORED.getMessage("pathToConfig", super.config));
        return configureGraph(super.keyspace, super.engineUrl);
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
                if(!graph.getVertexIndexedKeys(baseType.name()).contains(conceptProperty)) {
                    graph.createVertexIndex(conceptProperty, baseType.name(), indexConfig);
                }
            }
        }

        return graph;
    }

    private OrientGraphFactory getFactory(String name, String address){
        if(SPECIAL_IN_MEMORY.equals(name)){
            address = SPECIAL_IN_MEMORY; //Secret way of creating in-memory graphs.
        }

        String key = name + address;
        if(!openFactories.containsKey(key)){
            openFactories.put(key, new OrientGraphFactory(address + ":" + name));
        }
        return openFactories.get(key);
    }
}
