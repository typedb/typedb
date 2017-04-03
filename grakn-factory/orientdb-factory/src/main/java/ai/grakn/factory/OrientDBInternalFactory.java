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

import ai.grakn.Grakn;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.graph.internal.GraknOrientDBGraph;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

import static ai.grakn.util.ErrorMessage.INVALID_DATATYPE;

/**
 * <p>
 *     A Grakn Graph on top of {@link OrientGraph}
 * </p>
 *
 * <p>
 *     This produces an grakn graph on top of {@link OrientGraph}.
 *     The base construction process defined by {@link AbstractInternalFactory} ensures the graph factories are singletons.
 * </p>
 *
 * @author fppt
 */
public class OrientDBInternalFactory extends AbstractInternalFactory<GraknOrientDBGraph, OrientGraph> {
    private final Logger LOG = LoggerFactory.getLogger(OrientDBInternalFactory.class);
    private final Map<String, OrientGraphFactory> openFactories;

    public OrientDBInternalFactory(String keyspace, String engineUrl, Properties properties) {
        super(keyspace, engineUrl, properties);
        openFactories = new HashMap<>();
    }

    @Override
    GraknOrientDBGraph buildGraknGraphFromTinker(OrientGraph graph, boolean batchLoading) {
        return new GraknOrientDBGraph(graph, super.keyspace, super.engineUrl, batchLoading);
    }

    @Override
    OrientGraph buildTinkerPopGraph(boolean batchLoading) {
        LOG.warn(ErrorMessage.CONFIG_IGNORED.getMessage("properties", properties));
        return configureGraph(super.keyspace, super.engineUrl);
    }

    //TODO: Fix this later
    @Override
    protected OrientGraph getGraphWithNewTransaction(OrientGraph graph, boolean batchloading) {
        return graph;
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
        Set<String> labels = keys.keySet();

        for (String label : labels) {
            String [] configs = keys.getString(label).split(",");

            for (String propertyConfig : configs) {
                String[] propertyConfigs = propertyConfig.split(":");
                Schema.ConceptProperty property = Schema.ConceptProperty.valueOf(propertyConfigs[0]);
                boolean isUnique = Boolean.parseBoolean(propertyConfigs[1]);

                OType orientDataType = getOrientDataType(property);
                BaseConfiguration indexConfig = new BaseConfiguration();
                indexConfig.setProperty("keytype", orientDataType);
                //TODO: Figure out why this is not working when the Orient Guys say it should
                //indexConfig.setProperty("metadata.ignoreNullValues", true);

                if(isUnique){
                    indexConfig.setProperty("type", "UNIQUE");
                }

                if(!graph.getVertexIndexedKeys(label).contains(property.name())) {
                    graph.createVertexIndex(property.name(), label, indexConfig);
                }
            }
        }

        return graph;
    }

    private OType getOrientDataType(Schema.ConceptProperty property){
        Class propertyDataType = property.getDataType();

        if(propertyDataType.equals(String.class)){
            return OType.STRING;
        } else if(propertyDataType.equals(Long.class)){
            return OType.LONG;
        } else if(propertyDataType.equals(Double.class)){
            return OType.DOUBLE;
        } else if(propertyDataType.equals(Boolean.class)){
            return OType.BOOLEAN;
        } else {
            String options = String.class.getName() + ", " + Long.class.getName() + ", " +
                    Double.class.getName() + ", or " + Boolean.class.getName();
            throw new GraphRuntimeException(INVALID_DATATYPE.getMessage(propertyDataType.getName(), options));
        }
    }

    private OrientGraphFactory getFactory(String name, String address){
        if (Grakn.IN_MEMORY.equals(address)){
            address = "memory";
            //name = "/tmp/" + name;
        }

        String key = name + address;
        if(!openFactories.containsKey(key)){
            openFactories.put(key, new OrientGraphFactory(address + ":" + name));
        }
        return openFactories.get(key);
    }
}
