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

import ai.grakn.graph.internal.GraknTitanGraph;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.TitanIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

/**
 * <p>
 *     A Grakn Graph on top of {@link TitanGraph}
 * </p>
 *
 * <p>
 *     This produces an in memory grakn graph on top of {@link TitanGraph}.
 *     The base construction process defined by {@link AbstractInternalFactory} ensures the graph factories are singletons.
 * </p>
 *
 * @author fppt
 */
class TitanInternalFactory extends AbstractInternalFactory<GraknTitanGraph, TitanGraph> {
    private final static String DEFAULT_CONFIG = "backend-default";

    TitanInternalFactory(String keyspace, String engineUrl, Properties properties) {
        super(keyspace, engineUrl, properties);
    }

    @Override
    boolean isClosed(TitanGraph innerGraph) {
        return innerGraph.isClosed();
    }

    @Override
    public TitanGraph getGraphWithNewTransaction(TitanGraph graph){
        if(!graph.tx().isOpen()){
            graph.tx().open();
        }
        return graph;
    }

    @Override
    GraknTitanGraph buildGraknGraphFromTinker(TitanGraph graph, boolean batchLoading) {
        return new GraknTitanGraph(graph, super.keyspace, super.engineUrl, batchLoading);
    }

    @Override
    TitanGraph buildTinkerPopGraph(boolean batchLoading) {
        return newTitanGraph(super.keyspace, super.engineUrl, super.properties, batchLoading);
    }

    private synchronized TitanGraph newTitanGraph(String name, String address, Properties properties, boolean batchLoading){
        TitanGraph titanGraph = configureGraph(name, address, properties, batchLoading);
        buildTitanIndexes(titanGraph);
        titanGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        return titanGraph;
    }

    private TitanGraph configureGraph(String name, String address, Properties properties, boolean batchLoading){
        //Load default properties if none provided
        if(properties == null){
            properties = new Properties();
            InputStream in = getClass().getResourceAsStream(DEFAULT_CONFIG);
            try {
                properties.load(in);
                in.close();
            } catch (IOException e) {
                throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(DEFAULT_CONFIG), e);
            }
        }

        TitanFactory.Builder builder = TitanFactory.build().
                set("storage.hostname", address).
                set("storage.cassandra.keyspace", name).
                set("storage.batch-loading", batchLoading);

        properties.entrySet().forEach(entry-> builder.set(entry.getKey().toString(), entry.getValue()));

        return builder.open();
    }


    private static void buildTitanIndexes(TitanGraph graph) {
        TitanManagement management = graph.openManagement();

        makeVertexLabels(management);
        makeEdgeLabels(management);
        makePropertyKeys(management);

        makeIndicesVertexCentric(management);
        makeIndicesComposite(management);

        management.commit();
    }

    private static void makeEdgeLabels(TitanManagement management){
        for (Schema.EdgeLabel edgeLabel : Schema.EdgeLabel.values()) {
            EdgeLabel label = management.getEdgeLabel(edgeLabel.getLabel());
            if(label == null) {
                management.makeEdgeLabel(edgeLabel.getLabel()).make();
            }
        }
    }

    private static void makeVertexLabels(TitanManagement management){
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            VertexLabel foundLabel = management.getVertexLabel(baseType.name());
            if(foundLabel == null) {
                management.makeVertexLabel(baseType.name()).make();
            }
        }
    }

    private static void makeIndicesVertexCentric(TitanManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("indices-edges");
        Set<String> edgeLabels = keys.keySet();
        for(String edgeLabel : edgeLabels){
            String properties = keys.getString(edgeLabel);
            if(properties.length() > 0){
                String[] propertyKey = keys.getString(edgeLabel).split(",");
                for (String aPropertyKey : propertyKey) {
                    PropertyKey key = management.getPropertyKey(aPropertyKey);
                    if (key == null) {
                        throw new RuntimeException("Trying to create edge index on label [" + edgeLabel + "] but the property [" + aPropertyKey + "] does not exist");
                    }

                    RelationType relationType = management.getRelationType(edgeLabel);
                    if (management.getRelationIndex(relationType, edgeLabel + "by" + aPropertyKey) == null) {
                        EdgeLabel label = management.getEdgeLabel(edgeLabel);
                        management.buildEdgeIndex(label, edgeLabel + "by" + aPropertyKey, Direction.OUT, Order.decr, key);
                    }
                }
            }
        }
    }

    private static void makePropertyKeys(TitanManagement management){
        Arrays.stream(Schema.ConceptProperty.values()).forEach(property ->
                makePropertyKey(management, property.name(), property.getDataType()));

        Arrays.stream(Schema.EdgeProperty.values()).forEach(property ->
                makePropertyKey(management, property.name(), property.getDataType()));
    }

    private static void makePropertyKey(TitanManagement management, String propertyKey, Class type){
        if (management.getPropertyKey(propertyKey) == null) {
            management.makePropertyKey(propertyKey).dataType(type).make();
        }
    }

    private static void makeIndicesComposite(TitanManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("indices-composite");
        Set<String> keyString = keys.keySet();
        for(String propertyKeyLabel : keyString){
            String indexLabel = "by" + propertyKeyLabel;
            TitanIndex index = management.getGraphIndex(indexLabel);

            if(index == null) {
                boolean isUnique = Boolean.parseBoolean(keys.getString(propertyKeyLabel));
                PropertyKey key = management.getPropertyKey(propertyKeyLabel);
                TitanManagement.IndexBuilder indexBuilder = management.buildIndex(indexLabel, Vertex.class).addKey(key);
                if (isUnique) {
                    indexBuilder.unique();
                }
                indexBuilder.buildCompositeIndex();
            }
        }
    }
}

