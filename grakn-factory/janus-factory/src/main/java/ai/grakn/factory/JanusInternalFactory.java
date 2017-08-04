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

import ai.grakn.graph.internal.GraknJanusGraph;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Namifiable;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;


/**
 * <p>
 *     A Grakn Graph on top of {@link JanusGraph}
 * </p>
 *
 * <p>
 *     This produces a grakn graph on top of {@link JanusGraph}.
 *     The base construction process defined by {@link AbstractInternalFactory} ensures the graph factories are singletons.
 * </p>
 *
 * @author fppt
 */
final public class JanusInternalFactory extends AbstractInternalFactory<GraknJanusGraph, JanusGraph> {
    private final static Logger LOG = LoggerFactory.getLogger(JanusInternalFactory.class);
    private final static String DEFAULT_CONFIG = "backend-default";

    private static final AtomicBoolean strategiesApplied = new AtomicBoolean(false);

    JanusInternalFactory(String keyspace, String engineUrl, Properties properties) {
        super(keyspace, engineUrl, properties);
    }

    @Override
    public JanusGraph getGraphWithNewTransaction(JanusGraph graph, boolean batchloading){
        if(graph.isClosed()) graph = buildTinkerPopGraph(batchloading);

        if(!graph.tx().isOpen()){
            graph.tx().open();
        }
        return graph;
    }

    @Override
    GraknJanusGraph buildGraknGraphFromTinker(JanusGraph graph) {
        return new GraknJanusGraph(graph, super.keyspace, super.engineUrl, super.properties);
    }

    @Override
    JanusGraph buildTinkerPopGraph(boolean batchLoading) {
        return newJanusGraph(super.keyspace, super.engineUrl, super.properties, batchLoading);
    }

    private synchronized JanusGraph newJanusGraph(String name, String address, Properties properties, boolean batchLoading){
        JanusGraph JanusGraph = configureGraph(name, address, properties, batchLoading);
        buildJanusIndexes(JanusGraph);
        JanusGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);

        if (!strategiesApplied.getAndSet(true)) {
            TraversalStrategies strategies = TraversalStrategies.GlobalCache.getStrategies(StandardJanusGraph.class);
            strategies = strategies.clone().addStrategies(new JanusPreviousPropertyStepStrategy());
            TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraph.class, strategies);
            TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraphTx.class, strategies);
        }

        return JanusGraph;
    }

    private JanusGraph configureGraph(String name, String address, Properties properties, boolean batchLoading){
        //Load default properties if none provided
        if(properties == null){
            properties = new Properties();
            try (InputStream in = getClass().getResourceAsStream(DEFAULT_CONFIG)) {
                properties.load(in);
                in.close();
            } catch (IOException e) {
                throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(DEFAULT_CONFIG), e);
            }
        }


        JanusGraphFactory.Builder builder = JanusGraphFactory.build().
                set("storage.hostname", address).
                set("storage.cassandra.keyspace", name).
                set("storage.batch-loading", batchLoading);

        properties.forEach((key, value) -> builder.set(key.toString(), value));
        LOG.debug("Opening graph on {}", address);
        return builder.open();
    }


    private static void buildJanusIndexes(JanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();

        makeVertexLabels(management);
        makeEdgeLabels(management);
        makePropertyKeys(management);

        makeIndicesVertexCentric(management);
        makeIndicesComposite(management);

        management.commit();
    }

    private static void makeEdgeLabels(JanusGraphManagement management){
        for (Schema.EdgeLabel edgeLabel : Schema.EdgeLabel.values()) {
            EdgeLabel label = management.getEdgeLabel(edgeLabel.getLabel());
            if(label == null) {
                management.makeEdgeLabel(edgeLabel.getLabel()).make();
            }
        }
    }

    private static void makeVertexLabels(JanusGraphManagement management){
        for (Schema.BaseType baseType : Schema.BaseType.values()) {
            VertexLabel foundLabel = management.getVertexLabel(baseType.name());
            if(foundLabel == null) {
                management.makeVertexLabel(baseType.name()).make();
            }
        }
    }

    private static void makeIndicesVertexCentric(JanusGraphManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("indices-edges");
        Set<String> edgeLabels = keys.keySet();
        for(String edgeLabel : edgeLabels){
            String[] propertyKeyStrings = keys.getString(edgeLabel).split(",");

            //Get all the property keys we need
            Set<PropertyKey> propertyKeys = stream(propertyKeyStrings).map(keyId ->{
                PropertyKey key = management.getPropertyKey(keyId);
                if (key == null) {
                    throw new RuntimeException("Trying to create edge index on label [" + edgeLabel + "] but the property [" + keyId + "] does not exist");
                }
                return key;
            }).collect(Collectors.toSet());

            //Get the edge and indexing information
            RelationType relationType = management.getRelationType(edgeLabel);
            EdgeLabel label = management.getEdgeLabel(edgeLabel);

            //Create index on each property key
            for (PropertyKey key : propertyKeys) {
                if (management.getRelationIndex(relationType, edgeLabel + "by" + key.name()) == null) {
                    management.buildEdgeIndex(label, edgeLabel + "by" + key.name(), Direction.BOTH, Order.decr, key);
                }
            }

            //Create index on all property keys
            String propertyKeyId = propertyKeys.stream().map(Namifiable::name).collect(Collectors.joining("_"));
            if (management.getRelationIndex(relationType, edgeLabel + "by" + propertyKeyId) == null) {
                PropertyKey [] allKeys = propertyKeys.toArray(new PropertyKey[propertyKeys.size()]);
                management.buildEdgeIndex(label, edgeLabel + "by" + propertyKeyId, Direction.BOTH, Order.decr, allKeys);
            }
        }
    }

    private static void makePropertyKeys(JanusGraphManagement management){
        stream(Schema.VertexProperty.values()).forEach(property ->
                makePropertyKey(management, property.name(), property.getDataType()));

        stream(Schema.EdgeProperty.values()).forEach(property ->
                makePropertyKey(management, property.name(), property.getDataType()));
    }

    private static void makePropertyKey(JanusGraphManagement management, String propertyKey, Class type){
        if (management.getPropertyKey(propertyKey) == null) {
            management.makePropertyKey(propertyKey).dataType(type).make();
        }
    }

    private static void makeIndicesComposite(JanusGraphManagement management){
        ResourceBundle keys = ResourceBundle.getBundle("indices-composite");
        Set<String> keyString = keys.keySet();
        for(String propertyKeyLabel : keyString){
            String indexLabel = "by" + propertyKeyLabel;
            JanusGraphIndex index = management.getGraphIndex(indexLabel);

            if(index == null) {
                boolean isUnique = Boolean.parseBoolean(keys.getString(propertyKeyLabel));
                PropertyKey key = management.getPropertyKey(propertyKeyLabel);
                JanusGraphManagement.IndexBuilder indexBuilder = management.buildIndex(indexLabel, Vertex.class).addKey(key);
                if (isUnique) {
                    indexBuilder.unique();
                }
                indexBuilder.buildCompositeIndex();
            }
        }
    }
}

