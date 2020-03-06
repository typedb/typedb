/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.mapreduce;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphFactory;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.graphdb.database.RelationReader;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.query.QueryUtil;
import grakn.core.graph.graphdb.relations.RelationCache;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.TypeDefinitionMap;
import grakn.core.graph.graphdb.types.TypeInspector;
import grakn.core.graph.graphdb.types.system.BaseKey;
import grakn.core.graph.graphdb.types.system.BaseLabel;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class VertexDeserializer implements AutoCloseable {

    private final HadoopSetup setup;
    private final TypeInspector typeManager;
    private final IDManager idManager;

    private static final Logger LOG = LoggerFactory.getLogger(VertexDeserializer.class);

    VertexDeserializer(Configuration conf) {
        this.setup = new HadoopSetup(conf);
        this.typeManager = setup.getTypeInspector();
        this.idManager = setup.getIDManager();
    }

    private static Boolean isLoopAdded(Vertex vertex, String label) {
        Iterator<Vertex> adjacentVertices = vertex.vertices(Direction.BOTH, label);

        while (adjacentVertices.hasNext()) {
            Vertex adjacentVertex = adjacentVertices.next();

            if (adjacentVertex.equals(vertex)) {
                return true;
            }
        }

        return false;
    }

    // Read a single row from the edgestore and create a TinkerVertex corresponding to the row
    // The neighboring vertices are represented by DetachedVertex instances
    TinkerVertex readHadoopVertex(StaticBuffer key, Iterable<Entry> entries) {

        // Convert key to a vertex ID
        long vertexId = idManager.getKeyID(key);
        Preconditions.checkArgument(vertexId > 0);

        // Partitioned vertex handling
        if (idManager.isPartitionedVertex(vertexId)) {
            Preconditions.checkState(setup.getFilterPartitionedVertices(),
                                     "Read partitioned vertex (ID=%s), but partitioned vertex filtering is disabled.", vertexId);
            LOG.debug("Skipping partitioned vertex with ID {}", vertexId);
            return null;
        }

        // Create TinkerVertex
        TinkerGraph tg = TinkerGraph.open();

        TinkerVertex tv = null;

        // Iterate over edgestore columns to find the vertex's label relation
        for (Entry data : entries) {
            RelationReader relationReader = setup.getRelationReader();
            RelationCache relation = relationReader.parseRelation(data, false, typeManager);
            if (relation.typeId == BaseLabel.VertexLabelEdge.longId()) {
                // Found vertex Label
                long vertexLabelId = relation.getOtherVertexId();
                VertexLabel vl = typeManager.getExistingVertexLabel(vertexLabelId);
                // Create TinkerVertex with this label
                tv = getOrCreateVertex(vertexId, vl.name(), tg);
            }
        }

        // Added this following testing
        if (null == tv) {
            tv = getOrCreateVertex(vertexId, null, tg);
        }

        Preconditions.checkNotNull(tv, "Unable to determine vertex label for vertex with ID %s", vertexId);

        // Iterate over and decode edgestore columns (relations) on this vertex
        for (Entry data : entries) {
            try {
                RelationReader relationReader = setup.getRelationReader();
                RelationCache relation = relationReader.parseRelation(data, false, typeManager);

                if (IDManager.isSystemRelationTypeId(relation.typeId)) continue; //Ignore system types
                RelationType type = typeManager.getExistingRelationType(relation.typeId);
                if (((InternalRelationType) type).isInvisibleType()) continue; //Ignore hidden types

                // Decode and create the relation (edge or property)
                if (type.isPropertyKey()) {
                    // Decode property
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    VertexProperty.Cardinality card = getPropertyKeyCardinality(type.name());
                    tv.property(card, type.name(), value, T.id, relation.relationId);
                } else {
                    // Partitioned vertex handling
                    if (idManager.isPartitionedVertex(relation.getOtherVertexId())) {
                        Preconditions.checkState(setup.getFilterPartitionedVertices(),
                                                 "Read edge incident on a partitioned vertex, but partitioned vertex filtering is disabled.  " +
                                                         "Relation ID: %s.  This vertex ID: %s.  Other vertex ID: %s.  Edge label: %s.",
                                                 relation.relationId, vertexId, relation.getOtherVertexId(), type.name());
                        LOG.debug("Skipping edge with ID {} incident on partitioned vertex with ID {} (and nonpartitioned vertex with ID {})",
                                  relation.relationId, relation.getOtherVertexId(), vertexId);
                        continue;
                    }

                    // Decode edge
                    TinkerEdge te;

                    // We don't know the label of the other vertex, but one must be provided
                    TinkerVertex adjacentVertex = getOrCreateVertex(relation.getOtherVertexId(), null, tg);

                    // handle self-loop edges
                    if (tv.equals(adjacentVertex) && isLoopAdded(tv, type.name())) {
                        continue;
                    }

                    if (relation.direction.equals(Direction.IN)) {
                        te = (TinkerEdge) adjacentVertex.addEdge(type.name(), tv, T.id, relation.relationId);
                    } else if (relation.direction.equals(Direction.OUT)) {
                        te = (TinkerEdge) tv.addEdge(type.name(), adjacentVertex, T.id, relation.relationId);
                    } else {
                        throw new RuntimeException("Direction.BOTH is not supported");
                    }

                    if (relation.hasProperties()) {
                        // Load relation properties
                        for (Map.Entry<Long, Object> next : relation.properties().entrySet()) {
                            RelationType rt = typeManager.getExistingRelationType(next.getKey());
                            if (rt.isPropertyKey()) {
                                te.property(rt.name(), next.getValue());
                            } else {
                                throw new RuntimeException("Metaedges are not supported");
                            }
                        }


                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /*Since we are filtering out system relation types, we might end up with vertices that have no incident relations.
         This is especially true for schema vertices. Those are filtered out.     */
        if (!tv.edges(Direction.BOTH).hasNext() && !tv.properties().hasNext()) {
            LOG.trace("Vertex {} has no relations", vertexId);
            return null;
        }
        return tv;
    }

    private TinkerVertex getOrCreateVertex(long vertexId, String label, TinkerGraph tg) {
        TinkerVertex v;

        try {
            v = (TinkerVertex) tg.vertices(vertexId).next();
        } catch (NoSuchElementException e) {
            if (null != label) {
                v = (TinkerVertex) tg.addVertex(T.label, label, T.id, vertexId);
            } else {
                v = (TinkerVertex) tg.addVertex(T.id, vertexId);
            }
        }

        return v;
    }

    private VertexProperty.Cardinality getPropertyKeyCardinality(String name) {
        RelationType rt = typeManager.getRelationType(name);
        if (null == rt || !rt.isPropertyKey()) {
            return VertexProperty.Cardinality.single;
        }
        PropertyKey pk = typeManager.getExistingPropertyKey(rt.longId());
        switch (pk.cardinality()) {
            case SINGLE:
                return VertexProperty.Cardinality.single;
            case LIST:
                return VertexProperty.Cardinality.list;
            case SET:
                return VertexProperty.Cardinality.set;
            default:
                throw new IllegalStateException("Unknown cardinality " + pk.cardinality());
        }
    }

    public void close() {
        setup.close();
    }

    private static class HadoopSetup {

        private final ModifiableConfigurationHadoop scanConf;
        private final StandardJanusGraph graph;
        private final StandardJanusGraphTx tx;

        HadoopSetup(Configuration config) {
            scanConf = ModifiableConfigurationHadoop.of(ModifiableConfigurationHadoop.MAPRED_NS, config);
            BasicConfiguration bc = scanConf.getJanusGraphConf();
            graph = JanusGraphFactory.open(bc.getConfiguration());
            tx = graph.buildTransaction().readOnly().vertexCacheSize(200).start();
        }

        TypeInspector getTypeInspector() {
            //Pre-load schema
            for (JanusGraphSchemaCategory sc : JanusGraphSchemaCategory.values()) {
                for (JanusGraphVertex k : QueryUtil.getVertices(tx, BaseKey.SchemaCategory, sc)) {
                    JanusGraphSchemaVertex s = (JanusGraphSchemaVertex) k;
                    if (sc.hasName()) {
                        String name = s.name();
                        Preconditions.checkNotNull(name);
                    }
                    TypeDefinitionMap dm = s.getDefinition();
                    Preconditions.checkNotNull(dm);
                    s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.OUT);
                    s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.IN);
                }
            }
            return tx;
        }

        public IDManager getIDManager() {
            return graph.getIDManager();
        }

        RelationReader getRelationReader() {
            return graph.getEdgeSerializer();
        }

        public void close() {
            tx.rollback();
            graph.close();
        }

        boolean getFilterPartitionedVertices() {
            return scanConf.get(ModifiableConfigurationHadoop.FILTER_PARTITIONED_VERTICES);
        }
    }
}
