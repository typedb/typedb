/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.hadoop.formats.util.input;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphFactory;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.graphdb.database.RelationReader;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.query.QueryUtil;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.TypeDefinitionMap;
import grakn.core.graph.graphdb.types.TypeInspector;
import grakn.core.graph.graphdb.types.system.BaseKey;
import grakn.core.graph.graphdb.types.system.BaseLabel;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;
import grakn.core.graph.hadoop.config.JanusGraphHadoopConfiguration;
import grakn.core.graph.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class JanusGraphHadoopSetupImpl implements JanusGraphHadoopSetup {

    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    public static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);

    private final ModifiableHadoopConfiguration scanConf;
    private final StandardJanusGraph graph;
    private final StandardJanusGraphTx tx;

    public JanusGraphHadoopSetupImpl(Configuration config) {
        scanConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, config);
        BasicConfiguration bc = scanConf.getJanusGraphConf();
        graph = JanusGraphFactory.open(bc.getConfiguration());
        tx = graph.buildTransaction().readOnly().vertexCacheSize(200).start();
    }

    @Override
    public TypeInspector getTypeInspector() {
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

    @Override
    public SystemTypeInspector getSystemTypeInspector() {
        return new SystemTypeInspector() {
            @Override
            public boolean isSystemType(long typeId) {
                return IDManager.isSystemRelationTypeId(typeId);
            }

            @Override
            public boolean isVertexExistsSystemType(long typeId) {
                return typeId == BaseKey.VertexExists.longId();
            }

            @Override
            public boolean isVertexLabelSystemType(long typeId) {
                return typeId == BaseLabel.VertexLabelEdge.longId();
            }

            @Override
            public boolean isTypeSystemType(long typeId) {
                return typeId == BaseKey.SchemaCategory.longId() ||
                        typeId == BaseKey.SchemaDefinitionProperty.longId() ||
                        typeId == BaseKey.SchemaDefinitionDesc.longId() ||
                        typeId == BaseKey.SchemaName.longId() ||
                        typeId == BaseLabel.SchemaDefinitionEdge.longId();
            }
        };
    }

    @Override
    public IDManager getIDManager() {
        return graph.getIDManager();
    }

    @Override
    public RelationReader getRelationReader() {
        return graph.getEdgeSerializer();
    }

    @Override
    public void close() {
        tx.rollback();
        graph.close();
    }

    @Override
    public boolean getFilterPartitionedVertices() {
        return scanConf.get(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES);
    }
}