// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    public JanusGraphHadoopSetupImpl(final Configuration config) {
        scanConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, config);
        BasicConfiguration bc = scanConf.getJanusGraphConf();
        graph = (StandardJanusGraph) JanusGraphFactory.open(bc);
        tx = (StandardJanusGraphTx)graph.buildTransaction().readOnly().vertexCacheSize(200).start();
    }

    @Override
    public TypeInspector getTypeInspector() {
        //Pre-load schema
        for (JanusGraphSchemaCategory sc : JanusGraphSchemaCategory.values()) {
            for (JanusGraphVertex k : QueryUtil.getVertices(tx, BaseKey.SchemaCategory, sc)) {
                assert k instanceof JanusGraphSchemaVertex;
                JanusGraphSchemaVertex s = (JanusGraphSchemaVertex)k;
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