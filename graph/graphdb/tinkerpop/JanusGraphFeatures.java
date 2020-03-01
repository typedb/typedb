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

package grakn.core.graph.graphdb.tinkerpop;

import grakn.core.graph.diskstorage.keycolumnvalue.StoreFeatures;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * Blueprint's features of a JanusGraph.
 */

public class JanusGraphFeatures implements Graph.Features {

    private final GraphFeatures graphFeatures;
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures;

    private final StandardJanusGraph graph;

    private JanusGraphFeatures(StandardJanusGraph graph, StoreFeatures storageFeatures) {
        graphFeatures = new JanusGraphGeneralFeatures(storageFeatures.supportsPersistence());
        vertexFeatures = new JanusGraphVertexFeatures();
        edgeFeatures = new JanusGraphEdgeFeatures();
        this.graph = graph;
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public static JanusGraphFeatures getFeatures(StandardJanusGraph graph, StoreFeatures storageFeatures) {
        return new JanusGraphFeatures(graph, storageFeatures);
    }

    private static class JanusGraphDataTypeFeatures implements DataTypeFeatures {

        @Override
        public boolean supportsMapValues() {
            return true;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }

    private static class JanusGraphVariableFeatures extends JanusGraphDataTypeFeatures implements VariableFeatures {
    }

    private static class JanusGraphGeneralFeatures extends JanusGraphDataTypeFeatures implements GraphFeatures {

        private final boolean persists;

        private JanusGraphGeneralFeatures(boolean persists) {
            this.persists = persists;
        }

        @Override
        public VariableFeatures variables() {
            return new JanusGraphVariableFeatures();
        }

        @Override
        public boolean supportsComputer() {
            return true;
        }

        @Override
        public boolean supportsPersistence() {
            return persists;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return true;
        }
    }

    private static class JanusGraphVertexPropertyFeatures extends JanusGraphDataTypeFeatures implements VertexPropertyFeatures {

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }
    }

    private static class JanusGraphEdgePropertyFeatures extends JanusGraphDataTypeFeatures implements EdgePropertyFeatures {

    }

    private class JanusGraphVertexFeatures implements VertexFeatures {

        @Override
        public VertexProperty.Cardinality getCardinality(String key) {
            StandardJanusGraphTx tx = (StandardJanusGraphTx) JanusGraphFeatures.this.graph.newTransaction();
            try {
                if (!tx.containsPropertyKey(key)) {
                    return tx.getConfiguration().getAutoSchemaMaker().defaultPropertyCardinality(key).convert();
                }
                return tx.getPropertyKey(key).cardinality().convert();
            } finally {
                tx.rollback();
            }
        }

        @Override
        public VertexPropertyFeatures properties() {
            return new JanusGraphVertexPropertyFeatures();
        }

        @Override
        public boolean supportsNumericIds() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    private static class JanusGraphEdgeFeatures implements EdgeFeatures {
        @Override
        public EdgePropertyFeatures properties() {
            return new JanusGraphEdgePropertyFeatures();
        }

        @Override
        public boolean supportsCustomIds() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return false;
        }
    }

}
