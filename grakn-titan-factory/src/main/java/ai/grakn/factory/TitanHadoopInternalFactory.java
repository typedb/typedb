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

import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TitanHadoopInternalFactory extends AbstractInternalFactory<AbstractGraknGraph<HadoopGraph>, HadoopGraph> {
    private static final String CLUSTER_KEYSPACE = "titanmr.ioformat.conf.storage.cassandra.keyspace";
    private static final String INPUT_KEYSPACE = "cassandra.input.keyspace";
    private final Logger LOG = LoggerFactory.getLogger(TitanHadoopInternalFactory.class);

    TitanHadoopInternalFactory(String keyspace, String engineUrl, Properties properties) {
        super(keyspace, engineUrl, properties);

        properties.setProperty(CLUSTER_KEYSPACE, keyspace);
        properties.setProperty(INPUT_KEYSPACE, keyspace);
    }

    @Override
    boolean isClosed(HadoopGraph innerGraph) {
        return false;
    }

    @Override
    AbstractGraknGraph<HadoopGraph> buildGraknGraphFromTinker(HadoopGraph graph, boolean batchLoading) {
        throw new UnsupportedOperationException(ErrorMessage.CANNOT_PRODUCE_GRAPH.getMessage(HadoopGraph.class.getName()));
    }

    @Override
    HadoopGraph buildTinkerPopGraph(boolean batchLoading) {
        LOG.warn("Hadoop graph ignores parameter address [" + super.engineUrl + "]");
        return (HadoopGraph) GraphFactory.open(properties);
    }
}
