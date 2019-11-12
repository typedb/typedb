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
 *
 */

package grakn.core.graql.executor;

import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.executor.QueryExecutor;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.WriteExecutorFactory;
import grakn.core.kb.graql.executor.property.PropertyExecutorFactory;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.statistics.KeyspaceStatistics;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

public class ExecutorFactory {

    private final ConceptManager conceptManager;
    private HadoopGraph hadoopGraph;
    private KeyspaceStatistics keyspaceStatistics;

    public ExecutorFactory(ConceptManager conceptManager, HadoopGraph hadoopGraph, KeyspaceStatistics keyspaceStatistics) {
        this.conceptManager = conceptManager;
        this.hadoopGraph = hadoopGraph;
        this.keyspaceStatistics = keyspaceStatistics;
    }

    public ComputeExecutor compute() {
        return new ComputeExecutor(conceptManager, this, hadoopGraph, keyspaceStatistics);
    }

    public QueryExecutor read() {
        return null;
    }

    public WriteExecutorFactory write(Transaction transaction, PropertyExecutorFactory propertyExecutorFactory) {
        return new WriteExecutorFactoryImpl(transaction, conceptManager, propertyExecutorFactory);
    }
}
