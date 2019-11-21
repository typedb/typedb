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

import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.executor.ComputeExecutor;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.executor.QueryExecutor;
import grakn.core.kb.graql.gremlin.TraversalPlanFactory;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.statistics.KeyspaceStatistics;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

public class ExecutorFactoryImpl implements ExecutorFactory {

    private final ConceptManager conceptManager;
    private HadoopGraph hadoopGraph;
    private KeyspaceStatistics keyspaceStatistics;
    private TraversalPlanFactory traversalPlanFactory;
    private ReasonerQueryFactory reasonerQueryFactory;

    public ExecutorFactoryImpl(ConceptManager conceptManager, HadoopGraph hadoopGraph, KeyspaceStatistics keyspaceStatistics, TraversalPlanFactory traversalPlanFactory, ReasonerQueryFactory reasonerQueryFactory) {
        this.conceptManager = conceptManager;
        this.hadoopGraph = hadoopGraph;
        this.keyspaceStatistics = keyspaceStatistics;
        this.traversalPlanFactory = traversalPlanFactory;
        this.reasonerQueryFactory = reasonerQueryFactory;
    }

    @Override
    public ComputeExecutor compute() {
        return new ComputeExecutorImpl(conceptManager, this, hadoopGraph, keyspaceStatistics);
    }

    @Override
    public QueryExecutor transactional(boolean infer) {
        return new QueryExecutorImpl(conceptManager, this, infer, traversalPlanFactory, reasonerQueryFactory);
    }

}
