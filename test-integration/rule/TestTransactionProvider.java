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

package grakn.core.rule;

import grakn.core.concept.manager.ConceptListenerImpl;
import grakn.core.concept.manager.ConceptManagerImpl;
import grakn.core.concept.manager.ConceptNotificationChannelImpl;
import grakn.core.concept.structure.ElementFactory;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graql.executor.ExecutorFactoryImpl;
import grakn.core.graql.gremlin.TraversalPlanFactoryImpl;
import grakn.core.graql.reasoner.atom.PropertyAtomicFactory;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.cache.RuleCacheImpl;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.manager.ConceptListener;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.graql.executor.ExecutorFactory;
import grakn.core.kb.graql.gremlin.TraversalPlanFactory;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.server.AttributeManager;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.TransactionProvider;
import grakn.core.kb.server.cache.KeyspaceSchemaCache;
import grakn.core.kb.server.cache.TransactionCache;
import grakn.core.kb.server.statistics.KeyspaceStatistics;
import grakn.core.kb.server.statistics.UncomittedStatisticsDelta;
import grakn.core.server.session.TransactionImpl;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

import java.util.concurrent.locks.ReadWriteLock;

public class TestTransactionProvider implements TransactionProvider {
    private final StandardJanusGraph graph;
    private final HadoopGraph hadoopGraph;
    private final KeyspaceSchemaCache keyspaceSchemaCache;
    private final KeyspaceStatistics keyspaceStatistics;
    private final AttributeManager attributeManager;
    private ReadWriteLock graphLock;
    private final long typeShardThreshold;

    public TestTransactionProvider(StandardJanusGraph graph, HadoopGraph hadoopGraph,
                                   KeyspaceSchemaCache keyspaceSchemaCache, KeyspaceStatistics keyspaceStatistics,
                                   AttributeManager attributeManager, ReadWriteLock graphLock, long typeShardThreshold) {
        this.graph = graph;
        this.hadoopGraph = hadoopGraph;
        this.keyspaceSchemaCache = keyspaceSchemaCache;
        this.keyspaceStatistics = keyspaceStatistics;
        this.attributeManager = attributeManager;
        this.graphLock = graphLock;
        this.typeShardThreshold = typeShardThreshold;
    }

    @Override
    public Transaction newTransaction(Session session) {
        return null;
    }

    public TestTransaction testTransaction(Session session) {
        return new TestTransaction(session);
    }


    class TestTransaction {
        private final ConceptNotificationChannel conceptNotificationChannel;
        private final TransactionCache transactionCache;
        private final UncomittedStatisticsDelta statisticsDelta;
        private final StandardJanusGraphTx janusGraphTransaction;
        private final JanusTraversalSourceProvider janusTraversalSourceProvider;
        private final ElementFactory elementFactory;
        private final ConceptManagerImpl conceptManager;
        private final TraversalPlanFactory traversalPlanFactory;

        private final MultilevelSemanticCache queryCache;
        private final ExecutorFactoryImpl executorFactory;
        private final RuleCacheImpl ruleCache;
        private final PropertyAtomicFactory propertyAtomicFactory;
        private final ReasonerQueryFactory reasonerQueryFactory;
        private final ConceptListener conceptObserver;
        // actual transaction
        Transaction tx;

        // factories, etc.

        public TestTransaction(Session session) {
            // Data structures
            this.conceptNotificationChannel = new ConceptNotificationChannelImpl();
            this.transactionCache = new TransactionCache(keyspaceSchemaCache);
            this.statisticsDelta = new UncomittedStatisticsDelta();

            // Janus elements
            janusGraphTransaction = graph.newThreadBoundTransaction();
            janusTraversalSourceProvider = new JanusTraversalSourceProvider(janusGraphTransaction);
            elementFactory = new ElementFactory(janusGraphTransaction, janusTraversalSourceProvider);

            // Grakn elements
            conceptManager = new ConceptManagerImpl(elementFactory, transactionCache, conceptNotificationChannel, attributeManager);
            traversalPlanFactory = new TraversalPlanFactoryImpl(janusTraversalSourceProvider, conceptManager, typeShardThreshold, keyspaceStatistics);
            executorFactory = new ExecutorFactoryImpl(conceptManager, hadoopGraph, keyspaceStatistics, traversalPlanFactory, null);
            ruleCache = new RuleCacheImpl(conceptManager);
            queryCache = new MultilevelSemanticCache(executorFactory, traversalPlanFactory);

            propertyAtomicFactory = new PropertyAtomicFactory(conceptManager, ruleCache, queryCache, keyspaceStatistics);
            reasonerQueryFactory = new ReasonerQueryFactory(conceptManager, queryCache, ruleCache, executorFactory, propertyAtomicFactory, traversalPlanFactory);
            executorFactory.setReasonerQueryFactory(reasonerQueryFactory);
            propertyAtomicFactory.setReasonerQueryFactory(reasonerQueryFactory);
            ruleCache.setReasonerQueryFactory(reasonerQueryFactory);

            tx = new TransactionImpl(
                    session, janusGraphTransaction, conceptManager,
                    janusTraversalSourceProvider, transactionCache, queryCache, ruleCache, statisticsDelta,
                    executorFactory, traversalPlanFactory, reasonerQueryFactory,
                    graphLock, typeShardThreshold
            );

            conceptObserver = new ConceptListenerImpl(transactionCache, queryCache, ruleCache, statisticsDelta, attributeManager, janusGraphTransaction.toString());
            conceptNotificationChannel.subscribe(conceptObserver);
        }
    }
}
