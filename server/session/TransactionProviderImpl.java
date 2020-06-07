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
 *
 */

package grakn.core.server.session;

import grakn.core.concept.manager.ConceptListenerImpl;
import grakn.core.concept.manager.ConceptManagerImpl;
import grakn.core.concept.manager.ConceptNotificationChannelImpl;
import grakn.core.concept.structure.ElementFactory;
import grakn.core.core.JanusTraversalSourceProvider;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graql.executor.ExecutorFactoryImpl;
import grakn.core.graql.executor.TraversalExecutorImpl;
import grakn.core.graql.executor.property.PropertyExecutorFactoryImpl;
import grakn.core.graql.planning.TraversalPlanFactoryImpl;
import grakn.core.graql.reasoner.atom.PropertyAtomicFactory;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.reasoner.cache.RuleCacheImpl;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.graql.executor.TraversalExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutorFactory;
import grakn.core.kb.graql.planning.gremlin.TraversalPlanFactory;
import grakn.core.kb.keyspace.AttributeManager;
import grakn.core.kb.keyspace.KeyspaceSchemaCache;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.TransactionProvider;
import grakn.core.kb.server.cache.TransactionCache;
import grakn.core.keyspace.StatisticsDeltaImpl;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * A component performing inversion of control, removing the creation of Transactions from the SessionImpl
 */
public class TransactionProviderImpl implements TransactionProvider {
    private final StandardJanusGraph graph;
    private final HadoopGraph hadoopGraph;
    private final KeyspaceSchemaCache keyspaceSchemaCache;
    private final KeyspaceStatistics keyspaceStatistics;
    private final AttributeManager attributeManager;
    private ReadWriteLock graphLock;
    private final long typeShardThreshold;

    public TransactionProviderImpl(StandardJanusGraph graph, HadoopGraph hadoopGraph,
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

    /*
    TODO - this is the centralised circular hairball dependency mess
     */
    @Override
    public Transaction newTransaction(Session session) {

        // Data structures
        ConceptNotificationChannel conceptNotificationChannel = new ConceptNotificationChannelImpl();
        TransactionCache transactionCache = new TransactionCache(keyspaceSchemaCache);
        StatisticsDeltaImpl statisticsDelta = new StatisticsDeltaImpl();

        // Janus elements
        JanusGraphTransaction janusGraphTransaction = graph.newThreadBoundTransaction();
        JanusTraversalSourceProvider janusTraversalSourceProvider = new JanusTraversalSourceProvider(janusGraphTransaction);
        ElementFactory elementFactory = new ElementFactory(janusGraphTransaction, janusTraversalSourceProvider);

        // Grakn elements
        PropertyExecutorFactory propertyExecutorFactory = new PropertyExecutorFactoryImpl();
        ConceptManager conceptManager = new ConceptManagerImpl(elementFactory, transactionCache, conceptNotificationChannel, attributeManager);
        TraversalPlanFactory traversalPlanFactory = new TraversalPlanFactoryImpl(janusTraversalSourceProvider, conceptManager, propertyExecutorFactory, typeShardThreshold, keyspaceStatistics);
        TraversalExecutor traversalExecutor = new TraversalExecutorImpl(traversalPlanFactory, conceptManager);
        ExecutorFactoryImpl executorFactory = new ExecutorFactoryImpl(conceptManager, hadoopGraph, keyspaceStatistics, traversalPlanFactory, traversalExecutor);
        RuleCacheImpl ruleCache = new RuleCacheImpl(conceptManager, keyspaceStatistics);
        MultilevelSemanticCache queryCache = new MultilevelSemanticCache(traversalPlanFactory, traversalExecutor);

        PropertyAtomicFactory propertyAtomicFactory = new PropertyAtomicFactory(conceptManager, ruleCache, queryCache, keyspaceStatistics);
        ReasonerQueryFactory reasonerQueryFactory = new ReasonerQueryFactory(conceptManager, queryCache, ruleCache, keyspaceStatistics, executorFactory, propertyAtomicFactory, traversalPlanFactory, traversalExecutor);
        executorFactory.setReasonerQueryFactory(reasonerQueryFactory);
        propertyAtomicFactory.setReasonerQueryFactory(reasonerQueryFactory);
        ruleCache.setReasonerQueryFactory(reasonerQueryFactory);

        TransactionImpl tx = new TransactionImpl(
                session, janusGraphTransaction, conceptManager,
                janusTraversalSourceProvider, transactionCache, queryCache, ruleCache, statisticsDelta,
                executorFactory, reasonerQueryFactory,
                graphLock, typeShardThreshold
        );

        ConceptListenerImpl conceptListener = new ConceptListenerImpl(transactionCache, queryCache, ruleCache, statisticsDelta, attributeManager, janusGraphTransaction.toString());
        conceptNotificationChannel.subscribe(conceptListener);

        return tx;
    }
}
