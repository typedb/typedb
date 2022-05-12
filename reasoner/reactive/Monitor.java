/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.controller.Registry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Monitor extends Actor<Monitor> {

    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);
    private final Registry registry;
    private boolean terminated;
    private final Map<Reactive.Identifier<?, ?>, ReactiveNode> reactiveNodes;

    public Monitor(Driver<Monitor> driver, Registry registry) {
        super(driver, Monitor.class::getSimpleName);
        this.registry = registry;  // TODO: Does it matter that this depends upon the Registry?
        this.reactiveNodes = new HashMap<>();
    }

    private ReactiveNode getNode(Reactive.Identifier<?, ?> reactive) {
        return reactiveNodes.get(reactive);
    }

    private ReactiveNode getOrCreateNode(Reactive.Identifier<?, ?> reactive) {
        return reactiveNodes.computeIfAbsent(reactive, p -> new ReactiveNode(reactive));
    }

    private void putNode(Reactive.Identifier<?, ?> reactive, ReactiveNode reactiveNode) {
        ReactiveNode exists = reactiveNodes.put(reactive, reactiveNode);
        assert exists == null;
    }

    public <R> void registerRoot(Driver<? extends AbstractReactiveBlock<R, ?, ?, ?>> reactiveBlock, Reactive.Identifier<?, ?> root) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerRoot(root, driver()));
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the root, or a duplicate node will be created.
        RootNode rootNode = new RootNode(root);
        putNode(root, rootNode);
        ReactiveGraph reactiveGraph = new ReactiveGraph(reactiveBlock, rootNode, driver());
        rootNode.setGraph(reactiveGraph);
    }

    public void rootFinished(Reactive.Identifier<?, ?> root) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.rootFinalised(root, driver()));
        if (terminated) return;
        RootNode rootNode = getNode(root).asRoot();
        rootNode.graph().setFinished();
        rootNode.setFinished();
    }

    public void registerSource(Reactive.Identifier<?, ?> source) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerSource(source, driver()));
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the source, or a duplicate node will be created.
        assert reactiveNodes.get(source) == null;
        putNode(source, new SourceNode(source));
    }

    public void sourceFinished(Reactive.Identifier<?, ?> source) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.sourceFinished(source, driver()));
        if (terminated) return;
        getNode(source).asSource().setFinished();
    }

    public void registerPath(Reactive.Identifier<?, ?> subscriber, Reactive.Identifier<?, ?> publisher) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerPath(subscriber, publisher, driver()));
        if (terminated) return;
        ReactiveNode subscriberNode = reactiveNodes.computeIfAbsent(subscriber, n -> new ReactiveNode(subscriber));
        ReactiveNode publisherNode = reactiveNodes.computeIfAbsent(publisher, n -> new ReactiveNode(publisher));
        subscriberNode.addPublisher(publisherNode);
        // We could be learning about a new subscriber or publisher or both.
        // Propagate any graphs the subscriber belongs to to the publisher.
        subscriberNode.propagateReactiveGraphs(subscriberNode.activeUpstreamGraphMemberships());
    }

    public void createAnswer(Reactive.Identifier<?, ?> publisher) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.createAnswer(publisher, driver()));
        if (terminated) return;
        getOrCreateNode(publisher).createAnswer();
    }

    public void consumeAnswer(Reactive.Identifier<?, ?> subscriber) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.consumeAnswer(subscriber, driver()));
        if (terminated) return;
        ReactiveNode subscriberNode = getOrCreateNode(subscriber);
        subscriberNode.consumeAnswer();
    }

    private static class ReactiveGraph {  // TODO: A graph can effectively be a source node within another graph

        private final Driver<? extends AbstractReactiveBlock<?, ?, ?, ?>> rootReactiveBlock;
        private final RootNode rootNode;
        private final Driver<Monitor> monitor;
        private final Set<SourceNode> activeSources;
        private boolean finished;
        private long activeFrontiers;
        private long activeAnswers;

        ReactiveGraph(Driver<? extends AbstractReactiveBlock<?, ?, ?, ?>> rootReactiveBlock, RootNode rootNode,
                      Driver<Monitor> monitor) {
            this.rootReactiveBlock = rootReactiveBlock;
            this.rootNode = rootNode;
            this.monitor = monitor;
            this.activeSources = new HashSet<>();
            this.finished = false;
            this.activeFrontiers = 1;
            this.activeAnswers = 0;
        }

        private void setFinished() {
            finished = true;
        }

        private void finishRootNode() {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.finishRootNode(rootNode.root(), monitor));
            rootReactiveBlock.execute(actor -> actor.onFinished(rootNode.root()));
        }

        void checkFinished() {
            if (!finished && activeSources.isEmpty()){
                assert activeAnswers >= 0;
                assert activeFrontiers >= 0;
                if (activeAnswers == 0 && activeFrontiers == 0) {
                    setFinished();
                    finishRootNode();
                }
            }
        }

        public void updateAnswerCount(long delta) {
            activeAnswers += delta;
            if (delta < 0) checkFinished();
        }

        public void updateFrontiersCount(long delta) {
            activeFrontiers += delta;
            if (delta < 0) checkFinished();
        }

        public void addSource(SourceNode sourceNode) {
            activeSources.add(sourceNode);
        }

        public void sourceFinished(SourceNode sourceNode) {
            boolean contained = activeSources.remove(sourceNode);
            assert contained;
            checkFinished();
        }
    }

    private static class ReactiveNode {

        private final Set<ReactiveNode> publishers;
        protected final Map<ReactiveGraph, Set<ReactiveNode>> subscribersByGraph;
        protected final Reactive.Identifier<?, ?> reactive;
        private long answersCreated;
        private long answersConsumed;

        ReactiveNode(Reactive.Identifier<?, ?> reactive) {
            this.reactive = reactive;
            this.publishers = new HashSet<>();
            this.subscribersByGraph = new HashMap<>();
            this.answersCreated = 0;
            this.answersConsumed = 0;
        }

        public long answersCreated(ReactiveGraph reactiveGraph) {
            return answersCreated * subscribersByGraph.get(reactiveGraph).size();
        }

        public long answersConsumed() {
            return answersConsumed;
        }

        public long frontierJoins(ReactiveGraph reactiveGraph) {
            return subscribersByGraph.get(reactiveGraph).size();
        }

        public long frontierForks() {
            // TODO: We don't use this method in Source, which suggests this methods should belong in a sibling class
            //  to source rather than this parent class.
            if (publishers.size() == 0) return 1;
            else return publishers.size();
        }

        protected void logAnswerDelta(long delta, String methodName, ReactiveGraph graph) {
            LOG.debug("Answers {} from {} in Node {} for graph {}", delta, methodName, reactive, graph.hashCode());
        }

        protected void createAnswer() {
            answersCreated += 1;
            // TODO: We should remove the inactive graphs from subscribersByGraph and graphMemberships. In fact we should just use the one map and use the keys for the graph memberships
            subscribersByGraph.forEach((graph, subs) -> {
                logAnswerDelta(subs.size(), "createAnswer", graph);
                graph.updateAnswerCount(subs.size());
            });
        }

        protected void consumeAnswer() {
            answersConsumed += 1;
            LOG.debug("Answer consumed by {}", reactive);
            iterate(activeDownstreamGraphMemberships()).forEachRemaining(graph -> {
                logAnswerDelta(-1, "consumeAnswer", graph);
                graph.updateAnswerCount(-1);
            });
        }

        public Set<ReactiveNode> publishers() {
            return publishers;
        }

        public void addPublisher(ReactiveNode publisherNode) {
            boolean isNew = publishers.add(publisherNode);
            assert isNew;
            if (publishers.size() > 1) {
                // TODO: The graphs we update for is wrong here? I've changed the above inequality
                iterate(activeUpstreamGraphMemberships()).forEachRemaining(graph -> {
                    LOG.debug("Frontiers 1 from addPublisher() in Node {} for graph {}", reactive, graph.hashCode());
                    graph.updateFrontiersCount(1);
                });
            }
        }

        public Set<ReactiveGraph> addGraphs(Set<ReactiveGraph> graphs, ReactiveNode subscriber) {
            Set<ReactiveGraph> newGraphsFromSubscriber = new HashSet<>();
            for (ReactiveGraph graph : graphs) {
                Set<ReactiveNode> subscribers = subscribersByGraph.get(graph);
                if (subscribers == null) {
                    subscribersByGraph.computeIfAbsent(graph, ignored -> new HashSet<>()).add(subscriber);
                    newGraphsFromSubscriber.add(graph);
                    synchroniseGraphCounts(graph);
                } else {
                    if (subscribers.add(subscriber)) {
                        LOG.debug("Frontiers -1 from addSubscriberForGraphs()");
                        graph.updateFrontiersCount(-1);
                    }
                }
            }
            return newGraphsFromSubscriber;
        }

        protected void synchroniseGraphCounts(ReactiveGraph graph) {
            logAnswerDelta(answersCreated(graph) - answersConsumed(), "synchroniseGraphCounts", graph);
            graph.updateAnswerCount(answersCreated(graph) - answersConsumed());
            if (frontierForks() - frontierJoins(graph) > 0) LOG.debug("Frontiers {} from synchroniseGraphCounts() in Node {} for graph {}", frontierForks() - frontierJoins(graph), reactive, graph.hashCode());
            graph.updateFrontiersCount(frontierForks() - frontierJoins(graph));
        }

        public Set<ReactiveGraph> activeDownstreamGraphMemberships() {
            return iterate(subscribersByGraph.keySet()).filter(g -> !g.finished).toSet();
        }

        protected void propagateReactiveGraphs(Set<ReactiveGraph> toPropagate) {
            if (!toPropagate.isEmpty()) {
                publishers().forEach(publisher -> {
                    publisher.propagateReactiveGraphs(publisher.addGraphs(toPropagate, this));
                });
            }
        }

        protected Set<ReactiveGraph> activeUpstreamGraphMemberships() {
            return activeDownstreamGraphMemberships();
        }

        public RootNode asRoot() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        public SourceNode asSource() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }
    }

    private static class SourceNode extends ReactiveNode {

        private boolean finished;

        SourceNode(Reactive.Identifier<?, ?> source) {
            super(source);
            this.finished = false;
        }

        public boolean isFinished() {
            return finished;
        }

        protected void setFinished() {
            finished = true;
            iterate(activeDownstreamGraphMemberships()).forEachRemaining(g -> g.sourceFinished(this));
        }

        @Override
        public SourceNode asSource() {
            return this;
        }

        @Override
        protected void synchroniseGraphCounts(ReactiveGraph graph) {
            graph.addSource(this);
            if (isFinished()) graph.sourceFinished(this);
            logAnswerDelta(answersCreated(graph), "synchroniseGraphCounts in Source", graph);
            graph.updateAnswerCount(answersCreated(graph));
            LOG.debug("Frontiers {} from synchroniseGraphCounts() in Source {} for graph {}", - frontierJoins(graph), reactive, graph.hashCode());  // TODO: Remove debug statements
            graph.updateFrontiersCount(-frontierJoins(graph));
        }
    }

    private static class RootNode extends SourceNode {

        private ReactiveGraph reactiveGraph;

        RootNode(Reactive.Identifier<?, ?> root) {
            super(root);
        }

        public Reactive.Identifier<?, ?> root() {
            return reactive;
        }

        @Override
        public RootNode asRoot() {
            return this;
        }

        public void setGraph(ReactiveGraph reactiveGraph) {
            assert this.reactiveGraph == null;
            this.reactiveGraph = reactiveGraph;
            subscribersByGraph.computeIfAbsent(graph(), ignored -> new HashSet<>());
        }

        public ReactiveGraph graph() {
            assert reactiveGraph != null;
            return reactiveGraph;
        }

        @Override
        protected Set<ReactiveGraph> activeUpstreamGraphMemberships() {
            if (isFinished()) return set();
            else return set(graph());
        }

        @Override
        protected void synchroniseGraphCounts(ReactiveGraph graph) {
            assert !graph.equals(graph());
            super.synchroniseGraphCounts(graph);
        }

        protected void propagateReactiveGraphs(Set<ReactiveGraph> toPropagate) {
            if (!toPropagate.isEmpty()) {
                publishers().forEach(publisher -> {
                    Set<ReactiveGraph> toPropagateOn = new HashSet<>(toPropagate);
                    toPropagateOn.retainAll(activeUpstreamGraphMemberships());
                    publisher.propagateReactiveGraphs(publisher.addGraphs(toPropagateOn, this));
                });
            }
        }

    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Monitor interrupted by resource close: {}", e.getMessage());
                registry.terminate(e);
                return;
            } else {
                LOG.debug("Monitor interrupted by TypeDB exception: {}", e.getMessage());
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
        // TODO: Do we need to send the exception to anywhere else if we already terminate the registry?
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
    }

}
