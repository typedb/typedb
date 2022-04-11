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
import com.vaticle.typedb.core.reasoner.controller.Controller;
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

public class Monitor extends Actor<Monitor> {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    private final Registry registry;
    private boolean terminated;
    private final Map<Reactive.Identifier<?, ?>, ReactiveNode> reactiveNodes;

    public Monitor(Driver<Monitor> driver, Registry registry) {
        super(driver, Monitor.class::getSimpleName); this.registry = registry;  // TODO: Does it matter that this depends upon the Registry?
        this.reactiveNodes = new HashMap<>();
    }

    private ReactiveNode getNode(Reactive.Identifier<?, ?> reactive) {
        return reactiveNodes.get(reactive);
    }

    private ReactiveNode getOrCreateNode(Reactive.Identifier<?, ?> reactive) {
        return reactiveNodes.computeIfAbsent(reactive, p -> new ReactiveNode());
    }

    private void putNode(Reactive.Identifier<?, ?> reactive, ReactiveNode reactiveNode) {
        ReactiveNode exists = reactiveNodes.put(reactive, reactiveNode);
        assert exists == null;
    }

    public <R> void registerRoot(Driver<? extends ReactiveBlock<R, ?, ?, ?>> reactiveBlock, Reactive.Identifier<?, ?> root) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerRoot(root, driver()));
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the root, or a duplicate node will be created.
        RootNode rootNode = new RootNode(root);
        putNode(root, rootNode);
        ReactiveGraph reactiveGraph = new ReactiveGraph(reactiveBlock, rootNode, driver());
        rootNode.setGraph(reactiveGraph);
    }

    public void rootFinalised(Reactive.Identifier<?, ?> root) {
        // TODO: Improve this by having two separate finish states for a negation so that it can be finished as a
        //  root prior to incrementing the in-flight answer count. In this way either it receives a message from the
        //  monitor to say it's done, or it sends the monitor a message to tell that it's finished
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.rootFinalised(root, driver()));
        if (terminated) return;
        RootNode rootNode = getNode(root).asRoot();
        rootNode.setFinished();
        rootNode.graphMemberships().forEach(ReactiveGraph::checkFinished);
    }

    public void registerSource(Reactive.Identifier<?, ?> source) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerSource(source, driver()));
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the source, or a duplicate node will be created.
        assert reactiveNodes.get(source) == null;
        SourceNode sourceNode = new SourceNode();
        putNode(source, sourceNode);
    }

    public void sourceFinished(Reactive.Identifier<?, ?> source) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.sourceFinished(source, driver()));
        if (terminated) return;
        ReactiveNode sourceNode = getNode(source);
        sourceNode.asSource().setFinished();
        sourceNode.graphMemberships().forEach(ReactiveGraph::checkFinished);
    }

    public void registerPath(Reactive.Identifier<?, ?> subscriber, Reactive.Identifier<?, ?> publisher) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerPath(subscriber, publisher, driver()));
        if (terminated) return;
        ReactiveNode subscriberNode = reactiveNodes.computeIfAbsent(subscriber, n -> new ReactiveNode());
        ReactiveNode publisherNode = reactiveNodes.computeIfAbsent(publisher, n -> new ReactiveNode());
        subscriberNode.addPublisher(publisherNode);
        // We could be learning about a new subscriber or publisher or both.
        // Propagate any graphs the subscriber belongs to to the publisher.
        subscriberNode.propagateReactiveGraphs();
        subscriberNode.graphMemberships().forEach(ReactiveGraph::checkFinished);  // In case a root connects to an already complete graph it should terminate straight away  TODO: very inefficient
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
        subscriberNode.graphMemberships().forEach(ReactiveGraph::checkFinished);
    }

    private static class ReactiveGraph {  // TODO: A graph can effectively be a source node within another graph

        private final Driver<? extends ReactiveBlock<?, ?, ?, ?>> rootReactiveBlock;
        private final RootNode rootNode;
        private final Driver<Monitor> monitor;
        private final Set<ReactiveNode> reactives;
        private final Set<SourceNode> nestedSources;
        private boolean finished;

        ReactiveGraph(Driver<? extends ReactiveBlock<?, ?, ?, ?>> rootReactiveBlock, RootNode rootNode, Driver<Monitor> monitor) {
            this.rootReactiveBlock = rootReactiveBlock;
            this.rootNode = rootNode;
            this.monitor = monitor;
            this.reactives = new HashSet<>();
            this.nestedSources = new HashSet<>();
            this.finished = false;
        }

        private void setFinished() {
            finished = true;
        }

        private void finishRootNode() {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.finishRootNode(rootNode.root(), monitor));
            rootReactiveBlock.execute(actor -> actor.onFinished(rootNode.root()));
        }

        public void addReactiveNode(ReactiveNode toAdd) {
            if (!toAdd.equals(rootNode)) {
                if (toAdd.isSource()) nestedSources.add(toAdd.asSource());
                else reactives.add(toAdd);
            }
        }

        private boolean sourcesFinished() {
            for (SourceNode nestedSource : nestedSources) if (!nestedSource.isFinished()) return false;
            return true;
        }

        long activeAnswers() {
            long count = 0;
            count -= rootNode.answersConsumed();
            for (ReactiveNode reactive : reactives) count += reactive.answersCreated(this) - reactive.answersConsumed();
            for (SourceNode source : nestedSources) count += source.answersCreated(this);
            assert count >= 0;
            return count;
        }

        long activeFrontiers() {
            long count = 0;
            count += rootNode.frontierForks();
            for (ReactiveNode reactive : reactives) count += reactive.frontierForks() - reactive.frontierJoins(this);
            for (SourceNode source : nestedSources) count -= source.frontierJoins(this);
            assert count >= 0;
            return count;
        }

        void checkFinished() {
            if (!finished && sourcesFinished() && activeAnswers() == 0 && activeFrontiers() == 0) finishRootNode();
        }

        public Driver<? extends ReactiveBlock<?, ?, ?, ?>> rootReactiveBlock() {
            return rootReactiveBlock;
        }
    }

    private static class ReactiveNode {

        private final Set<ReactiveGraph> graphMemberships;
        private final Set<ReactiveNode> publishers;
        private final Map<ReactiveGraph, Set<ReactiveNode>> subscribersByGraph;
        private long answersCreated;
        private long answersConsumed;

        ReactiveNode() {
            this.graphMemberships = new HashSet<>();
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
            if (publishers.size() == 0) return 1;
            else return publishers.size();
        }

        protected void createAnswer() {
            answersCreated += 1;
        }

        protected void consumeAnswer() {
            answersConsumed += 1;
        }

        public Set<ReactiveNode> publishers() {
            return publishers;
        }

        public void addPublisher(ReactiveNode publisherNode) {
            boolean isNew = publishers.add(publisherNode);
            assert isNew;
        }

        public boolean addSubscriberGraphs(ReactiveNode subscriber, Set<ReactiveGraph> reactiveGraphs) {
            reactiveGraphs.forEach(g -> subscribersByGraph.computeIfAbsent(g, n -> new HashSet<>()).add(subscriber));
            return addGraphMemberships(reactiveGraphs);
        }

        public Set<ReactiveGraph> graphMemberships() {
            return graphMemberships;
        }

        public boolean addGraphMemberships(Set<ReactiveGraph> reactiveGraphs) {
            reactiveGraphs.forEach(g -> g.addReactiveNode(this));  // TODO: Should we skip this step for roots?
            return graphMemberships.addAll(reactiveGraphs);
        }

        public void propagateReactiveGraphs() {
            if (!graphsToPropagate().isEmpty()) {
                publishers().forEach(publisher -> {
                    if (publisher.addSubscriberGraphs(this, graphsToPropagate())) publisher.propagateReactiveGraphs();
                });
            }
        }

        public boolean isRoot() {
            return false;
        }

        public RootNode asRoot() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        public boolean isSource() {
            return false;
        }

        public SourceNode asSource() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        protected Set<ReactiveGraph> graphsToPropagate() {
            return graphMemberships();
        }
    }

    private static class SourceNode extends ReactiveNode {

        private boolean finished;

        SourceNode() {
            this.finished = false;
        }

        public boolean isFinished() {
            return finished;
        }

        protected void setFinished() {
            finished = true;
        }

        @Override
        public boolean isSource() {
            return true;
        }

        @Override
        public SourceNode asSource() {
            return this;
        }
    }

    private static class RootNode extends SourceNode {

        private final Reactive.Identifier<?, ?> root;
        private ReactiveGraph reactiveGraph;

        RootNode(Reactive.Identifier<?, ?> root) {
            super();
            this.root = root;
        }

        public Reactive.Identifier<?, ?> root() {
            return root;
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public RootNode asRoot() {
            return this;
        }

        public void setGraph(ReactiveGraph reactiveGraph) {
            assert this.reactiveGraph == null;
            this.reactiveGraph = reactiveGraph;
            addGraphMemberships(set(reactiveGraph));
        }

        public ReactiveGraph graph() {
            assert reactiveGraph != null;
            return reactiveGraph;
        }

        @Override
        protected Set<ReactiveGraph> graphsToPropagate() {
            return set(graph());
        }

        @Override
        protected void setFinished() {
            super.setFinished();
            graph().setFinished();
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
        reactiveNodes.values().forEach(node -> {
            if (node.isRoot()) node.asRoot().graph().rootReactiveBlock().execute(actor -> actor.exception(e));
        });
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
    }

}
