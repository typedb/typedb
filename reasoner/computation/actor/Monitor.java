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

package com.vaticle.typedb.core.reasoner.computation.actor;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.Source;
import com.vaticle.typedb.core.reasoner.controller.Registry;
import com.vaticle.typedb.core.reasoner.utils.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
    // TODO: Reactive elements from inside other actors shouldn't have been sent here over an actor boundary.
    private final Map<Reactive, ReactiveNode> reactiveNodes;

    public Monitor(Driver<Monitor> driver, Registry registry) {
        super(driver, Monitor.class.getSimpleName()); this.registry = registry;
        this.reactiveNodes = new HashMap<>();
    }

    private ReactiveNode getNode(Reactive reactive) {
        return reactiveNodes.get(reactive);
    }

    private ReactiveNode getOrCreateNode(Reactive reactive) {
        return reactiveNodes.computeIfAbsent(reactive, p -> new ReactiveNode());
    }

    private void putNode(Reactive reactive, ReactiveNode reactiveNode) {
        ReactiveNode exists = reactiveNodes.put(reactive, reactiveNode);
        assert exists == null;
    }

    private <R> void registerRoot(Driver<? extends Processor<R, ?, ?, ?>> processor, Reactive.Receiver.Finishable<R> root) {
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the root, or a duplicate node will be created.
        RootNode rootNode = new RootNode(root);
        putNode(root, rootNode);
        ReactiveGraph reactiveGraph = new ReactiveGraph(processor, rootNode, driver());
        rootNode.setGraph(reactiveGraph);
        rootNode.addToGraphs(set(reactiveGraph));
    }

    private <R> void rootFinished(Reactive.Receiver.Finishable<R> root) {
        if (terminated) return;
        RootNode rootNode = getNode(root).asRoot();
        rootNode.setFinished();
        rootNode.graphMemberships().forEach(ReactiveGraph::checkFinished);
    }

    private <R> void registerSource(Reactive.Provider<R> source) {
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the source, or a duplicate node will be created.
        assert reactiveNodes.get(source) == null;
        SourceNode sourceNode = new SourceNode();
        putNode(source, sourceNode);
    }

    private <R> void sourceFinished(Source<R> source) {
        if (terminated) return;
        ReactiveNode sourceNode = getNode(source);
        sourceNode.asSource().setFinished();
        sourceNode.graphMemberships().forEach(ReactiveGraph::checkFinished);
    }

    private <R> void registerPath(Reactive.Receiver<R> receiver, Reactive.Provider<R> provider) {
        if (terminated) return;
        ReactiveNode receiverNode = reactiveNodes.computeIfAbsent(receiver, n -> new ReactiveNode());
        ReactiveNode providerNode = reactiveNodes.computeIfAbsent(provider, n -> new ReactiveNode());
        boolean isNew = receiverNode.addProvider(providerNode);
        assert isNew;
        // We could be learning about a new receiver or provider or both.
        // Propagate any graphs the receiver belongs to to the provider.
        receiverNode.propagateReactiveGraphs();
        receiverNode.graphMemberships().forEach(ReactiveGraph::checkFinished);  // In case a root connects to an already complete graph it should terminate straight away  TODO: very inefficient
    }

    private <R> void createAnswer(int numCreated, Reactive.Provider<R> provider) {
        if (terminated) return;
        getOrCreateNode(provider).createAnswer(numCreated);
    }

    private <R> void consumeAnswer(Reactive.Receiver<R> receiver) {
        if (terminated) return;
        ReactiveNode receiverNode = getOrCreateNode(receiver);
        receiverNode.consumeAnswer();
        receiverNode.graphMemberships().forEach(ReactiveGraph::checkFinished);
    }

    private void forkFrontier(int numForks, Reactive forker) {
        if (terminated) return;
        getOrCreateNode(forker).forkFrontier(numForks);
    }

    private void joinFrontier(Reactive joiner) {
        if (terminated) return;
        ReactiveNode joinerNode = getNode(joiner);
        joinerNode.joinFrontier();
        joinerNode.graphMemberships().forEach(ReactiveGraph::checkFinished);
    }

    private static class ReactiveGraph {  // TODO: A graph can effectively be a source node within another graph

        private final Driver<? extends Processor<?, ?, ?, ?>> rootProcessor;
        private final RootNode rootNode;
        private final Driver<Monitor> monitor;
        private final Set<ReactiveNode> reactives;
        private final Set<SourceNode> nestedSources;
        private boolean finished;

        ReactiveGraph(Driver<? extends Processor<?, ?, ?, ?>> rootProcessor, RootNode rootNode, Driver<Monitor> monitor) {
            this.rootProcessor = rootProcessor;
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
            rootProcessor.execute(actor -> actor.onFinished(rootNode.root()));
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
            count += rootNode.answersConsumed();
            for (ReactiveNode reactive : reactives) count += reactive.answersCreated() + reactive.answersConsumed();
            for (SourceNode source : nestedSources) count += source.answersCreated();
            assert count >= 0;
            return count;
        }

        long activeFrontiers() {
            long count = 0;
            count += rootNode.frontierForks();
            for (ReactiveNode reactive : reactives) count += reactive.frontierForks() + reactive.frontierJoins();
            for (SourceNode source : nestedSources) count += source.frontierJoins();
            assert count >= 0;
            return count;
        }

        void checkFinished() {
            if (!finished && sourcesFinished() && activeAnswers() == 0 && activeFrontiers() == 0) {
                setFinished();
                rootNode.setFinished();  // TODO: This duplication is why this graph and the root node should be the same object
                finishRootNode();
            }
        }

        public Driver<? extends Processor<?, ?, ?, ?>> rootProcessor() {
            return rootProcessor;
        }
    }

    private static class ReactiveNode {

        private final Set<ReactiveGraph> graphMemberships;
        private final Set<ReactiveNode> providers;
        private long answersCreated;
        private long answersConsumed;
        private long frontierForks;
        private long frontierJoins;

        ReactiveNode() {
            this.graphMemberships = new HashSet<>();
            this.providers = new HashSet<>();
            this.answersCreated = 0;
            this.answersConsumed = 0;
            this.frontierForks = 0;
            this.frontierJoins = 0;
        }

        public long answersCreated() {
            return answersCreated;
        }

        public long answersConsumed() {
            return answersConsumed;
        }

        public long frontierJoins() {
            return frontierJoins;
        }

        public long frontierForks() {
            return frontierForks;
        }

        protected void createAnswer(int numCreated) {
            answersCreated += numCreated;
        }

        protected void consumeAnswer() {
            answersConsumed -= 1;
        }

        public void forkFrontier(int numForks) {
            frontierForks += numForks;
        }

        public void joinFrontier() {
            frontierJoins -= 1;
        }

        public Set<ReactiveNode> providers() {
            return providers;
        }

        public boolean addProvider(ReactiveNode providerNode) {
            return providers.add(providerNode);
        }

        public Set<ReactiveGraph> graphMemberships() {
            return graphMemberships;
        }

        public boolean addGraphMemberships(Set<ReactiveGraph> reactiveGraphs) {
            return graphMemberships.addAll(reactiveGraphs);
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

        public void propagateReactiveGraphs() {
            if (!graphsToPropagate().isEmpty()) {
                providers().forEach(provider -> {
                    if (provider.addToGraphs(graphsToPropagate())) provider.propagateReactiveGraphs();
                });
            }
        }

        protected Set<ReactiveGraph> graphsToPropagate() {
            return graphMemberships();
        }

        public boolean addToGraphs(Set<ReactiveGraph> reactiveGraphs) {
            reactiveGraphs.forEach(g -> g.addReactiveNode(this));
            return addGraphMemberships(reactiveGraphs);
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

        private final Reactive.Receiver.Finishable<?> root;
        private ReactiveGraph reactiveGraph;

        RootNode(Reactive.Receiver.Finishable<?> root) {
            super();
            this.root = root;
        }

        public Reactive.Receiver.Finishable<?> root() {
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
            if (node.isRoot()) node.asRoot().graph().rootProcessor().execute(actor -> actor.exception(e));
        });
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
    }

    public static class MonitorRef {  // TODO: Rename to MonitorWrapper

        private final Driver<Monitor> monitor;

        public MonitorRef(Driver<Monitor> monitor) {
            this.monitor = monitor;
        }

        public <R> void registerRoot(Driver<? extends Processor<R, ?, ?, ?>> processor, Reactive.Receiver.Finishable<R> root) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerRoot(root, monitor));
            monitor.execute(actor -> actor.registerRoot(processor, root));
        }

        public <R> void rootFinished(Reactive.Receiver.Finishable<R> root) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.rootFinished(root, monitor));
            monitor.execute(actor -> actor.rootFinished(root));
        }

        public <R> void registerPath(Reactive.Receiver<R> receiver, @Nullable Reactive.Provider<R> provider) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerPath(receiver, provider, monitor));
            monitor.execute(actor -> actor.registerPath(receiver, provider));
        }

        public <R> void registerSource(Reactive.Provider<R> source) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.registerSource(source, monitor));
            monitor.execute(actor -> actor.registerSource(source));
        }

        public <R> void sourceFinished(Source<R> source) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.sourceFinished(source, monitor));
            monitor.execute(actor -> actor.sourceFinished(source));
        }

        public <R> void createAnswer(Reactive.Provider<R> provider) {
            createAnswer(1, provider);
        }

        public <R> void createAnswer(int numCreated, Reactive.Provider<R> provider) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.createAnswer(numCreated, provider, monitor));
            monitor.execute(actor -> actor.createAnswer(numCreated, provider));
        }

        public <R> void consumeAnswer(Reactive.Receiver<R> receiver) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.consumeAnswer(receiver, monitor));
            monitor.execute(actor -> actor.consumeAnswer(receiver));
        }

        public void forkFrontier(int numForks, Reactive forker) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.forkFrontier(numForks, forker, monitor));
            monitor.execute(actor -> actor.forkFrontier(numForks, forker));
        }

        public void joinFrontiers(Reactive joiner) {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.joinFrontier(joiner, monitor));
            monitor.execute(actor -> actor.joinFrontier(joiner));
        }

    }

}
