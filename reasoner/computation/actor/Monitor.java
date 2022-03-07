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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public class Monitor extends Actor<Monitor> {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    private final Registry registry;
    private boolean terminated;
    // TODO: Reactive elements from inside other actors shouldn't have been sent here over an actor boundary.
    private final Map<Reactive, Set<Reactive.Provider<?>>> paths;
    private final Map<Reactive, Set<ReactiveGraph>> reactiveGraphMembership;
    private final Map<Reactive, Integer> reactiveAnswers;
    private final Map<Reactive, Integer> reactiveFrontiers;
    private final Map<Reactive, ReactiveGraph> reactiveGraphRoots;
    private final Set<Reactive> finishedReactiveGraphRoots;
    private final Set<Reactive.Provider<?>> sources;
    private final Set<Reactive.Provider<?>> finishedSources;

    public Monitor(Driver<Monitor> driver, Registry registry) {
        super(driver, Monitor.class.getSimpleName()); this.registry = registry;
        this.paths = new HashMap<>();
        this.reactiveGraphMembership = new HashMap<>();
        this.reactiveAnswers = new HashMap<>();
        this.reactiveFrontiers = new HashMap<>();
        this.reactiveGraphRoots = new HashMap<>();
        this.finishedReactiveGraphRoots = new HashSet<>();
        this.sources = new HashSet<>();
        this.finishedSources = new HashSet<>();
    }

    private <R> void registerRoot(Driver<? extends Processor<R, ?, ?, ?>> processor, Reactive.Receiver.Finishable<R> root) {
        // We assume that this is called before the root has any connected paths registered
        // TODO: What if a new root is registered which shares paths with an already complete ReactiveGraph? It should be able to find all of its contained paths
        ReactiveGraph reactiveGraph = new ReactiveGraph(processor, root);
        ReactiveGraph existingRoot = reactiveGraphRoots.put(root, reactiveGraph);
        assert existingRoot == null;
        addToGraphs(root, set(reactiveGraph));
    }

    private <R> void registerPath(Reactive.Receiver<R> receiver, Reactive.Provider<R> provider) {
        paths.computeIfAbsent(receiver, p -> new HashSet<>()).add(provider);
        // We could be learning about a new receiver or provider or both.
        // Propagate any graphs the receiver belongs to to the provider.
        propagateReactiveGraphs(receiver);
    }

    private void propagateReactiveGraphs(Reactive reactive) {
        Set<ReactiveGraph> toPropagate;
        if (reactiveGraphRoots.containsKey(reactive)) toPropagate = set(reactiveGraphRoots.get(reactive));
        else toPropagate = reactiveGraphMembership.getOrDefault(reactive, set());
        if (!toPropagate.isEmpty()) {
            Set<Reactive.Provider<?>> providers = paths.getOrDefault(reactive, set());
            providers.forEach(child -> {
                // checkFinished(receiverGraphs); // TODO: In case a root connects to an already complete graph it should terminate straight away - we need to check for that which means we need to check eagerly here. Except we can't do this or we'll terminate straight away.
                if (addToGraphs(child, toPropagate)) propagateReactiveGraphs(child);
            });
        }
    }

    boolean addToGraphs(Reactive toAdd, Set<ReactiveGraph> reactiveGraphs) {
        Set<ReactiveGraph> providerGraphs = reactiveGraphMembership.computeIfAbsent(toAdd, r -> new HashSet<>());
        reactiveGraphs.forEach(g -> g.reactives.add(toAdd));
        return providerGraphs.addAll(reactiveGraphs);
    }

    private <R> void registerSource(Reactive.Provider<R> source) {
        sources.add(source);
    }

    private <R> void sourceFinished(Source<R> source) {
        finishedSources.add(source);
        reactiveGraphMembership.getOrDefault(source, set()).forEach(this::checkFinished);
    }

    private <R> void rootFinished(Reactive.Receiver.Finishable<R> root) {
        finishedReactiveGraphRoots.add(root);
        reactiveGraphMembership.getOrDefault(root, set()).forEach(this::checkFinished);
    }

    private <R> void createAnswer(int numCreated, Reactive.Provider<R> provider) {
        reactiveAnswers.putIfAbsent(provider, 0);
        reactiveAnswers.computeIfPresent(provider, (r, c) -> c + numCreated);
        // TODO: We shouldn't check finished on creating an answer
        reactiveGraphMembership.getOrDefault(provider, set()).forEach(this::checkFinished);
    }

    private <R> void consumeAnswer(Reactive.Receiver<R> receiver) {
        reactiveAnswers.putIfAbsent(receiver, 0);
        reactiveAnswers.computeIfPresent(receiver, (r, c) -> c - 1);
        reactiveGraphMembership.getOrDefault(receiver, set()).forEach(this::checkFinished);
    }

    private void forkFrontier(int numForks, Reactive forker) {
        reactiveFrontiers.putIfAbsent(forker, 0);
        reactiveFrontiers.computeIfPresent(forker, (r, c) -> c + numForks);
    }

    private void joinFrontier(Reactive joiner) {
        reactiveFrontiers.putIfAbsent(joiner, 0);
        reactiveFrontiers.computeIfPresent(joiner, (r, c) -> c - 1);
        reactiveGraphMembership.getOrDefault(joiner, set()).forEach(this::checkFinished);
    }

    void checkFinished(ReactiveGraph reactiveGraph) {
        // TODO: Include that the root must be pulling
        if (reactiveGraph.nestedRootsFinished(reactiveGraphRoots.keySet(), finishedReactiveGraphRoots)
                && reactiveGraph.sourcesFinished(sources, finishedSources)
                && reactiveGraph.activeAnswers(reactiveAnswers) == 0
                && reactiveGraph.activeFrontiers(reactiveFrontiers) == 0) {
            reactiveGraph.setFinished();
        }
    }

    private static class ReactiveGraph {

        private final Driver<? extends Processor<?, ?, ?, ?>> rootProcessor;
        private final Reactive.Receiver.Finishable<?> root;
        private final Set<Reactive> reactives;

        ReactiveGraph(Driver<? extends Processor<?,?,?,?>> rootProcessor, Reactive.Receiver.Finishable<?> root) {
            this.rootProcessor = rootProcessor;
            this.root = root;
            this.reactives = new HashSet<>();
        }

        private void setFinished() {
            rootProcessor.execute(actor -> actor.onFinished(root));
        }

        private boolean nestedRootsFinished(Set<Reactive> reactiveGraphs,
                                            Set<Reactive> finishedReactiveGraphRoots) {
            Set<Reactive> unfinished = new HashSet<>(reactiveGraphs);
            unfinished.retainAll(reactives);
            unfinished.removeAll(finishedReactiveGraphRoots);
            return unfinished.equals(set(root));
        }

        private boolean sourcesFinished(Set<Reactive.Provider<?>> sources, Set<Reactive.Provider<?>> finishedSources) {
            Set<Reactive> unfinished = new HashSet<>(sources);
            unfinished.retainAll(reactives);
            unfinished.removeAll(finishedSources);
            return unfinished.isEmpty();
        }

        int activeAnswers(Map<Reactive, Integer> reactiveAnswers) {
            int count = 0;
            for (Reactive reactive : reactives) count += reactiveAnswers.getOrDefault(reactive, 0);
            assert count >= 0;
            return count;
        }

        int activeFrontiers(Map<Reactive, Integer> reactiveFrontiers) {
            int count = 0;
            for (Reactive reactive : reactives) count += reactiveFrontiers.getOrDefault(reactive, 0);
            assert count >= 0;
            return count;
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
                LOG.debug("Monitor   interrupted by TypeDB exception: {}", e.getMessage());
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
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

        public void joinFrontiers2(Reactive joiner) {
            // TODO: The only usage shouldn't be needed
        }
    }

}
