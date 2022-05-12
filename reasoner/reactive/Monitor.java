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
        putNode(root, new RootNode(root, reactiveBlock, driver()));
    }

    public void rootFinished(Reactive.Identifier<?, ?> root) {
        Tracer.getIfEnabled().ifPresent(tracer -> tracer.rootFinalised(root, driver()));
        if (terminated) return;
        getNode(root).asRoot().setFinished();
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
        addPath(subscriberNode, publisherNode);
    }

    private void addPath(ReactiveNode subscriberNode, ReactiveNode publisherNode) {
        subscriberNode.fork(publisherNode);
        // We could be learning about a new subscriber or publisher or both.
        // Propagate any roots the subscriber belongs to to the publisher.
        publisherNode.addRootsViaSubscriber(subscriberNode.activeUpstreamRoots(), subscriberNode);
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

    private static class ReactiveNode {

        private final Set<ReactiveNode> publishers;
        protected final Map<RootNode, Set<ReactiveNode>> downstreamRoots;
        protected final Reactive.Identifier<?, ?> reactive;
        private long answersCreated;
        private long answersConsumed;

        ReactiveNode(Reactive.Identifier<?, ?> reactive) {
            this.reactive = reactive;
            this.publishers = new HashSet<>();
            this.downstreamRoots = new HashMap<>();
            this.answersCreated = 0;
            this.answersConsumed = 0;
        }

        public long totalAnswersCreated(RootNode rootNode) {
            return answersCreated * downstreamRoots.get(rootNode).size();
        }

        public long totalAnswersConsumed() {
            return answersConsumed;
        }

        public long totalFrontierJoins(RootNode rootNode) {
            return downstreamRoots.get(rootNode).size();
        }

        public long totalFrontierForks() {
            // TODO: We don't use this method in Source, which suggests this methods should belong in a sibling class
            //  to source rather than this parent class.
            if (publishers.size() == 0) return 1;
            else return publishers.size();
        }

        protected void logAnswerDelta(long delta, String methodName, RootNode rootNode) {
            // TODO: Remove
            if (delta != 0) LOG.debug("Answers {} from {} in Node {} for rootNode {}", delta, methodName, reactive, rootNode.hashCode());
        }

        private void logFrontierDelta(long delta, String methodName, RootNode rootNode) {
            // TODO: Remove
            if (delta != 0) LOG.debug("Frontiers {} from {} in Node {} for rootNode {}", delta, methodName, reactive, rootNode.hashCode());
        }

        protected void createAnswer() {
            answersCreated += 1;
            // TODO: We should remove the inactive roots from subscribersByRoot
            downstreamRoots.forEach((root, subs) -> {
                logAnswerDelta(subs.size(), "createAnswer", root);
                root.updateAnswerCount(subs.size());
            });
        }

        protected void consumeAnswer() {
            answersConsumed += 1;
            LOG.debug("Answer consumed by {}", reactive);
            iterate(activeUpstreamRoots()).forEachRemaining(root -> {
                logAnswerDelta(-1, "consumeAnswer", root);
                root.updateAnswerCount(-1);
            });
        }

        public Set<ReactiveNode> publishers() {
            return publishers;
        }

        public void fork(ReactiveNode publisherNode) {
            boolean isNew = publishers.add(publisherNode);
            assert isNew;
            if (publishers.size() > 1) {
                // TODO: The roots we update for is wrong here? I've changed the above inequality
                iterate(activeUpstreamRoots()).forEachRemaining(root -> {
                    logFrontierDelta(1, "addPublisher", root);
                    root.updateFrontiersCount(1);
                });
            }
        }

        public void addRootsViaSubscriber(Set<RootNode> rootNodes, ReactiveNode subscriber) {
            Set<RootNode> newRootsFromSubscriber = new HashSet<>();
            for (RootNode root : rootNodes) {
                Set<ReactiveNode> subscribers = downstreamRoots.get(root);
                if (subscribers == null) {
                    downstreamRoots.computeIfAbsent(root, ignored -> new HashSet<>()).add(subscriber);
                    newRootsFromSubscriber.add(root);
                    synchroniseRoot(root);
                } else if (subscribers.add(subscriber)) {
                    synchroniseRootExtraSubscriber(root);
                }
            }
            propagateRootsUpstream(newRootsFromSubscriber);
        }

        protected void propagateRootsUpstream(Set<RootNode> toPropagate) {
            if (!toPropagate.isEmpty()) {
                publishers().forEach(publisher -> publisher.addRootsViaSubscriber(toPropagate, this));
            }
        }

        protected void synchroniseRootExtraSubscriber(RootNode rootNode) {
            logAnswerDelta(totalAnswersCreated(rootNode), "synchroniseRootExtraSubscriber", rootNode);
            rootNode.updateAnswerCount(answersCreated);
            logFrontierDelta(-1, "synchroniseRootExtraSubscriber", rootNode);
            rootNode.updateFrontiersCount(-1);
        }

        protected void synchroniseRoot(RootNode rootNode) {
            logAnswerDelta(totalAnswersCreated(rootNode) - totalAnswersConsumed(), "synchroniseRootCounts", rootNode);
            rootNode.updateAnswerCount(totalAnswersCreated(rootNode) - totalAnswersConsumed());
            logFrontierDelta(totalFrontierForks() - totalFrontierJoins(rootNode), "synchroniseRootCounts", rootNode);
            rootNode.updateFrontiersCount(totalFrontierForks() - totalFrontierJoins(rootNode));
        }

        public Set<RootNode> activeDownstreamRoots() {
            return iterate(downstreamRoots.keySet()).filter(g -> !g.finished).toSet();
        }

        protected Set<RootNode> activeUpstreamRoots() {
            return activeDownstreamRoots();
        }

        public RootNode asRoot() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        public SourceNode asSource() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

    }

    private static class SourceNode extends ReactiveNode {

        protected boolean finished;

        SourceNode(Reactive.Identifier<?, ?> source) {
            super(source);
            this.finished = false;
        }

        public boolean isFinished() {
            return finished;
        }

        protected void setFinished() {
            finished = true;
            iterate(activeDownstreamRoots()).forEachRemaining(g -> g.sourceFinished(this));
        }

        @Override
        public SourceNode asSource() {
            return this;
        }

        @Override
        protected void synchroniseRoot(RootNode rootNode) {
            rootNode.addSource(this);
            if (isFinished()) rootNode.sourceFinished(this);
            logAnswerDelta(totalAnswersCreated(rootNode), "synchroniseRootCounts in Source", rootNode);
            rootNode.updateAnswerCount(totalAnswersCreated(rootNode));
            logAnswerDelta(-totalFrontierJoins(rootNode), "synchroniseRootCounts", rootNode);
            rootNode.updateFrontiersCount(-totalFrontierJoins(rootNode));
        }

    }

    private static class RootNode extends SourceNode {

        private final Driver<? extends AbstractReactiveBlock<?, ?, ?, ?>> rootReactiveBlock;
        private final Driver<Monitor> monitor;
        private final Set<SourceNode> activeSources;
        private long activeFrontiers;
        private long activeAnswers;

        RootNode(Reactive.Identifier<?, ?> root, Driver<? extends AbstractReactiveBlock<?, ?, ?, ?>> rootReactiveBlock,
                 Driver<Monitor> monitor) {
            super(root);
            this.rootReactiveBlock = rootReactiveBlock;
            this.monitor = monitor;
            this.activeSources = new HashSet<>();
            this.activeFrontiers = 1;
            this.activeAnswers = 0;
        }

        @Override
        public RootNode asRoot() {
            return this;
        }

        @Override
        protected Set<RootNode> activeUpstreamRoots() {
            if (isFinished()) return set();
            else return set(this);
        }

        @Override
        protected void synchroniseRoot(RootNode rootNode) {
            assert !rootNode.equals(this);
            super.synchroniseRoot(rootNode);
        }

        protected void propagateRootsUpstream(Set<RootNode> toPropagate) {
            if (!toPropagate.isEmpty()) {
                Set<RootNode> toPropagateOnwards = new HashSet<>(toPropagate);
                toPropagateOnwards.retainAll(activeUpstreamRoots());  // TODO: Limits to active upstream, not implied by the method name
                publishers().forEach(publisher -> publisher.addRootsViaSubscriber(toPropagateOnwards, this));
            }
        }

        private void finishRootNode() {
            Tracer.getIfEnabled().ifPresent(tracer -> tracer.finishRootNode(reactive, monitor));
            rootReactiveBlock.execute(actor -> actor.onFinished(reactive));
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
