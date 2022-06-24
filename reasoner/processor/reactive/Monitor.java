/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.common.Tracer;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Monitor extends Actor<Monitor> {

    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);
    private boolean terminated;
    private final Map<Reactive.Identifier, ReactiveNode> reactiveNodes;
    private final Tracer tracer;

    public Monitor(Driver<Monitor> driver, @Nullable Tracer tracer) {
        super(driver, Monitor.class::getSimpleName);
        this.tracer = tracer;
        this.reactiveNodes = new HashMap<>();
    }

    private Optional<Tracer> tracer() {
        return Optional.ofNullable(tracer);
    }

    private ReactiveNode getNode(Reactive.Identifier reactive) {
        return reactiveNodes.get(reactive);
    }

    private ReactiveNode getOrCreateNode(Reactive.Identifier reactive) {
        return reactiveNodes.computeIfAbsent(reactive, p -> new ReactiveNode(reactive));
    }

    private void putNode(Reactive.Identifier reactive, ReactiveNode reactiveNode) {
        ReactiveNode exists = reactiveNodes.put(reactive, reactiveNode);
        assert exists == null;
    }

    public <R> void registerRoot(Driver<? extends AbstractProcessor<R, ?, ?, ?>> processor, Reactive.Identifier root) {
        tracer().ifPresent(tracer -> tracer.registerRoot(root, driver()));
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the root, or a duplicate node will be created.
        putNode(root, new RootNode(root, processor, driver()));
    }

    public void rootFinished(Reactive.Identifier root) {
        tracer().ifPresent(tracer -> tracer.rootFinalised(root, driver()));
        if (terminated) return;
        getNode(root).asRoot().setFinished();
    }

    void registerSource(Reactive.Identifier source) {
        tracer().ifPresent(tracer -> tracer.registerSource(source, driver()));
        if (terminated) return;
        // Note this MUST be called before any paths are registered to or from the source, or a duplicate node will be created.
        assert reactiveNodes.get(source) == null;
        putNode(source, new SourceNode(source));
    }

    public void sourceFinished(Reactive.Identifier source) {
        tracer().ifPresent(tracer -> tracer.sourceFinished(source, driver()));
        if (terminated) return;
        getNode(source).asSource().setFinished();
    }

    public void registerPath(Reactive.Identifier subscriber, Reactive.Identifier publisher) {
        tracer().ifPresent(tracer -> tracer.registerPath(subscriber, publisher, driver()));
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

    public void createAnswer(Reactive.Identifier publisher) {
        tracer().ifPresent(tracer -> tracer.createAnswer(publisher, driver()));
        if (terminated) return;
        getOrCreateNode(publisher).createAnswer();
    }

    public void consumeAnswer(Reactive.Identifier subscriber) {
        tracer().ifPresent(tracer -> tracer.consumeAnswer(subscriber, driver()));
        if (terminated) return;
        ReactiveNode subscriberNode = getOrCreateNode(subscriber);
        subscriberNode.consumeAnswer();
    }

    private static class ReactiveNode {

        private final Set<ReactiveNode> publishers;
        private final Map<RootNode, Set<ReactiveNode>> downstreamRoots;
        final Reactive.Identifier reactive;
        private long answersCreated;
        private long answersConsumed;

        ReactiveNode(Reactive.Identifier reactive) {
            this.reactive = reactive;
            this.publishers = new HashSet<>();
            this.downstreamRoots = new HashMap<>();
            this.answersCreated = 0;
            this.answersConsumed = 0;
        }

        long totalAnswersCreated(RootNode rootNode) {
            return answersCreated * downstreamRoots.get(rootNode).size();
        }

        private long totalAnswersConsumed() {
            return answersConsumed;
        }

        long totalFrontierJoins(RootNode rootNode) {
            return downstreamRoots.get(rootNode).size();
        }

        private long totalFrontierForks() {
            if (publishers.size() == 0) return 1;
            else return publishers.size();
        }

        private void createAnswer() {
            answersCreated += 1;
            downstreamRoots.forEach((root, subs) -> root.updateAnswerCount(subs.size()));
        }

        private void consumeAnswer() {
            answersConsumed += 1;
            iterate(activeUpstreamRoots()).forEachRemaining(root -> root.updateAnswerCount(-1));
        }

        Set<ReactiveNode> publishers() {
            return publishers;
        }

        private void fork(ReactiveNode publisherNode) {
            boolean isNew = publishers.add(publisherNode);
            assert isNew;
            if (publishers.size() > 1) {
                iterate(activeUpstreamRoots()).forEachRemaining(root -> root.updateFrontiersCount(1));
            }
        }

        void addRootsViaSubscriber(Set<RootNode> rootNodes, ReactiveNode subscriber) {
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

        void propagateRootsUpstream(Set<RootNode> toPropagate) {
            if (!toPropagate.isEmpty()) {
                publishers().forEach(publisher -> publisher.addRootsViaSubscriber(toPropagate, this));
            }
        }

        private void synchroniseRootExtraSubscriber(RootNode rootNode) {
            rootNode.updateAnswerCount(answersCreated);
            rootNode.updateFrontiersCount(-1);
        }

        void synchroniseRoot(RootNode rootNode) {
            rootNode.updateAnswerCount(totalAnswersCreated(rootNode) - totalAnswersConsumed());
            rootNode.updateFrontiersCount(totalFrontierForks() - totalFrontierJoins(rootNode));
        }

        Set<RootNode> activeDownstreamRoots() {
            return iterate(downstreamRoots.keySet()).filter(g -> !g.finished).toSet();
        }

        Set<RootNode> activeUpstreamRoots() {
            return activeDownstreamRoots();
        }

        boolean isRoot() {
            return false;
        }

        RootNode asRoot() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        SourceNode asSource() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

    }

    private static class SourceNode extends ReactiveNode {

        boolean finished;

        SourceNode(Reactive.Identifier source) {
            super(source);
            this.finished = false;
        }

        boolean isFinished() {
            return finished;
        }

        void setFinished() {
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
            rootNode.updateAnswerCount(totalAnswersCreated(rootNode));
            rootNode.updateFrontiersCount(-totalFrontierJoins(rootNode));
        }

    }

    private class RootNode extends SourceNode {

        private final Driver<? extends AbstractProcessor<?, ?, ?, ?>> rootProcessor;
        private final Driver<Monitor> monitor;
        private final Set<SourceNode> activeSources;
        private long activeFrontiers;
        private long activeAnswers;
        private boolean finishing;

        private RootNode(Reactive.Identifier root, Driver<? extends AbstractProcessor<?, ?, ?, ?>> rootProcessor,
                         Driver<Monitor> monitor) {
            super(root);
            this.rootProcessor = rootProcessor;
            this.monitor = monitor;
            this.activeSources = new HashSet<>();
            this.activeFrontiers = 1;
            this.activeAnswers = 0;
            this.finishing = false;
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
                publishers().forEach(publisher -> publisher.addRootsViaSubscriber(activeUpstreamRoots(), this));
            }
        }

        private void finishRootNode() {
            tracer().ifPresent(tracer -> tracer.finishRootNode(reactive, monitor));
            rootProcessor.execute(actor -> actor.onFinished(reactive));
        }

        private void checkFinished() {
            assert !finishing;
            if (!finished && activeSources.isEmpty()){
                assert activeFrontiers >= 0;
                if (activeFrontiers == 0) {
                    assert activeAnswers >= 0;
                    if (activeAnswers == 0) {
                        finishing = true;
                        finishRootNode();
                    }
                }
            }
        }

        void updateAnswerCount(long delta) {
            activeAnswers += delta;
            if (delta < 0) checkFinished();
        }

        void updateFrontiersCount(long delta) {
            activeFrontiers += delta;
            if (delta < 0) checkFinished();
        }

        void addSource(SourceNode sourceNode) {
            activeSources.add(sourceNode);
        }

        void sourceFinished(SourceNode sourceNode) {
            boolean contained = activeSources.remove(sourceNode);
            assert contained;
            checkFinished();
        }

        @Override
        public boolean isRoot() {
            return true;
        }

        @Override
        public RootNode asRoot() {
            return this;
        }

    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Monitor interrupted by resource close: {}", e.getMessage());
                propagateThrowableToRootProcessors(e);
                return;
            } else {
                LOG.debug("Monitor interrupted by TypeDB exception: {}", e.getMessage());
            }
        }
        LOG.error("Actor exception", e);
        propagateThrowableToRootProcessors(e);
    }

    private void propagateThrowableToRootProcessors(Throwable e) {
        iterate(reactiveNodes.values())
                .filter(ReactiveNode::isRoot)
                .forEachRemaining(node -> node.asRoot().rootProcessor.execute(actor -> actor.exception(e)));
    }

    public void terminate(Throwable cause) {
        LOG.debug("Monitor terminated.", cause);
        this.terminated = true;
    }

}
