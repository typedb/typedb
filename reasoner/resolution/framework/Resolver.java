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

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.concurrent.producer.Producers;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.TraceId;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;

public abstract class Resolver<RESOLVER extends ReasonerActor<RESOLVER>> extends ReasonerActor<RESOLVER> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    private final Map<Pair<Request.Visit, TraceId>, Request.Visit> requestRouter;
    protected final ResolverRegistry registry;

    protected Resolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry) {
        super(driver, name);
        this.registry = registry;
        this.requestRouter = new HashMap<>();
        // Note: initialising downstream actors in constructor will create all actors ahead of time, so it is non-lazy
        // additionally, it can cause deadlock within ResolverRegistry as different threads initialise actors
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Resolver interrupted by resource close: {}", e.getMessage());
                registry.terminate(e);
                return;
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
    }

    public abstract void receiveVisit(Request.Visit fromUpstream, int iteration);

    protected abstract void receiveRevisit(Request.Revisit fromUpstream, int iteration);

    protected abstract void receiveAnswer(Response.Answer fromDownstream, int iteration);

    protected abstract void receiveFail(Response.Fail fromDownstream, int iteration);

    protected void receiveCycle(Response.Cycle fromDownstream, int iteration) {
        LOG.trace("{}: received Cycle: {}", name(), fromDownstream);
        if (isTerminated()) return;
        Request.Visit toDownstream = fromDownstream.sourceRequest();
        Request.Visit fromUpstream = fromUpstream(toDownstream);
        cycleToUpstream(fromUpstream, fromDownstream.origins(), iteration);
    }

    protected abstract void initialiseDownstreamResolvers(); //TODO: This method should only be required of the coordinating actors

    protected Request.Visit fromUpstream(Request.Visit toDownstream) {
        assert toDownstream.traceId().rootId() != -1;
        Pair<Request.Visit, TraceId> ds = new Pair<>(toDownstream, toDownstream.traceId());
        assert requestRouter.containsKey(ds);
        assert requestRouter.get(ds).traceId() == toDownstream.traceId();
        return requestRouter.get(ds);
    }

    // TODO: Rename to sendRequest or request
    protected void requestFromDownstream(Downstream downstream, Request.Visit fromUpstream, int iteration) {
        requestFromDownstream(downstream.toVisit(fromUpstream.traceId()), fromUpstream, iteration);
    }

    protected void requestFromDownstream(Request.Visit request, Request.Visit fromUpstream, int iteration) {
        LOG.trace("{} : Sending a new answer Visit to downstream: {}", name(), request);
        assert fromUpstream.traceId().rootId() != -1;
        assert request.traceId() == fromUpstream.traceId();
        if (registry.resolutionTracing()) ResolutionTracer.get().request(request, iteration);
        // TODO: we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(new Pair<>(request, request.traceId()), fromUpstream);
        request.receiver().execute(actor -> actor.receiveVisit(request, iteration));
    }

    protected void requestFromDownstream(Downstream downstream, Request.Revisit fromUpstream, int iteration) {
        requestFromDownstream(downstream.toRevisit(fromUpstream.visit().traceId(), fromUpstream.cycle()), fromUpstream, iteration);
    }

    protected void requestFromDownstream(Request.Revisit toRevisit, Request.Revisit fromUpstream, int iteration) {
        Request.Visit downstream = toRevisit.visit();
        LOG.trace("{} : Sending a new answer Visit to downstream: {}", name(), downstream);
        assert fromUpstream.visit().traceId().rootId() != -1;
        assert downstream.traceId() == fromUpstream.visit().traceId();
        if (registry.resolutionTracing()) ResolutionTracer.get().request(downstream, iteration);
        assert requestRouter.containsKey(new Pair<>(downstream, downstream.traceId()));
        assert requestRouter.get(new Pair<>(downstream, downstream.traceId())).equals(fromUpstream.visit());
        downstream.receiver().execute(actor -> actor.receiveVisit(downstream, iteration));
    }

    // TODO: Rename to sendResponse or respond
    protected void answerToUpstream(AnswerState answer, Request.Visit fromUpstream, int iteration) {
        assert answer.isPartial();
        Answer response = Answer.create(fromUpstream, answer.asPartial());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseAnswer(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveAnswer(response, iteration));
    }

    protected void failToUpstream(Request.Visit fromUpstream, int iteration) {
        Response.Fail response = new Response.Fail(fromUpstream);
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseExhausted(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveFail(response, iteration));
    }

    protected void cycleToUpstream(Request.Visit fromUpstream, Set<Response.Cycle.Origin> cycleOrigins, int iteration) {
        assert !fromUpstream.partialAnswer().parent().isTop();
        Response.Cycle response = new Response.Cycle(fromUpstream, cycleOrigins);
        LOG.trace("{} : Sending a new Response.Cycle to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseCycle(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveCycle(response, iteration));
    }

    protected void cycleToUpstream(Request.Visit fromUpstream, int numAnswersSeen, int iteration) {
        assert !fromUpstream.partialAnswer().parent().isTop();
        Response.Cycle response = new Response.Cycle.Origin(fromUpstream, numAnswersSeen);
        LOG.trace("{} : Sending a new Response.Cycle to upstream", name());
        if (registry.resolutionTracing()) ResolutionTracer.get().responseCycle(response, iteration);
        fromUpstream.sender().execute(actor -> actor.receiveCycle(response, iteration));
    }

    protected FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, ConceptMap bounds) {
        return compatibleBounds(conjunction, bounds).map(c -> {
            GraphTraversal.Thing traversal = boundTraversal(conjunction.traversal(), c);
            return registry.traversalEngine().iterator(traversal).map(v -> registry.conceptManager().conceptMap(v));
        }).orElse(Iterators.empty());
    }

    protected Producer<ConceptMap> traversalProducer(Conjunction conjunction, ConceptMap bounds, int parallelisation) {
        return compatibleBounds(conjunction, bounds).map(b -> {
            GraphTraversal.Thing traversal = boundTraversal(conjunction.traversal(), b);
            return registry.traversalEngine().producer(traversal, Either.first(INCREMENTAL), parallelisation)
                    .map(vertexMap -> registry.conceptManager().conceptMap(vertexMap));
        }).orElse(Producers.empty());
    }

    private Optional<ConceptMap> compatibleBounds(Conjunction conjunction, ConceptMap bounds) {
        Map<Retrievable, Concept> newBounds = new HashMap<>();
        for (Map.Entry<Retrievable, ? extends Concept> entry : bounds.concepts().entrySet()) {
            Retrievable id = entry.getKey();
            Concept bound = entry.getValue();
            Variable conjVariable = conjunction.variable(id);
            assert conjVariable != null;
            if (conjVariable.isThing()) {
                if (!conjVariable.asThing().iid().isPresent()) newBounds.put(id, bound);
                else if (!conjVariable.asThing().iid().get().iid().equals(bound.asThing().getIID())) {
                    return Optional.empty();
                }
            } else if (conjVariable.isType()) {
                if (!conjVariable.asType().label().isPresent()) newBounds.put(id, bound);
                else if (!conjVariable.asType().label().get().properLabel().equals(bound.asType().getLabel())) {
                    return Optional.empty();
                }
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
        return Optional.of(new ConceptMap(newBounds));
    }

    protected static GraphTraversal.Thing boundTraversal(GraphTraversal.Thing traversal, ConceptMap bounds) {
        bounds.concepts().forEach((id, concept) -> {
            if (concept.isThing()) traversal.iid(id.asVariable(), concept.asThing().getIID());
            else {
                traversal.clearLabels(id.asVariable());
                traversal.labels(id.asVariable(), concept.asType().getLabel());
            }
        });
        return traversal;
    }

    public static class DownstreamManager {
        protected final List<Downstream> downstreams;

        public DownstreamManager() {
            this.downstreams = new ArrayList<>();
        }

        public DownstreamManager(List<Downstream> downstreams) {
            this.downstreams = downstreams;
        }

        public boolean hasNext() {
            return !downstreams.isEmpty();
        }

        public Downstream next() {
            return downstreams.get(0);
        }

        public void add(Downstream downstream) {
            assert !(downstreams.contains(downstream)) : "downstream answer producer already contains this request";
            downstreams.add(downstream);
        }

        public void remove(Downstream downstream) {
            downstreams.remove(downstream);
        }

        public void clear() {
            downstreams.clear();
        }

        public static class Blockable extends DownstreamManager {

            protected Map<Downstream, Set<Response.Cycle.Origin>> blocked;

            public Blockable() {
                this.blocked = new LinkedHashMap<>();
            }

            public Blockable(List<Downstream> downstreams) {
                super(downstreams);
                this.blocked = new LinkedHashMap<>();
            }

            public boolean contains(Downstream downstream) {
                return downstreams.contains(downstream) || blocked.containsKey(downstream);
            }

            public boolean hasNextUnblocked() {
                return super.hasNext();
            }

            public Downstream nextUnblocked() {
                return super.next();
            }

            @Override
            public boolean hasNext() {
                return !downstreams.isEmpty() || !blocked.isEmpty();
            }

            @Override
            public Downstream next() {
                if (super.hasNext()) return super.next();
                else {
                    Optional<Downstream> b = iterate(blocked.keySet()).first();
                    assert b.isPresent();
                    return b.get();
                }
            }

            @Override
            public void remove(Downstream downstream) {
                downstreams.remove(downstream);
                blocked.remove(downstream);
            }

            public Set<Response.Cycle.Origin> blockers() {
                return iterate(blocked.values()).flatMap(Iterators::iterate).toSet();
            }

            public void block(Downstream blocked, Response.Cycle.Origin blocker) { // TODO Should take a set
                assert downstreams.contains(blocked) || this.blocked.containsKey(blocked);
                this.blocked.computeIfAbsent(blocked, b -> new HashSet<>()).add(blocker);
                downstreams.remove(blocked);
            }

            public void unblock(Response.Cycle.Origin cycleOrigin) {
                blocked.forEach((downstream, blockingCycles) -> {
                    // TODO: Actually add this to a set of Downstreams that will create Revisit requests
                    blockingCycles.remove(cycleOrigin);
                    if (blockingCycles.isEmpty()) {
                        blocked.remove(downstream);
                        downstreams.add(downstream);
                    }
                });
            }
        }
    }
}
