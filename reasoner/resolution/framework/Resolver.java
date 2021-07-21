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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.concurrent.producer.Producers;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Answer;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;

public abstract class Resolver<RESOLVER extends Resolver<RESOLVER>> extends Actor<RESOLVER> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    private final Map<Request, Request> requestRouter;
    protected final ResolverRegistry registry;
    protected final TraversalEngine traversalEngine;
    protected final ConceptManager conceptMgr;
    private final boolean resolutionTracing;
    private boolean terminated;

    protected Resolver(Driver<RESOLVER> driver, String name, ResolverRegistry registry, TraversalEngine traversalEngine,
                       ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, name);
        this.registry = registry;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.resolutionTracing = resolutionTracing;
        this.terminated = false;
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
                registry.terminateResolvers(e);
                return;
            }
        }
        LOG.error("Actor exception", e);
        registry.terminateResolvers(e);
    }

    public abstract void receiveRequest(Request fromUpstream, int iteration);

    protected abstract void receiveAnswer(Response.Answer fromDownstream, int iteration);

    protected abstract void receiveFail(Response.Fail fromDownstream, int iteration);

    public void terminate(Throwable cause) {
        LOG.debug("Resolver terminated. ", cause);
        this.terminated = true;
    }

    public boolean isTerminated() { return terminated; }

    protected abstract void initialiseDownstreamResolvers();

    protected Request fromUpstream(Request toDownstream) {
        assert requestRouter.containsKey(toDownstream);
        return requestRouter.get(toDownstream);
    }

    protected void requestFromDownstream(Request request, Request fromUpstream, int iteration) {
        LOG.trace("{} : Sending a new answer Request to downstream: {}", name(), request);
        if (resolutionTracing) ResolutionTracer.get().request(this.name(), request.receiver().name(), iteration,
                                                              request.partialAnswer().conceptMap().concepts().keySet().toString());
        // TODO: we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Driver<? extends Resolver<?>> receiver = request.receiver();
        receiver.execute(actor -> actor.receiveRequest(request, iteration));
    }

    protected void answerToUpstream(AnswerState answer, Request fromUpstream, int iteration) {
        assert answer.isPartial();
        Answer response = Answer.create(fromUpstream, answer.asPartial());
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (resolutionTracing) ResolutionTracer.get().responseAnswer(
                this.name(), fromUpstream.sender().name(), iteration,
                response.asAnswer().answer().conceptMap().concepts().keySet().toString()
        );
        fromUpstream.sender().execute(actor -> actor.receiveAnswer(response, iteration));
    }

    protected void failToUpstream(Request fromUpstream, int iteration) {
        Response.Fail response = new Response.Fail(fromUpstream);
        LOG.trace("{} : Sending a new Response.Answer to upstream", name());
        if (resolutionTracing) ResolutionTracer.get().responseExhausted(
                this.name(), fromUpstream.sender().name(), iteration
        );
        fromUpstream.sender().execute(actor -> actor.receiveFail(response, iteration));
    }

    protected FunctionalIterator<ConceptMap> traversalIterator(Conjunction conjunction, ConceptMap bounds) {
        return compatibleBounds(conjunction, bounds).map(c -> {
            GraphTraversal traversal = boundTraversal(conjunction.traversal(), c);
            return traversalEngine.iterator(traversal).map(conceptMgr::conceptMap);
        }).orElse(Iterators.empty());
    }

    protected Producer<ConceptMap> traversalProducer(Conjunction conjunction, ConceptMap bounds, int parallelisation) {
        return compatibleBounds(conjunction, bounds).map(b -> {
            GraphTraversal traversal = boundTraversal(conjunction.traversal(), b);
            return traversalEngine.producer(traversal, Either.first(INCREMENTAL), parallelisation).map(conceptMgr::conceptMap);
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

    protected GraphTraversal boundTraversal(GraphTraversal traversal, ConceptMap bounds) {
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
        private final LinkedHashSet<Request> downstreams;
        private Iterator<Request> downstreamSelector;

        public DownstreamManager() {
            this.downstreams = new LinkedHashSet<>();
            this.downstreamSelector = downstreams.iterator();
        }

        public boolean hasDownstream() {
            return !downstreams.isEmpty();
        }

        public Request nextDownstream() {
            if (!downstreamSelector.hasNext()) downstreamSelector = downstreams.iterator();
            return downstreamSelector.next();
        }

        public void addDownstream(Request request) {
            assert !(downstreams.contains(request)) : "downstream answer producer already contains this request";

            downstreams.add(request);
            downstreamSelector = downstreams.iterator();
        }

        public void removeDownstream(Request request) {
            boolean removed = downstreams.remove(request);
            // only update the iterator when removing an element, to avoid resetting and reusing first request too often
            // note: this is a large performance win when processing large batches of requests
            if (removed) downstreamSelector = downstreams.iterator();
        }

        public void clearDownstreams() {
            downstreams.clear();
            downstreamSelector = Iterators.empty();
        }

        public boolean contains(Request downstreamRequest) {
            return downstreams.contains(downstreamRequest);
        }
    }
}
