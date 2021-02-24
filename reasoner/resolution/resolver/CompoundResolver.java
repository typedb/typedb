/*
 * Copyright (C) 2021 Grakn Labs
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
 */

package grakn.core.reasoner.resolution.resolver;

import grakn.core.concept.ConceptManager;
import grakn.core.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

public abstract class CompoundResolver<T extends CompoundResolver<T>> extends Resolver<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CompoundResolver.class);

    private final Actor<ResolutionRecorder> resolutionRecorder;
    final Map<Request, Responses> responses;
    boolean isInitialised;

    protected CompoundResolver(Actor<T> self, String name, ResolverRegistry registry, TraversalEngine traversalEngine,
                               ConceptManager conceptMgr, boolean resolutionTracing, Actor<ResolutionRecorder> resolutionRecorder) {
        super(self, name, registry, traversalEngine, conceptMgr, resolutionTracing);
        this.resolutionRecorder = resolutionRecorder;
        this.responses = new HashMap<>();
        this.isInitialised = false;
    }

    protected abstract void nextAnswer(Request fromUpstream, Responses responseProducer, int iteration);

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        LOG.trace("{}: received Request: {}", name(), fromUpstream);
        if (!isInitialised) initialiseDownstreamResolvers();
        if (isTerminated()) return;

        Responses responseProducer = mayUpdateAndGetResponseProducer(fromUpstream, iteration);
        if (iteration < responseProducer.iteration()) {
            // short circuit if the request came from a prior iteration
            failToUpstream(fromUpstream, iteration);
        } else {
            assert iteration == responseProducer.iteration();
            nextAnswer(fromUpstream, responseProducer, iteration);
        }
    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {
        LOG.trace("{}: received Exhausted, with iter {}: {}", name(), iteration, fromDownstream);
        if (isTerminated()) return;

        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        Responses responseProducer = responses.get(fromUpstream);

        if (iteration < responseProducer.iteration()) {
            // short circuit old iteration failed messages back out of the actor model
            failToUpstream(fromUpstream, iteration);
            return;
        }
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        nextAnswer(fromUpstream, responseProducer, iteration);
    }

    private Responses mayUpdateAndGetResponseProducer(Request fromUpstream, int iteration) {
        if (!responses.containsKey(fromUpstream)) {
            responses.put(fromUpstream, responsesCreate(fromUpstream, iteration));
        } else {
            Responses responses = this.responses.get(fromUpstream);
            assert responses.iteration() == iteration ||
                    responses.iteration() + 1 == iteration;

            if (responses.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                Responses responseProducerNextIter = responsesReiterate(fromUpstream, responses, iteration);
                this.responses.put(fromUpstream, responseProducerNextIter);
            }
        }
        return responses.get(fromUpstream);
    }

    abstract Responses responsesCreate(Request fromUpstream, int iteration);

    abstract Responses responsesReiterate(Request fromUpstrea, Responses priorResponses, int iteration);

    static class Responses {

        private final int iteration;
        private final LinkedHashSet<Request> downstreamProducer;
        private Iterator<Request> downstreamProducerSelector;

        public Responses(int iteration) {
            this.iteration = iteration;
            downstreamProducer = new LinkedHashSet<>();
            downstreamProducerSelector = downstreamProducer.iterator();
        }

        public boolean hasDownstreamProducer() {
            return !downstreamProducer.isEmpty();
        }

        public Request nextDownstreamProducer() {
            if (!downstreamProducerSelector.hasNext()) downstreamProducerSelector = downstreamProducer.iterator();
            return downstreamProducerSelector.next();
        }

        public void addDownstreamProducer(Request request) {
            assert !(downstreamProducer.contains(request)) : "downstream answer producer already contains this request";

            downstreamProducer.add(request);
            downstreamProducerSelector = downstreamProducer.iterator();
        }

        public void removeDownstreamProducer(Request request) {
            boolean removed = downstreamProducer.remove(request);
            // only update the iterator when removing an element, to avoid resetting and reusing first request too often
            // note: this is a large performance win when processing large batches of requests
            if (removed) downstreamProducerSelector = downstreamProducer.iterator();
        }

        public int iteration() {
            return iteration;
        }
    }
}
