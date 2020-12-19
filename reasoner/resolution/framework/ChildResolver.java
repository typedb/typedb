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

package grakn.core.reasoner.resolution.framework;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.ResolverRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class ChildResolver<T extends ChildResolver<T>> extends Resolver<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ChildResolver.class);

    private final Map<Request, ResponseProducer> responseProducers;
    private boolean isInitialised;

    public ChildResolver(Actor<T> self, String name) {
        super(self, name);
        isInitialised = false;
        responseProducers = new HashMap<>();
    }

    protected abstract Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer);

    /*
     *
     * Handlers for messages sent into the execution actor that are dispatched via the actor model.
     *
     */
    @Override
    public void executeReceiveRequest(Request fromUpstream, ResolverRegistry registry, int iteration) {
        LOG.trace("{}: Receiving a new Request: {}", name, fromUpstream);
        if (!isInitialised) {
            LOG.debug(name + ": initialising downstream actors");
            initialiseDownstreamActors(registry);
            isInitialised = true;
        }

        if (!responseProducers.containsKey(fromUpstream)) {
            LOG.debug("{}: Creating a new ResponseProducer for the given Request: {}", name, fromUpstream);
            responseProducers.put(fromUpstream, responseProducerCreate(fromUpstream, iteration));
        } else {
            ResponseProducer responseProducer = responseProducers.get(fromUpstream);

            assert responseProducer.iteration() == iteration ||
                    responseProducer.iteration() + 1 == iteration;

            if (responseProducer.iteration() + 1 == iteration) {
                // when the same request for the next iteration the first time, re-initialise required state
                ResponseProducer responseProducerNextIter = responseProducerReiterate(fromUpstream, responseProducer, iteration);
                responseProducers.put(fromUpstream, responseProducerNextIter);
            }
        }

        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        // short circuit if the request came from a prior iteration
        if (iteration < responseProducer.iteration()) respondToUpstream(new Response.Exhausted(fromUpstream), registry, iteration);

        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducers.get(fromUpstream));
        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry, iteration);
        else respondToUpstream(action.second(), registry, iteration);
    }

    @Override
    protected void executeReceiveAnswer(Response.Answer fromDownstream, ResolverRegistry registry, int iteration) {
        LOG.trace("{}: Receiving a new Answer from downstream: {}", name, fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry, iteration);
        else respondToUpstream(action.second(), registry, iteration);
    }

    @Override
    protected void executeReceiveExhausted(Response.Exhausted fromDownstream, ResolverRegistry registry, int iteration) {
        LOG.trace("{}: Receiving a new Exhausted from downstream: {}", name, fromDownstream);
        Request toDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = fromUpstream(toDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        Either<Request, Response> action = receiveExhausted(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry, iteration);
        else respondToUpstream(action.second(), registry, iteration);

    }

    private void respondToUpstream(Response response, ResolverRegistry registry, int iteration) {
        Actor<? extends Resolver<?>> receiver = response.sourceRequest().sender();
        if (response.isAnswer()) {
            LOG.trace("{} : Sending a new Response.Answer to upstream", name);
            receiver.tell(actor -> actor.executeReceiveAnswer(response.asAnswer(), registry, iteration));
        } else if (response.isExhausted()) {
            LOG.trace("{}: Sending a new Response.Exhausted to upstream", name);
            receiver.tell(actor -> actor.executeReceiveExhausted(response.asExhausted(), registry, iteration));
        } else {
            throw new RuntimeException(("Unknown response type " + response.getClass().getSimpleName()));
        }
    }

}
