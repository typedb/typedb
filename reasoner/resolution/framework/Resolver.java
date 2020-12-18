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

public abstract class Resolver<T extends Resolver<T>> extends Actor.State<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    protected final String name;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;
    private boolean isInitialised;

    public Resolver(Actor<T> self, String name) {
        super(self);
        this.name = name;
        isInitialised = false;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public String name() {
        return name;
    }

    protected abstract void initialiseDownstreamActors(ResolverRegistry registry);

    protected abstract ResponseProducer responseProducerCreate(Request fromUpstream);

    protected abstract ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer);

    /*
     *
     * Handlers for messages sent into the execution actor that are dispatched via the actor model.
     *
     */
    public void executeReceiveRequest(Request fromUpstream, ResolverRegistry registry) {
        LOG.trace("{}: Receiving a new Request: {}", name, fromUpstream);
        if (!isInitialised) {
            LOG.debug(name + ": initialising downstream actors");
            initialiseDownstreamActors(registry);
            isInitialised = true;
        }

        if (!responseProducers.containsKey(fromUpstream)) {
            LOG.debug("{}: Creating a new ResponseProducer for the given Request: {}", name, fromUpstream);
            responseProducers.put(fromUpstream, responseProducerCreate(fromUpstream));
        } else {
            ResponseProducer responseProducer = responseProducers.get(fromUpstream);

            assert responseProducer.iteration() == fromUpstream.iteration() ||
                    responseProducer.iteration() + 1 == fromUpstream.iteration();

            if (responseProducer.iteration() + 1 == fromUpstream.iteration()) {
                ResponseProducer responseProducerNextIter = responseProducerReiterate(fromUpstream, responseProducer);
                responseProducers.put(fromUpstream, responseProducerNextIter);
            }
        }

        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducers.get(fromUpstream));
        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveAnswer(Response.Answer fromDownstream, ResolverRegistry registry) {
        LOG.trace("{}: Receiving a new Answer from downstream: {}", name, fromDownstream);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveExhausted(Response.Exhausted fromDownstream, ResolverRegistry registry) {
        LOG.trace("{}: Receiving a new Exhausted from downstream: {}", name, fromDownstream);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        Either<Request, Response> action = receiveExhausted(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);

    }

    /*
     *
     * Helper method private to this class.
     *
     * */
    private void requestFromDownstream(Request request, Request fromUpstream, ResolverRegistry registry) {
        LOG.trace("{} : Sending a new answer Request to downstream: {}", name, request);
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends Resolver<?>> receiver = request.receiver();
        receiver.tell(actor -> actor.executeReceiveRequest(request, registry));
    }

    private void respondToUpstream(Response response, ResolverRegistry registry) {
        if (response.isRootResponse()) {
            return;
        }

        Actor<? extends Resolver<?>> receiver = response.sourceRequest().sender();
        if (response.isAnswer()) {
            LOG.trace("{} : Sending a new Response.Answer to upstream", name);
            receiver.tell(actor -> actor.executeReceiveAnswer(response.asAnswer(), registry));
        } else if (response.isExhausted()) {
            LOG.trace("{}: Sending a new Response.Exhausted to upstream", name);
            receiver.tell(actor -> actor.executeReceiveExhausted(response.asExhausted(), registry));
        } else {
            throw new RuntimeException(("Unknown response type " + response.getClass().getSimpleName()));
        }
    }

}
