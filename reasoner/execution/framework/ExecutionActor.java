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

package grakn.core.reasoner.execution.framework;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.core.reasoner.execution.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class ExecutionActor<T extends ExecutionActor<T>> extends Actor.State<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionActor.class);

    protected String name;
    private boolean isInitialised;
    @Nullable
    private final LinkedBlockingQueue<Response> responses;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;

    public ExecutionActor(Actor<T> self, String name) {
        this(self, name, null);
    }

    public ExecutionActor(Actor<T> self, String name, @Nullable LinkedBlockingQueue<Response> responses) {
        super(self);
        this.name = name;
        isInitialised = false;
        this.responses = responses;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public String name() { return name; }

    protected abstract ResponseProducer createResponseProducer(Request fromUpstream);

    protected abstract void initialiseDownstreamActors(Registry registry);

    protected abstract Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer);

    /*
     *
     * Handlers for messages sent into the execution actor that are dispatched via the actor model.
     *
     */
    public void executeReceiveRequest(Request fromUpstream, Registry registry) {
        LOG.trace("{}: Receiving a new Request from upstream: {}", name, fromUpstream);
        if (!isInitialised) {
            LOG.debug(name + ": initialising downstream actors");
            initialiseDownstreamActors(registry);
            isInitialised = true;
        }

        ResponseProducer responseProducer = responseProducers.computeIfAbsent(fromUpstream, key -> {
            LOG.debug("{}: Creating a new ResponseProducer for the given Request: {}", name, fromUpstream);
            return createResponseProducer(fromUpstream);
        });
        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducer);
        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveAnswer(Response.Answer fromDownstream, Registry registry) {
        LOG.trace("{}: Receiving a new Answer from downstream: {}", name, fromDownstream);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveExhausted(Response.Exhausted fromDownstream, Registry registry) {
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
    private void requestFromDownstream(Request request, Request fromUpstream, Registry registry) {
        LOG.trace("{} : Sending a new Request in order to request for an answer from downstream: {}", name, request);
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends ExecutionActor<?>> receiver = request.receiver();
        receiver.tell(actor -> actor.executeReceiveRequest(request, registry));
    }

    private void respondToUpstream(Response response, Registry registry) {
        LOG.trace("{}: Sending a new Response to respond with an answer to upstream: {}", name, response);
        Actor<? extends ExecutionActor<?>> receiver = response.sourceRequest().sender();
        if (receiver == null) {
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("{}: Writing a new Response to output queue", name);
            responses.add(response);
        } else {
            if (response.isAnswer()) {
                LOG.trace("{} : Sending a new Response.Answer to upstream", name);
                receiver.tell(actor -> actor.executeReceiveAnswer(response.asAnswer(), registry));
            } else if (response.isExhausted()) {
                LOG.trace("{}: Sending a new Response.Exhausted to upstream", name);
                receiver.tell(actor -> actor.executeReceiveExhausted(response.asExhausted(), registry));
            } else {
                throw new RuntimeException(("Unknown message type " + response.getClass().getSimpleName()));
            }
        }
    }

}
