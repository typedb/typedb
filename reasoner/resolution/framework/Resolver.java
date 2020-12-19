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

import grakn.common.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.ResolverRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class Resolver<T extends Resolver<T>> extends Actor.State<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    protected final String name;
    private final Map<Request, Request> requestRouter;

    protected Resolver(Actor<T> self, String name) {
        super(self);
        this.name = name;
        this.requestRouter = new HashMap<>();
    }

    public String name() {
        return name;
    }

    protected abstract void initialiseDownstreamActors(ResolverRegistry registry);

    protected abstract ResponseProducer responseProducerCreate(Request fromUpstream, int iteration);

    protected abstract ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducer, int iteration);

    public abstract void executeReceiveRequest(Request fromUpstream, ResolverRegistry registry, int iteration);

    protected abstract void executeReceiveAnswer(Response.Answer fromDownstream, ResolverRegistry registry, int iteration);

    protected abstract void executeReceiveExhausted(Response.Exhausted fromDownstream, ResolverRegistry registry, int iteration);

    protected Request fromUpstream(Request toDownstream) {
        assert requestRouter.containsKey(toDownstream);
        return requestRouter.get(toDownstream);
    }

    protected void requestFromDownstream(Request request, Request fromUpstream, ResolverRegistry registry, int iteration) {
        LOG.trace("{} : Sending a new answer Request to downstream: {}", name, request);
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends Resolver<?>> receiver = request.receiver();
        receiver.tell(actor -> actor.executeReceiveRequest(request, registry, iteration));
    }
}
