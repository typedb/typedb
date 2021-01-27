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
 *
 */

package grakn.core.reasoner.resolution.framework;

import grakn.core.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class Resolver<T extends Resolver<T>> extends Actor.State<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    private final String name;
    private final Map<Request, Request> requestRouter;
    protected final ResolverRegistry registry;
    protected final TraversalEngine traversalEngine;
    private final boolean explanations;

    protected Resolver(Actor<T> self, String name, ResolverRegistry registry, TraversalEngine traversalEngine, boolean explanations) {
        super(self);
        this.name = name;
        this.registry = registry;
        this.traversalEngine = traversalEngine;
        this.explanations = explanations;
        this.requestRouter = new HashMap<>();
        // Note: initialising downstream actors in constructor will create all actors ahead of time, so it is non-lazy
        // additionally, it can cause deadlock within ResolverRegistry as different threads initialise actors
    }

    public String name() {
        return name;
    }

    protected boolean explanations() { return explanations; }

    public abstract void receiveRequest(Request fromUpstream, int iteration);

    protected abstract void receiveAnswer(Response.Answer fromDownstream, int iteration);

    protected abstract void receiveExhausted(Response.Exhausted fromDownstream, int iteration);

    protected abstract void initialiseDownstreamActors();

    protected abstract ResponseProducer responseProducerCreate(Request fromUpstream, int iteration);

    protected abstract ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducer, int newIteration);

    protected Request fromUpstream(Request toDownstream) {
        assert requestRouter.containsKey(toDownstream);
        return requestRouter.get(toDownstream);
    }

    protected void requestFromDownstream(Request request, Request fromUpstream, int iteration) {
        LOG.trace("{} : Sending a new answer Request to downstream: {}", name, request);
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends Resolver<?>> receiver = request.receiver();
        receiver.tell(actor -> actor.receiveRequest(request, iteration));
    }

    protected void respondToUpstream(Response response, int iteration) {
        Actor<? extends Resolver<?>> receiver = response.sourceRequest().sender();
        if (response.isAnswer()) {
            LOG.trace("{} : Sending a new Response.Answer to upstream", name());
            receiver.tell(actor -> actor.receiveAnswer(response.asAnswer(), iteration));
        } else if (response.isExhausted()) {
            LOG.trace("{}: Sending a new Response.Exhausted to upstream", name());
            receiver.tell(actor -> actor.receiveExhausted(response.asExhausted(), iteration));
        } else {
            throw new RuntimeException(("Unknown response type " + response.getClass().getSimpleName()));
        }
    }

    protected Traversal boundTraversal(Traversal traversal, ConceptMap bounds) {
        bounds.concepts().forEach((ref, concept) -> {
            if (concept.isThing()) traversal.iid(Identifier.Variable.of(ref), concept.asThing().getIID());
            else traversal.labels(Identifier.Variable.of(ref), concept.asType().getLabel());
        });
        return traversal;
    }
}
