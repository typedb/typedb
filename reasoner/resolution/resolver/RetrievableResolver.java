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

import grakn.core.common.concurrent.actor.Actor;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import grakn.core.traversal.TraversalEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetrievableResolver extends ResolvableResolver<RetrievableResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RetrievableResolver.class);

    public RetrievableResolver(Actor<RetrievableResolver> self, Retrievable retrievable, ResolverRegistry registry, TraversalEngine traversalEngine) {
        super(self, RetrievableResolver.class.getSimpleName() + "(pattern: " + retrievable + ")", registry, traversalEngine);
    }


    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {
        // TODO
    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {
        // TODO
    }

    @Override
    protected void receiveExhausted(Response.Exhausted fromDownstream, int iteration) {
        // TODO
    }

    @Override
    protected void initialiseDownstreamActors() {
        // TODO
    }

    @Override
    protected ResponseProducer responseProducerCreate(Request fromUpstream, int iteration) {
        return null; // TODO
    }

    @Override
    protected ResponseProducer responseProducerReiterate(Request fromUpstream, ResponseProducer responseProducer, int newIteration) {
        return null; // TODO
    }
}
