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

package grakn.core.reasoner.resolution.resolver;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.Rule;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO unify and materialise in receiveAnswer
public class RuleResolver extends ConjunctionResolver<RuleResolver> {
    private static final Logger LOG = LoggerFactory.getLogger(RuleResolver.class);

    public RuleResolver(Actor<RuleResolver> self, Rule rule) {
        super(self, RuleResolver.class.getSimpleName() + "(rule:" + rule + ")", rule.when());
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        return produceMessage(fromUpstream, responseProducer);
    }

    Either<Request, Response> produceMessage(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: traversal answer: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                ResolutionAnswer answer = new ResolutionAnswer(conceptMap, conjunction.toString(), ResolutionAnswer.Derivation.EMPTY, self());
                return Either.second(new Response.Answer(fromUpstream, answer));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    @Override
    Response.Answer createResponse(Request fromUpstream, ResolutionAnswer answer) {
        return new Response.Answer(fromUpstream, answer);
    }
}
