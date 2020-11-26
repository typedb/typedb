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
import grakn.core.reasoner.resolution.MockTransaction;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Answer;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.reasoner.resolution.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class Concludable extends Resolver<Concludable> {
    private static final Logger LOG = LoggerFactory.getLogger(Concludable.class);

    private final Long traversalPattern;
    private final long traversalSize;
    private final List<List<Long>> rules;
    private final Map<Actor<Rule>, List<Long>> ruleActorSources;
    private final Set<ConceptMap> receivedConceptMaps;
    private Actor<ResolutionRecorder> resolutionRecorder;

    public Concludable(Actor<Concludable> self, Long traversalPattern, List<List<Long>> rules, long traversalSize) {
        super(self, Concludable.class.getSimpleName() + "(pattern: " + traversalPattern + ")");
        this.traversalPattern = traversalPattern;
        this.traversalSize = traversalSize;
        this.rules = rules;
        ruleActorSources = new HashMap<>();
        receivedConceptMaps = new HashSet<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream,
                                                   ResponseProducer responseProducer) {
        // TODO may combine with partial concept maps from the fromUpstream message

        LOG.trace("{}: hasProduced: {}", name, fromDownstream.answer().conceptMap());
        if (!responseProducer.hasProduced(fromDownstream.answer().conceptMap())) {
            responseProducer.recordProduced(fromDownstream.answer().conceptMap());

            // update partial derivation provided from upstream to carry derivations sideways
            Answer.Derivation derivation = new Answer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(), fromDownstream.answer())));
            Answer answer = new Answer(fromDownstream.answer().conceptMap(), traversalPattern.toString(), derivation, self());

            return Either.second(new Response.Answer(fromUpstream, answer, fromUpstream.unifiers()));
        } else {
            Answer.Derivation derivation = new Answer.Derivation(map(pair(fromDownstream.sourceRequest().receiver(), fromDownstream.answer())));
            Answer deduplicated = new Answer(fromDownstream.answer().conceptMap(), traversalPattern.toString(), derivation, self());
            LOG.debug("Recording deduplicated answer: {}", deduplicated);
            resolutionRecorder.tell(actor -> actor.record(deduplicated));

            return produceMessage(fromUpstream, responseProducer);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    protected ResponseProducer createResponseProducer(Request request) {
        Iterator<ConceptMap> traversal = (new MockTransaction(traversalSize)).query(traversalPattern, request.partialConceptMap());
        ResponseProducer responseProducer = new ResponseProducer(traversal);

        if (!receivedConceptMaps.contains(request.partialConceptMap())) {
            registerDownstreamRules(responseProducer, request.path(), request.partialConceptMap(), request.unifiers());
            receivedConceptMaps.add(request.partialConceptMap());
        }
        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(ResolverRegistry registry) {
        resolutionRecorder = registry.resolutionRecorder();
        for (List<Long> rule : rules) {
            Actor<Rule> ruleActor = registry.registerRule(rule, 1L);
            ruleActorSources.put(ruleActor, rule);
        }
    }

    private Either<Request, Response> produceMessage(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            ConceptMap conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: hasProduced: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                Answer answer = new Answer(conceptMap, traversalPattern.toString(), new Answer.Derivation(map()), self());
                return Either.second(new Response.Answer(fromUpstream, answer, fromUpstream.unifiers()));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    private void registerDownstreamRules(ResponseProducer responseProducer, Request.Path path, ConceptMap partialConceptMap,
                                         List<Object> unifiers) {
        for (Actor<Rule> ruleActor : ruleActorSources.keySet()) {
            Request toDownstream = new Request(path.append(ruleActor), partialConceptMap, unifiers, Answer.Derivation.EMPTY);
            responseProducer.addDownstreamProducer(toDownstream);
        }
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }
}

