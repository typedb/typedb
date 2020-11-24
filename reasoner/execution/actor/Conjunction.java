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

package grakn.core.reasoner.execution.actor;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.core.reasoner.execution.AnswerRecorder;
import grakn.core.reasoner.execution.MockTransaction;
import grakn.core.reasoner.execution.Registry;
import grakn.core.reasoner.execution.framework.Answer;
import grakn.core.reasoner.execution.framework.ExecutionActor;
import grakn.core.reasoner.execution.framework.Request;
import grakn.core.reasoner.execution.framework.Response;
import grakn.core.reasoner.execution.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;

public class Conjunction<T extends Conjunction<T>> extends ExecutionActor<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

    private final Long traversalSize;
    private final Long traversalOffset;
    private final List<Long> conjunction;
    private final List<Actor<Concludable>> plannedConcludables;
    private Actor<AnswerRecorder> executionRecorder;

    public Conjunction(Actor<T> self, String name, List<Long> conjunction, Long traversalSize,
                       Long traversalOffset, LinkedBlockingQueue<Response> responses) {
        super(self, name, responses);

        this.conjunction = conjunction;
        this.traversalSize = traversalSize;
        this.traversalOffset = traversalOffset;
        this.plannedConcludables = new ArrayList<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer) {
        Actor<? extends ExecutionActor<?>> sender = fromDownstream.sourceRequest().receiver();
        List<Long> conceptMap = list(conjunction, fromDownstream.answer().conceptMap());

        Answer.Derivation derivation = fromDownstream.sourceRequest().partialResolutions();
        if (fromDownstream.answer().isInferred()) {
            derivation = derivation.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
        }

        if (isLast(sender)) {
            LOG.trace("{}: hasProduced: {}", name, conceptMap);

            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);

                Answer answer = new Answer(conceptMap, conjunction.toString(), derivation, self());
                Response.Answer response = new Response.Answer(fromUpstream, answer, fromUpstream.unifiers());
                if (fromUpstream.sender() == null) {
                    LOG.debug("Recording root answer execution tree for: {}", answer.conceptMap());
                    executionRecorder.tell(state -> state.record(answer));
                }
                return Either.second(response);
            } else {
                return produceMessage(fromUpstream, responseProducer);
            }
        } else {
            Actor<Concludable> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream),
                    conceptMap, fromDownstream.unifiers(), derivation);
            responseProducer.addDownstreamProducer(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());

        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    protected ResponseProducer createResponseProducer(Request request) {
        Iterator<List<Long>> traversal = (new MockTransaction(traversalSize, traversalOffset, 1)).query(conjunction);
        ResponseProducer responseProducer = new ResponseProducer(traversal);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0)), request.partialConceptMap(),
                request.unifiers(), new Answer.Derivation(map()));
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(Registry registry) {
        executionRecorder = registry.executionRecorder();
        List<Long> planned = list(conjunction);
        // in the future, we'll check if the atom is rule resolvable first
        for (Long atomicPattern : planned) {
            Actor<Concludable> atomicActor = registry.registerConcludable(atomicPattern, (pattern) ->
                    Actor.create(self().eventLoopGroup(), (newActor) -> new Concludable(newActor, pattern, Arrays.asList(), 5L)));
            plannedConcludables.add(atomicActor);
        }
    }

    private Either<Request, Response> produceMessage(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            List<Long> conceptMap = responseProducer.traversalProducer().next();
            LOG.trace("{}: traversal answer: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                Answer answer = new Answer(conceptMap, conjunction.toString(), Answer.Derivation.EMPTY, self());
                return Either.second(new Response.Answer(fromUpstream, answer, fromUpstream.unifiers()));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    private boolean isLast(Actor<? extends ExecutionActor<?>> actor) {
        return plannedConcludables.get(plannedConcludables.size() - 1).equals(actor);
    }

    private Actor<Concludable> nextPlannedDownstream(Actor<? extends ExecutionActor<?>> actor) {
        return plannedConcludables.get(plannedConcludables.indexOf(actor) + 1);
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }
}
