/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution.resolver;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public class Materialiser extends Actor<Materialiser> {
    private static final Logger LOG = LoggerFactory.getLogger(Materialiser.class);

    private final ResolverRegistry registry;
    private final TraversalEngine traversalEngine;
    private final ConceptManager conceptMgr;
    private final boolean resolutionTracing;

    public Materialiser(Driver<Materialiser> driver, ResolverRegistry registry, TraversalEngine traversalEngine,
                        ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, Materialiser.class.getSimpleName());
        this.registry = registry;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.resolutionTracing = resolutionTracing;
    }

    public void receiveRequest(Request request) {
        if (isTerminated()) return;
        Optional<Map<Identifier.Variable, Concept>> materialisation = request.conclusion()
                .materialise(request.partialAnswer().conceptMap(), traversalEngine, conceptMgr);
        if (resolutionTracing) {
            if (materialisation.isPresent()) {
                ResolutionTracer.get().responseAnswer(
                        this.name(), request.sender().name(), -1, materialisation.get().keySet().toString());
            } else {
                ResolutionTracer.get().responseExhausted(this.name(), request.sender().name(), -1);
            }
        }
        Response response = new Response(request, materialisation.orElse(null), request.partialAnswer());
        request.sender().execute(actor -> actor.receiveMaterialisation(response));
    }

    @Override
    protected void exception(Throwable e) {
        if (e instanceof TypeDBException && ((TypeDBException) e).code().isPresent()) {
            String code = ((TypeDBException) e).code().get();
            if (code.equals(RESOURCE_CLOSED.code())) {
                LOG.debug("Resolver interrupted by resource close: {}", e.getMessage());
                registry.terminate(e);
                return;
            }
        }
        LOG.error("Actor exception", e);
        registry.terminate(e);
    }

    public static class Request {

        private final Driver<BoundConclusionResolver> sender;
        private final Actor.Driver<Materialiser> receiver;
        private final Rule.Conclusion conclusion;
        private final AnswerState.Partial<?> partialAnswer;

        private Request(Actor.Driver<BoundConclusionResolver> sender, Actor.Driver<Materialiser> receiver,
                        Rule.Conclusion conclusion, AnswerState.Partial<?> partialAnswer) {
            this.sender = sender;
            this.receiver = receiver;
            this.conclusion = conclusion;
            this.partialAnswer = partialAnswer;
        }

        public static Request create(Actor.Driver<BoundConclusionResolver> sender, Actor.Driver<Materialiser> receiver,
                                     Rule.Conclusion conclusion, AnswerState.Partial<?> partialAnswer) {
            return new Request(sender, receiver, conclusion, partialAnswer);
        }

        public Rule.Conclusion conclusion() {
            return conclusion;
        }

        public AnswerState.Partial<?> partialAnswer() {
            return partialAnswer;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "sender=" + sender +
                    ", receiver=" + receiver +
                    ", conclusion=" + conclusion +
                    '}';
        }

        public Driver<BoundConclusionResolver> sender() {
            return sender;
        }

        public Actor.Driver<Materialiser> receiver() {
            return receiver;
        }
    }

    static class Response {

        private final Request request;
        private final Map<Identifier.Variable, Concept> materialisation;
        private final AnswerState.Partial<?> partialAnswer;

        private Response(Request request, @Nullable Map<Identifier.Variable, Concept> materialisation, AnswerState.Partial<?> partialAnswer) {
            this.request = request;
            this.materialisation = materialisation;
            this.partialAnswer = partialAnswer;
        }

        public Optional<Map<Identifier.Variable, Concept>> materialisation() {
            return Optional.ofNullable(materialisation);
        }

        public AnswerState.Partial<?> partialAnswer() {
            return partialAnswer;
        }

        public Request sourceRequest() {
            return request;
        }
    }
}
