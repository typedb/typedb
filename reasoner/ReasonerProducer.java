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
 */

package grakn.core.reasoner;

import grakn.common.concurrent.actor.Actor;
import grakn.core.common.producer.Producer;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.logic.transform.TransformedConceptMap;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.RootResolver;

public class ReasonerProducer implements Producer<ConceptMap> {

    private final Actor<RootResolver> rootResolver;
    private final ResolverRegistry registry;
    private final Request resolveRequest;
    private boolean done;
    private Sink<ConceptMap> sink = null;


    public ReasonerProducer(Conjunction conjunction, ResolverRegistry resolverRegistry) {
        this.rootResolver = resolverRegistry.createRoot(conjunction, this::onAnswer, this::onDone);
        this.resolveRequest = new Request(new Request.Path(rootResolver), TransformedConceptMap.empty(), ResolutionAnswer.Derivation.EMPTY);
        this.registry = resolverRegistry;
    }

    private void onAnswer(final ResolutionAnswer answer) {
        sink.put(answer.conceptMap());
    }

    private void onDone() {
        if (!done) {
            done = true;
            sink.done(this);
        }
    }

    @Override
    public void produce(Sink<ConceptMap> sink, int count) {
        assert this.sink == null || this.sink == sink;
        this.sink = sink;
        for (int i = 0; i < count; i++) {
            rootResolver.tell(actor -> actor.executeReceiveRequest(resolveRequest, registry));
        }
    }

    @Override
    public void recycle() {

    }
}