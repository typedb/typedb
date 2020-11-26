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
import grakn.core.common.async.Producer;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.framework.Answer;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.resolver.Root;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class ReasonerProducer extends Producer<ConceptMap> {

    private final Actor<Root> rootResolver;
    private final ResolverRegistry registry;
    private final Request resolveRequest;
    private boolean done;


//    public ReasonerProducer(Conjunction conjunction, ResolverRegistry registry, Consumer<ConceptMap> onAnswer, Runnable onDone) {
//        super(onAnswer, onDone);
//        TODO update
//        this.rootResolver = registry.createRoot(conjunction, 1L, this::onAnswer, this::onExhausted);
//        this.resolveRequest = new Request(new Request.Path(rootResolver), new ConceptMap(), Arrays.asList(), Answer.Derivation.EMPTY);
//        this.registry = registry;
//    }

    // TODO remove, only for testing
    public ReasonerProducer(final List<Long> conjunctionPattern, final ResolverRegistry registry, final long conjunctionTraversalSize,
                            Consumer<ConceptMap> onAnswer, Runnable onDone) {
        super(onAnswer, onDone);
        this.rootResolver = registry.createRoot(conjunctionPattern, conjunctionTraversalSize, this::onAnswer, this::onDone);
        this.resolveRequest = new Request(new Request.Path(rootResolver), new ConceptMap(), Arrays.asList(), Answer.Derivation.EMPTY);
        this.registry = registry;
    }

    @Override
    public void next() {
        rootResolver.tell(actor -> actor.executeReceiveRequest(resolveRequest, registry));
    }

    private void onAnswer(final Answer answer) {
        onAnswer.accept(answer.conceptMap());
    }

    private void onDone() {
        this.done = true;
        onDone.run();
    }
}
