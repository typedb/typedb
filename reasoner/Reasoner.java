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

package grakn.core.reasoner;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Type;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.producer.Producer;
import grakn.core.concurrent.producer.Producers;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_CONJUNCTION;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static grakn.core.concurrent.common.ExecutorService.PARALLELISATION_FACTOR;
import static grakn.core.concurrent.common.ExecutorService.eventLoop;
import static grakn.core.concurrent.producer.Producers.produce;

public class Reasoner {
    private static final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final ResolverRegistry resolverRegistry;
    private final Actor<ResolutionRecorder> resolutionRecorder; // for explanations
    private final Context.Query defaultContext;

    public Reasoner(ConceptManager conceptMgr, LogicManager logicMgr,
                    TraversalEngine traversalEng, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.defaultContext = new Context.Query(context, new Options.Query());
        this.defaultContext.producer(EXHAUSTIVE);
        this.resolutionRecorder = Actor.create(eventLoop(), ResolutionRecorder::new);
        this.resolverRegistry = new ResolverRegistry(eventLoop(), resolutionRecorder, traversalEng, conceptMgr, logicMgr);
    }

    ResolverRegistry resolverRegistry() {
        return resolverRegistry;
    }

    private Producer<ConceptMap> resolve(Conjunction conjunction) {
        return new ReasonerProducer(conjunction, resolverRegistry);
    }

    private boolean isInfer(Context.Query context) {
        return context.options().infer() && !context.transactionType().isWrite() && logicMgr.rules().hasNext();
    }

    private boolean conjunctionContainsThings(Conjunction conjunction, List<Identifier.Variable.Name> filter) {
        return !filter.isEmpty() && iterate(filter).anyMatch(id -> conjunction.variable(id).isThing()) ||
                iterate(conjunction.variables()).anyMatch(Variable::isThing);
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction, List<Identifier.Variable.Name> filter,
                                                Context.Query context) {
        ResourceIterator<Conjunction> conjs = iterate(disjunction.conjunctions());
        if (!context.options().parallel()) return conjs.flatMap(conj -> iterator(conj, filter, context));
        else return produce(conjs.map(conj -> producer(conj, filter, context)).toList(), context.producer());
    }

    private Producer<ConceptMap> producer(Conjunction conjunction) {
        return producer(conjunction, list(), defaultContext);
    }

    private Producer<ConceptMap> producer(Conjunction conjunction, List<Identifier.Variable.Name> filter,
                                          Context.Query context) {
        Producer<ConceptMap> producer;
        logicMgr.typeResolver().resolve(conjunction);
        if (conjunction.isSatisfiable()) {
            if (isInfer(context)) producer = resolve(conjunction);
            else producer = traversalEng.producer(
                    conjunction.traversal(filter), context.producer(), PARALLELISATION_FACTOR
            ).map(conceptMgr::conceptMap);
        } else if (!conjunction.isBounded() && conjunctionContainsThings(conjunction, filter)) {
            throw GraknException.of(UNSATISFIABLE_CONJUNCTION, conjunction);
        } else {
            return Producers.empty();
        }

        if (conjunction.negations().isEmpty()) return producer;
        else return producer.filter(ans -> !iterate(conjunction.negations())
                .flatMap(negation -> iterator(negation.disjunction(), ans)).hasNext()
        );
    }

    public Producer<ConceptMap> producer(Conjunction conjunction, ConceptMap bounds) {
        return producer(bound(conjunction, bounds));
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction) {
        return iterator(conjunction, list(), defaultContext);
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction, List<Identifier.Variable.Name> filter,
                                                  Context.Query context) {
        ResourceIterator<ConceptMap> answers;
        logicMgr.typeResolver().resolve(conjunction);
        if (conjunction.isSatisfiable()) {
            if (isInfer(context)) answers = produce(resolve(conjunction), context.producer());
            else answers = traversalEng.iterator(conjunction.traversal(filter)).map(conceptMgr::conceptMap);
        } else if (!conjunction.isBounded() && conjunctionContainsThings(conjunction, filter)) {
            throw GraknException.of(UNSATISFIABLE_CONJUNCTION, conjunction);
        } else {
            return Iterators.empty();
        }

        if (conjunction.negations().isEmpty()) return answers;
        else return answers.filter(answer -> !iterate(conjunction.negations()).flatMap(
                negation -> iterator(negation.disjunction(), answer)
        ).hasNext());
    }

    private ResourceIterator<ConceptMap> iterator(Disjunction disjunction, ConceptMap bounds) {
        return iterate(disjunction.conjunctions()).flatMap(c -> iterator(c, bounds));
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction, ConceptMap bounds) {
        return iterator(bound(conjunction, bounds));
    }

    private Conjunction bound(Conjunction conjunction, ConceptMap bounds) {
        Conjunction newClone = conjunction.clone();
        newClone.bound(bounds.toMap(Type::getLabel, Thing::getIID));
        return newClone;
    }
}
