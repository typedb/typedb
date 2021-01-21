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

import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.concurrent.actor.Actor;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.BaseIterator;
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.producer.Producer;
import grakn.core.common.producer.Producers;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegister;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.concurrent.ExecutorService.PARALLELISATION_FACTOR;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_CONJUNCTION;
import static grakn.core.common.exception.ErrorMessage.ThingRead.CONTRADICTORY_BOUND_VARIABLE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.single;
import static grakn.core.common.producer.Producers.produce;

public class Reasoner {
    private static final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Context.Transaction context;
    private final ResolverRegister resolverManager;
    private final Actor<ResolutionRecorder> resolutionRecorder; // for explanations

    public Reasoner(TraversalEngine traversalEng, ConceptManager conceptMgr, LogicManager logicMgr, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.context = context;
        this.resolutionRecorder = Actor.create(ExecutorService.eventLoopGroup(), ResolutionRecorder::new);
        this.resolverManager = new ResolverRegister(
                ExecutorService.eventLoopGroup(), resolutionRecorder, traversalEng, conceptMgr, logicMgr
        );
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction, List<Identifier.Variable.Name> filter,
                                                boolean isParallel) {
        BaseIterator<Conjunction> conjunctions = iterate(disjunction.conjunctions());
        if (!isParallel) return conjunctions.flatMap(conj -> iterator(conj, filter));
        else return produce(conjunctions.flatMap(conj -> producers(conj, filter)).toList());
    }

    private ResourceIterator<Producer<ConceptMap>> producers(Conjunction conjunction) {
        return producers(conjunction, list());
    }

    private ResourceIterator<Producer<ConceptMap>> producers(Conjunction conjunction,
                                                             List<Identifier.Variable.Name> filter) {
        if (context.isSchemaWrite()) LOG.warn("Reasoning is disabled in schema write transactions");

        List<Producer<ConceptMap>> answerProducers = new ArrayList<>();
        final Conjunction conj = logicMgr.typeResolver().resolve(conjunction);
        if (conj.isSatisfiable()) {
            answerProducers.add(traversalEng.producer(conj.traversal(filter), PARALLELISATION_FACTOR).map(conceptMgr::conceptMap));
            if (!context.isSchemaWrite()) answerProducers.add(this.resolve(conj));
        } else if (!filter.isEmpty() && iterate(filter).anyMatch(id -> conj.variable(id).isThing()) ||
                iterate(conjunction.variables()).anyMatch(Variable::isThing)) {
            throw GraknException.of(UNSATISFIABLE_CONJUNCTION, conjunction);
        } else {
            return single(Producers.empty());
        }

        if (conjunction.negations().isEmpty()) return iterate(answerProducers);
        else return iterate(answerProducers).map(p -> p.filter(answer -> !produce(
                iterate(conjunction.negations()).flatMap(n -> iterate(producers(n.disjunction(), answer))).toList()
        ).hasNext()));
    }

    private ResourceIterator<Producer<ConceptMap>> producers(Disjunction disjunction, ConceptMap bounds) {
        return iterate(disjunction.conjunctions()).flatMap(conj -> iterate(producers(conj, bounds)));
    }

    public ResourceIterator<Producer<ConceptMap>> producers(Conjunction conjunction, ConceptMap bounds) {
        return producers(bound(conjunction, bounds));
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction) {
        return iterator(conjunction, list());
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction, List<Identifier.Variable.Name> filter) {
        if (context.isSchemaWrite()) LOG.warn("Reasoning is disabled in schema write transactions");

        ResourceIterator<ConceptMap> answers;
        final Conjunction conj = logicMgr.typeResolver().resolve(conjunction);
        if (conj.isSatisfiable()) {
            answers = traversalEng.iterator(conjunction.traversal(filter)).map(conceptMgr::conceptMap);
            if (!context.isSchemaWrite()) answers = link(answers, produce(resolve(conj)));
        } else if (!filter.isEmpty() && iterate(filter).anyMatch(id -> conj.variable(id).isThing()) ||
                iterate(conjunction.variables()).anyMatch(Variable::isThing)) {
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
        newClone.forEach(var -> {
            if (var.id().isName() && bounds.contains(var.id().reference().asName())) {
                Concept boundVar = bounds.get(var.id().reference().asName());
                if (var.isType() != boundVar.isType()) throw GraknException.of(CONTRADICTORY_BOUND_VARIABLE, var);
                else if (var.isType()) var.asType().label(boundVar.asType().getLabel());
                else if (var.isThing()) var.asThing().iid(boundVar.asThing().getIID());
                else throw GraknException.of(ILLEGAL_STATE);
            }
        });
        return newClone;
    }

    ResolverRegister resolverManager() {
        return resolverManager;
    }

    private Producer<ConceptMap> resolve(Conjunction conjunction) {
        return Producers.empty();
        // TODO enable reasoner when ready!
        // return new ReasonerProducer(conjunction, resolverManager);
    }
}
