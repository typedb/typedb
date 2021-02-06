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
import grakn.core.common.parameters.Label;
import grakn.core.common.parameters.Options;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Type;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.producer.Producer;
import grakn.core.logic.LogicManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_CONJUNCTION;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static grakn.core.concurrent.common.Executors.PARALLELISATION_FACTOR;
import static grakn.core.concurrent.common.Executors.asyncPool1;
import static grakn.core.concurrent.common.Executors.eventLoopGroup;
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
        this.resolutionRecorder = Actor.create(eventLoopGroup(), ResolutionRecorder::new);
        this.resolverRegistry = new ResolverRegistry(eventLoopGroup(), resolutionRecorder, traversalEng, conceptMgr, logicMgr);
    }

    ResolverRegistry resolverRegistry() {
        return resolverRegistry;
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction, Set<Identifier.Variable.Name> filter,
                                                Context.Query context) {

        Disjunction satisfiableDisjunction = resolveTypesAndFilter(disjunction, filter);
        if (satisfiableDisjunction.conjunctions().isEmpty()) return Iterators.empty();
        if (isInfer(satisfiableDisjunction, context)) return resolve(satisfiableDisjunction, filter, context);

        ResourceIterator<ConceptMap> answers;
        ResourceIterator<Conjunction> conjs = iterate(satisfiableDisjunction.conjunctions());
        if (!context.options().parallel()) answers = conjs.flatMap(conj -> iterator(conj, filter, context));
        else answers = produce(conjs.map(c -> producer(c, filter, context)).toList(), context.producer(), asyncPool1());
        if (satisfiableDisjunction.conjunctions().size() > 1) answers = answers.distinct();
        return answers;
    }

    private boolean isInfer(Disjunction disjunction, Context.Query context) {
        if (!context.options().infer() || context.transactionType().isWrite() || !logicMgr.rules().hasNext()) {
            return false;
        }
        return true;
//        for (Conjunction conj : disjunction.conjunctions()) {
//            for (Variable var : conj.variables()) {
//                if (var.resolvedTypes().isEmpty()) return true;
//                for (Label type : var.resolvedTypes()) {
//                    if (logicMgr.rulesConcluding(type).hasNext() || logicMgr.rulesConcludingHas(type).hasNext()) {
//                        return true;
//                    }
//                }
//            }
//        }
//        return false;
    }

    private ResourceIterator<ConceptMap> resolve(Disjunction disjunction, Set<Identifier.Variable.Name> filter, Context.Query context) {
        Set<Reference.Name> nameFilter = iterate(filter).map(Identifier.Variable.Name::reference).toSet();
        if (disjunction.conjunctions().size() == 1) {
            return produce(new ReasonerProducer(disjunction.conjunctions().get(0), resolverRegistry, nameFilter), context.producer(), asyncPool1());
        } else {
            return produce(new ReasonerProducer(disjunction, resolverRegistry, nameFilter), context.producer(), asyncPool1());
        }
    }

    private Disjunction resolveTypesAndFilter(Disjunction disjunction, Set<Identifier.Variable.Name> filter) {
        disjunction.conjunctions().forEach(conj -> logicMgr.typeResolver().resolve(conj));
        for (Conjunction conjunction : disjunction.conjunctions()) {
            if (!conjunction.isSatisfiable() && !conjunction.isBounded() && conjunctionContainsThings(conjunction, filter)) {
                throw GraknException.of(UNSATISFIABLE_CONJUNCTION, conjunction);
            }
        }
        List<Conjunction> satisfiable = iterate(disjunction.conjunctions()).filter(Conjunction::isSatisfiable).toList();
        return new Disjunction(satisfiable);
    }

    private boolean conjunctionContainsThings(Conjunction conjunction, Set<Identifier.Variable.Name> filter) {
        return !filter.isEmpty() && iterate(filter).anyMatch(id -> conjunction.variable(id).isThing()) ||
                iterate(conjunction.variables()).anyMatch(Variable::isThing);
    }

    // ---- non-reasoning paths ----

    private Producer<ConceptMap> producer(Conjunction conjunction, Set<Identifier.Variable.Name> filter,
                                          Context.Query context) {
        Producer<ConceptMap> producer = traversalEng.producer(
                conjunction.traversal(filter), context.producer(), PARALLELISATION_FACTOR
        ).map(conceptMgr::conceptMap);

        if (conjunction.negations().isEmpty()) return producer;
        else return producer.filter(answer -> !iterate(conjunction.negations())
                .flatMap(negation -> iterator(negation.disjunction(), answer)).hasNext()
        );
    }

    private ResourceIterator<ConceptMap> iterator(Disjunction disjunction, ConceptMap bounds) {
        Disjunction satisfiableDisjunction = resolveTypesAndFilter(disjunction, set());
        if (satisfiableDisjunction.conjunctions().isEmpty()) return Iterators.empty();
        return iterate(satisfiableDisjunction.conjunctions()).flatMap(c -> iterator(c, bounds));
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction, ConceptMap bounds) {
        return iterator(bound(conjunction, bounds));
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction) {
        return iterator(conjunction, set(), defaultContext);
    }

    private ResourceIterator<ConceptMap> iterator(Conjunction conjunction, Set<Identifier.Variable.Name> filter,
                                                  Context.Query context) {
        ResourceIterator<ConceptMap> answers = traversalEng.iterator(conjunction.traversal(filter))
                .map(conceptMgr::conceptMap);
        if (conjunction.negations().isEmpty()) return answers;
        else return answers.filter(answer -> !iterate(conjunction.negations()).flatMap(
                negation -> iterator(negation.disjunction(), answer)
        ).hasNext());
    }

    private Conjunction bound(Conjunction conjunction, ConceptMap bounds) {
        Conjunction newClone = conjunction.clone();
        newClone.bound(bounds.toMap(Type::getLabel, Thing::getIID));
        return newClone;
    }
}
