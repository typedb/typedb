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

import grakn.common.collection.Either;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.iterator.Iterators;
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
import grakn.core.pattern.Negation;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.resolution.ResolutionRecorder;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import graql.lang.query.GraqlMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static grakn.common.collection.Collections.list;
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
    private final Actor.Driver<ResolutionRecorder> resolutionRecorder; // for explanations
    private final Context.Query defaultContext;

    public Reasoner(ConceptManager conceptMgr, LogicManager logicMgr,
                    TraversalEngine traversalEng, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.defaultContext = new Context.Query(context, new Options.Query());
        this.defaultContext.producer(Either.first(EXHAUSTIVE));
        this.resolutionRecorder = Actor.driver(ResolutionRecorder::new, eventLoopGroup());
        this.resolverRegistry = new ResolverRegistry(eventLoopGroup(), resolutionRecorder, traversalEng, conceptMgr,
                                                     logicMgr, this.defaultContext.options().traceInference());
    }

    ResolverRegistry resolverRegistry() {
        return resolverRegistry;
    }


    private boolean mayReason(Disjunction disjunction, Context.Query context) {
        if (!context.options().infer() || context.transactionType().isWrite() || !logicMgr.rules().hasNext()) {
            return false;
        }
        return mayReason(disjunction);
    }

    private boolean mayReason(Disjunction disjunction) {
        for (Conjunction conj : disjunction.conjunctions()) {
            Set<Variable> vars = conj.variables();
            Set<Negation> negs = conj.negations();
            if (iterate(vars).anyMatch(v -> v.resolvedTypes().isEmpty())) return true;
            if (iterate(vars).flatMap(v -> iterate(v.resolvedTypes())).distinct().anyMatch(this::hasRule)) return true;
            if (!negs.isEmpty() && iterate(negs).anyMatch(n -> mayReason(n.disjunction()))) return true;
        }
        return false;
    }

    private boolean hasRule(Label type) {
        return logicMgr.rulesConcluding(type).hasNext() || logicMgr.rulesConcludingHas(type).hasNext();
    }

    public FunctionalIterator<ConceptMap> execute(Disjunction disjunction, GraqlMatch.Modifiers modifiers, Context.Query context) {
        resolveTypes(disjunction, list());
        Set<Identifier.Variable.Name> filter =
                iterate(modifiers.filter()).map(v -> v.reference().asName()).map(Identifier.Variable::of).toSet();
        iterate(disjunction.conjunctions()).filter(c -> !c.isCoherent() && !isSchemaQuery(c, filter)).map(c -> {
            throw GraknException.of(UNSATISFIABLE_CONJUNCTION, c);
        });

        if (mayReason(disjunction, context)) return executeReasoner(disjunction, modifiers, context);
        else return executeTraversal(disjunction, context, filter);
    }

    private FunctionalIterator<ConceptMap> executeReasoner(Disjunction disjunction, GraqlMatch.Modifiers modifiers,
                                                           Context.Query context) {
        ReasonerProducer producer = disjunction.conjunctions().size() == 1
                ? new ReasonerProducer(disjunction.conjunctions().get(0), resolverRegistry, modifiers, context.options())
                : new ReasonerProducer(disjunction, resolverRegistry, modifiers, context.options());
        return produce(producer, context.producer(), asyncPool1());
    }

    private FunctionalIterator<ConceptMap> executeTraversal(Disjunction disjunction, Context.Query context,
                                                            Set<Identifier.Variable.Name> filter) {
        FunctionalIterator<ConceptMap> answers;
        FunctionalIterator<Conjunction> conjs = iterate(disjunction.conjunctions());
        if (!context.options().parallel()) answers = conjs.flatMap(conj -> iterator(conj, filter, context));
        else answers = produce(conjs.map(c -> producer(c, filter, context)).toList(), context.producer(), asyncPool1());
        if (disjunction.conjunctions().size() > 1) answers = answers.distinct();
        return answers;
    }

    private Producer<ConceptMap> producer(Conjunction conjunction, Set<Identifier.Variable.Name> filter,
                                          Context.Query context) {
        if (conjunction.negations().isEmpty()) {
            return traversalEng.producer(
                    conjunction.traversal(filter), context.producer(), PARALLELISATION_FACTOR
            ).map(conceptMgr::conceptMap);
        } else {
            return traversalEng.producer(
                    conjunction.traversal(), context.producer(), PARALLELISATION_FACTOR
            ).map(conceptMgr::conceptMap).filter(answer -> !iterate(conjunction.negations()).flatMap(
                    negation -> iterator(negation.disjunction(), answer)
            ).hasNext()).map(answer -> answer.filter(filter)).distinct();
        }
    }

    private FunctionalIterator<ConceptMap> iterator(Disjunction disjunction, ConceptMap bounds) {
        return iterate(disjunction.conjunctions()).flatMap(c -> iterator(c, bounds));
    }

    private FunctionalIterator<ConceptMap> iterator(Conjunction conjunction, ConceptMap bounds) {
        return iterator(bound(conjunction, bounds), set(), defaultContext);
    }

    private FunctionalIterator<ConceptMap> iterator(Conjunction conjunction, Set<Identifier.Variable.Name> filter,
                                                    Context.Query context) {
        if (!conjunction.isCoherent()) return Iterators.empty();
        if (conjunction.negations().isEmpty()) {
            return traversalEng.iterator(conjunction.traversal(filter)).map(conceptMgr::conceptMap);
        } else {
            return traversalEng.iterator(conjunction.traversal()).map(conceptMgr::conceptMap).filter(
                    ans -> !iterate(conjunction.negations()).flatMap(n -> iterator(n.disjunction(), ans)).hasNext()
            ).map(conceptMap -> conceptMap.filter(filter)).distinct();
        }
    }

    private Conjunction bound(Conjunction conjunction, ConceptMap bounds) {
        Conjunction newClone = conjunction.clone();
        newClone.bound(bounds.toMap(Type::getLabel, Thing::getIID));
        return newClone;
    }

}
