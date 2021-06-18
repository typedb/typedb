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

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.concurrent.producer.Producers;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.resolution.ResolverRegistry;
import com.vaticle.typedb.core.reasoner.resolution.answer.Explanation;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.actor;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;

public class Reasoner {

    private static final Logger LOG = LoggerFactory.getLogger(Reasoner.class);

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final ResolverRegistry resolverRegistry;
    private final ExplainablesManager explainablesManager;
    private final Context.Query defaultContext;

    public Reasoner(ConceptManager conceptMgr, LogicManager logicMgr,
                    TraversalEngine traversalEng, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.defaultContext = new Context.Query(context, new Options.Query());
        this.defaultContext.producer(Either.first(EXHAUSTIVE));
        this.resolverRegistry = new ResolverRegistry(actor(), traversalEng, conceptMgr, logicMgr, defaultContext.options().traceInference());
        this.explainablesManager = new ExplainablesManager();
    }

    public ResolverRegistry resolverRegistry() {
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
            if (iterate(vars).flatMap(v -> iterate(v.resolvedTypes())).distinct().anyMatch(this::hasRule)) return true;
            if (!negs.isEmpty() && iterate(negs).anyMatch(n -> mayReason(n.disjunction()))) return true;
        }
        return false;
    }

    private boolean hasRule(Label type) {
        return logicMgr.rulesConcluding(type).hasNext() || logicMgr.rulesConcludingHas(type).hasNext();
    }

    public FunctionalIterator<ConceptMap> execute(Disjunction disjunction, TypeQLMatch.Modifiers modifiers, Context.Query context) {
        logicMgr.typeResolver().resolve(disjunction);

        if (!disjunction.isCoherent()) {
            Set<Conjunction> causes = incoherentConjunctions(disjunction);
            throw TypeDBException.of(UNSATISFIABLE_PATTERN, disjunction, causes);
        }

        if (mayReason(disjunction, context)) return executeReasoner(disjunction, modifiers, context);
        else return executeTraversal(disjunction, context, filter(modifiers.filter()));
    }

    private Set<Identifier.Variable.Name> filter(List<UnboundVariable> typeQLVars) {
        return iterate(typeQLVars).map(v -> v.reference().asName()).map(Identifier.Variable::of).toSet();
    }

    private Set<Conjunction> incoherentConjunctions(Disjunction disjunction) {
        assert !disjunction.isCoherent();
        Set<Conjunction> causes = new HashSet<>();
        for (Conjunction conj : disjunction.conjunctions()) {
            // TODO: this logic can be more complete if it did not only assume the nested negation is to blame
            FunctionalIterator<Negation> incoherentNegs = iterate(conj.negations()).filter(n -> !n.isCoherent());
            if (!conj.isCoherent() && !incoherentNegs.hasNext()) causes.add(conj);
            else incoherentNegs.forEachRemaining(n -> causes.addAll(incoherentConjunctions(n.disjunction())));
        }
        return causes;
    }

    private FunctionalIterator<ConceptMap> executeReasoner(Disjunction disjunction, TypeQLMatch.Modifiers modifiers,
                                                           Context.Query context) {
        ReasonerProducer producer = disjunction.conjunctions().size() == 1
                ? new ReasonerProducer(disjunction.conjunctions().get(0), modifiers, context.options(), resolverRegistry, explainablesManager)
                : new ReasonerProducer(disjunction, modifiers, context.options(), resolverRegistry, explainablesManager);
        return produce(producer, context.producer(), async1());
    }

    private FunctionalIterator<ConceptMap> executeTraversal(Disjunction disjunction, Context.Query context,
                                                            Set<Identifier.Variable.Name> filter) {
        FunctionalIterator<ConceptMap> answers;
        FunctionalIterator<Conjunction> conjs = iterate(disjunction.conjunctions());
        if (!context.options().parallel()) answers = conjs.flatMap(conj -> iterator(conj, filter, context));
        else answers = produce(conjs.map(c -> producer(c, filter, context)).toList(), context.producer(), async1());
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

    public FunctionalIterator<Explanation> explain(long explainableId, Context.Query defaultContext) {
        Conjunction explainableConjunction = explainablesManager.getConjunction(explainableId);
        ConceptMap explainableBounds = explainablesManager.getBounds(explainableId);
        return Producers.produce(
                list(new ExplanationProducer(explainableConjunction, explainableBounds, defaultContext.options(), resolverRegistry, explainablesManager)),
                Either.first(Arguments.Query.Producer.INCREMENTAL),
                async1()
        );
    }
}
