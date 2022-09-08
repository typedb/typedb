/*
 * Copyright (C) 2022 Vaticle
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
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.concurrent.producer.Producers;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.TypeQLArg;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.builder.Sortable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_SUB_PATTERN;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.concurrent.executor.Executors.PARALLELISATION_FACTOR;
import static com.vaticle.typedb.core.concurrent.executor.Executors.actor;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async1;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;

public class Reasoner {

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final ControllerRegistry controllerRegistry;
    private final ExplainablesManager explainablesManager;

    public Reasoner(ConceptManager conceptMgr, LogicManager logicMgr,
                    TraversalEngine traversalEng, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.controllerRegistry = new ControllerRegistry(actor(), traversalEng, conceptMgr, logicMgr, context);
        this.explainablesManager = new ExplainablesManager();
    }

    public ControllerRegistry controllerRegistry() {
        return controllerRegistry;
    }

    public FunctionalIterator<? extends ConceptMap> execute(Disjunction disjunction, TypeQLMatch.Modifiers modifiers, Context.Query context) {
        inferAndValidateTypes(disjunction);
        FunctionalIterator<? extends ConceptMap> answers;
        if (mayReason(disjunction, context)) {
            answers = executeReasoner(disjunction, filter(modifiers.filter()), context);
            if (modifiers.sort().isPresent()) answers = eagerSort(answers, modifiers.sort().get());
        } else if (modifiers.sort().isPresent() && isNativelySortable(disjunction, modifiers.sort().get())) {
            answers = executeTraversalSorted(disjunction, context, filter(modifiers.filter()), modifiers.sort().get());
        } else {
            answers = executeTraversal(disjunction, context, filter(modifiers.filter()));
            if (modifiers.sort().isPresent()) answers = eagerSort(answers, modifiers.sort().get());
        }

        if (modifiers.offset().isPresent()) answers = answers.offset(modifiers.offset().get());
        if (modifiers.limit().isPresent()) answers = answers.limit(modifiers.limit().get());
        return answers;
    }

    private void inferAndValidateTypes(Disjunction disjunction) {
        logicMgr.typeInference().applyCombination(disjunction);
        if (!disjunction.isCoherent()) {
            Set<Conjunction> causes = incoherentConjunctions(disjunction);
            if (set(disjunction.conjunctions()).equals(causes)) {
                throw TypeDBException.of(UNSATISFIABLE_PATTERN, disjunction);
            } else {
                throw TypeDBException.of(UNSATISFIABLE_SUB_PATTERN, disjunction, causes);
            }
        }
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

    private boolean mayReason(Disjunction disjunction, Context.Query context) {
        if (!context.options().infer() || context.transactionType().isWrite() || !logicMgr.rules().hasNext()) {
            return false;
        }
        return mayReason(disjunction);
    }

    private boolean mayReason(Disjunction disjunction) {
        for (Conjunction conj : disjunction.conjunctions()) {
            Set<Variable> vars = conj.variables();
            List<Negation> negs = conj.negations();
            if (iterate(vars).flatMap(v -> iterate(v.inferredTypes())).distinct().anyMatch(this::ruleConcludes)) {
                return true;
            }
            if (!negs.isEmpty() && iterate(negs).anyMatch(n -> mayReason(n.disjunction()))) return true;
        }
        return false;
    }

    private boolean ruleConcludes(Label type) {
        return logicMgr.rulesConcluding(type).hasNext() || logicMgr.rulesConcludingHas(type).hasNext();
    }

    private boolean isNativelySortable(Disjunction disjunction, Sortable.Sorting sorting) {
        for (Conjunction conjunction : disjunction.conjunctions()) {
            for (UnboundVariable sortVar : sorting.vars()) {
                assert sortVar.isNamed();
                Variable variable = conjunction.variable(Identifier.Variable.name(sortVar.name()));
                if (variable.isThing() && iterate(variable.inferredTypes()).map(traversalEng.graph().schema()::getType)
                        .anyMatch(type -> type.isAttributeType() && !type.asType().valueType().isNativelySorted())) {
                    return false;
                }
            }
        }
        return true;
    }

    private Set<Identifier.Variable.Retrievable> filter(List<UnboundVariable> typeQLVars) {
        Set<Identifier.Variable.Retrievable> names = new HashSet<>();
        iterate(typeQLVars).map(v -> v.reference().asName()).map(Identifier.Variable::of).forEachRemaining(names::add);
        return names;
    }

    private FunctionalIterator<? extends ConceptMap> eagerSort(FunctionalIterator<? extends ConceptMap> answers, Sortable.Sorting sorting) {
        List<Identifier.Variable.Name> sortVars = iterate(sorting.vars()).map(var -> Identifier.Variable.name(var.name())).toList();
        Comparator<ConceptMap> comparator = ConceptMap.Ordered.Comparator.create(sortVars, sorting.order() == TypeQLArg.Order.ASC);
        return iterate(answers.stream().sorted(comparator).iterator());
    }

    public FunctionalIterator<ConceptMap> executeReasoner(Disjunction disjunction, Set<Identifier.Variable.Retrievable> filter,
                                                          Context.Query context) {
        ReasonerProducer.Match producer = disjunction.conjunctions().size() == 1
                ? new ReasonerProducer.Match.Conjunction(disjunction.conjunctions().get(0), filter, context.options(), controllerRegistry, explainablesManager)
                : new ReasonerProducer.Match.Disjunction(disjunction, filter, context.options(), controllerRegistry, explainablesManager);
        return produce(producer, context.producer(), async1());
    }

    public FunctionalIterator<ConceptMap> executeTraversal(Disjunction disjunction, Context.Query context,
                                                           Set<Identifier.Variable.Retrievable> filter) {
        FunctionalIterator<ConceptMap> answers;
        FunctionalIterator<Conjunction> conjs = iterate(disjunction.conjunctions());
        if (!context.options().parallel()) answers = conjs.flatMap(conj -> iterator(conj, filter));
        else answers = produce(conjs.map(c -> producer(c, filter)).toList(), context.producer(), async1());
        if (disjunction.conjunctions().size() > 1) answers = answers.distinct();
        return answers;
    }

    public FunctionalIterator<ConceptMap.Ordered> executeTraversalSorted(Disjunction disjunction, Context.Query context,
                                                                         Set<Identifier.Variable.Retrievable> filter,
                                                                         Sortable.Sorting sorting) {
        FunctionalIterator<Conjunction> conjs = iterate(disjunction.conjunctions());
        SortedIterator.Order order = sorting.order() == TypeQLArg.Order.ASC ? ASC : SortedIterator.DESC;
        FunctionalIterator<ConceptMap.Ordered> answers = conjs.flatMap(conj -> iteratorSorted(conj, filter, sorting.vars(), order));
        if (disjunction.conjunctions().size() > 1) answers = answers.distinct();
        return answers;
    }

    private Producer<ConceptMap> producer(Conjunction conjunction, Set<Identifier.Variable.Retrievable> filter) {
        if (conjunction.negations().isEmpty()) {
            return traversalEng.producer(conjunction.traversal(filter), PARALLELISATION_FACTOR)
                    .map(conceptMgr::conceptMap);
        } else {
            return traversalEng.producer(conjunction.traversal(), PARALLELISATION_FACTOR)
                    .map(conceptMgr::conceptMap).filter(answer -> !isNegated(answer, conjunction.negations()))
                    .map(answer -> answer.filter(filter)).distinct();
        }
    }

    private boolean isNegated(ConceptMap answer, List<Negation> negations) {
        return iterate(negations).flatMap(n -> iterator(n.disjunction(), answer)).first().isPresent();
    }

    private FunctionalIterator<ConceptMap> iterator(Disjunction disjunction, ConceptMap bounds) {
        return iterate(disjunction.conjunctions()).flatMap(c -> iterator(c, bounds));
    }

    private FunctionalIterator<ConceptMap> iterator(Conjunction conjunction, ConceptMap bounds) {
        return iterator(bound(conjunction, bounds), set());
    }

    private FunctionalIterator<ConceptMap> iterator(Conjunction conjunction,
                                                    Set<Identifier.Variable.Retrievable> filter) {
        if (!conjunction.isCoherent()) return Iterators.empty();
        FunctionalIterator<ConceptMap> answers = traversalEng.iterator(conjunction.traversal(filter)).map(conceptMgr::conceptMap);
        if (conjunction.negations().isEmpty()) return answers;
        else {
            return traversalEng.iterator(conjunction.traversal()).map(conceptMgr::conceptMap)
                    .filter(ans -> !isNegated(ans, conjunction.negations()))
                    .map(conceptMap -> conceptMap.filter(filter)).distinct();
        }
    }

    private <ORDER extends SortedIterator.Order> SortedIterator<ConceptMap.Ordered, ORDER> iteratorSorted(Conjunction conjunction,
                                                                                                          Set<Identifier.Variable.Retrievable> filter,
                                                                                                          List<UnboundVariable> sortVars,
                                                                                                          ORDER order) {
        List<Identifier.Variable.Name> sortIDs = iterate(sortVars).map(v -> Identifier.Variable.name(v.name())).toList();
        ConceptMap.Ordered.Comparator comparator = ConceptMap.Ordered.Comparator.create(sortIDs, order == ASC);
        SortedIterator<ConceptMap.Ordered, ORDER> answers = traversalEng.iterator(conjunction.traversal(filter, sortIDs), order)
                .mapSorted(vertexMap -> conceptMgr.conceptMapOrdered(vertexMap, comparator), order);
        if (conjunction.negations().isEmpty()) return answers;
        else {
            return answers.filter(ans -> !isNegated(ans, conjunction.negations()))
                    .mapSorted(conceptMap -> conceptMap.filter(filter), order).distinct();
        }
    }

    private Conjunction bound(Conjunction conjunction, ConceptMap bounds) {
        Conjunction clone = conjunction.clone();
        Map<Identifier.Variable.Retrievable, Either<Label, ByteArray>> converted = new HashMap<>();
        iterate(bounds.concepts().entrySet()).forEachRemaining(e -> converted.put(
                e.getKey(),
                e.getValue().isType() ? Either.first(e.getValue().asType().getLabel()) : Either.second(e.getValue().asThing().getIID())
        ));
        clone.bound(converted);
        return clone;
    }

    public FunctionalIterator<Explanation> explain(long explainableId, Context.Query defaultContext) {
        Concludable explainableConcludable = explainablesManager.getConcludable(explainableId);
        ConceptMap explainableBounds = explainablesManager.getBounds(explainableId);
        return Producers.produce(
                list(new ReasonerProducer.Explain(explainableConcludable, explainableBounds, defaultContext.options(),
                        controllerRegistry, explainablesManager)),
                Either.first(Arguments.Query.Producer.INCREMENTAL),
                async1()
        );
    }

    public void close() {
        controllerRegistry.close();
    }
}
