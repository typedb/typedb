/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.concurrent.producer.Producers;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.reasoner.common.ReasonerPerfCounters;
import com.vaticle.typedb.core.reasoner.controller.ControllerRegistry;
import com.vaticle.typedb.core.reasoner.planner.ReasonerPlanner;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers.Filter;
import com.vaticle.typedb.core.traversal.common.Modifiers.Sorting;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INFERENCE_INCOHERENT_MATCH_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INFERENCE_INCOHERENT_MATCH_SUB_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.SORT_ATTRIBUTE_NOT_COMPARABLE;
import static com.vaticle.typedb.core.common.iterator.Iterators.cartesian;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;
import static com.vaticle.typedb.core.common.parameters.Order.Asc.ASC;
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
    private final ReasonerPlanner planner;
    private final ReasonerPerfCounters perfCounters;

    public Reasoner(ConceptManager conceptMgr, LogicManager logicMgr,
                    TraversalEngine traversalEng, Context.Transaction context) {
        this.conceptMgr = conceptMgr;
        this.traversalEng = traversalEng;
        this.logicMgr = logicMgr;
        this.perfCounters = new ReasonerPerfCounters(context.options().infer() && context.options().reasonerPerfCounters());
        this.planner = ReasonerPlanner.create(traversalEng, conceptMgr, logicMgr, perfCounters, context.options().explain());
        this.controllerRegistry = new ControllerRegistry(actor(), traversalEng, conceptMgr, logicMgr, planner, perfCounters, context);
        this.explainablesManager = new ExplainablesManager();
    }

    public ControllerRegistry controllerRegistry() {
        return controllerRegistry;
    }

    public FunctionalIterator<? extends ConceptMap> execute(Disjunction disjunction, List<Identifier.Variable.Name> filterVars,
                                                            TypeQLQuery.Modifiers modifiers, Context.Query context,
                                                            ConceptMap bindings) {
        Disjunction boundDisjunction;
        if (!bindings.concepts().isEmpty()) {
            boundDisjunction = new Disjunction(iterate(disjunction.conjunctions()).map(c -> bound(c, bindings)).toList());
        } else boundDisjunction = disjunction;
        inferAndValidateTypes(boundDisjunction);
        Filter filter = filterVars.isEmpty() ? Filter.create(boundDisjunction.returnedVariables()) : Filter.create(filterVars);
        Optional<Sorting> sorting = modifiers.sort().map(Sorting::create);
        sorting.ifPresent(value -> validateSorting(boundDisjunction, value));
        Disjunction answerableDisjunction = filterUnanswerable(boundDisjunction);
        FunctionalIterator<? extends ConceptMap> answers;
        if (answerableDisjunction.conjunctions().isEmpty()) return empty();
        else if (mayReason(answerableDisjunction, context)) {
            answers = executeReasoner(answerableDisjunction, filter, context);
            if (sorting.isPresent()) answers = eagerSort(answers, sorting.get());
        } else if (sorting.isPresent() && isNativelySortable(answerableDisjunction, sorting.get())) {
            answers = executeTraversalSorted(answerableDisjunction, filter, sorting.get());
        } else {
            if (sorting.isPresent()) {
                answers = executeTraversal(answerableDisjunction, context.producer(Either.first(EXHAUSTIVE)), filter);
                answers = eagerSort(answers, sorting.get());
            } else if (modifiers.limit().isPresent()) {
                answers = executeTraversal(answerableDisjunction, context.producer(Either.second(modifiers.offset().orElse(0L) + modifiers.limit().get())), filter);
            } else {
                answers = executeTraversal(answerableDisjunction, context.producer(Either.first(INCREMENTAL)), filter);
            }
        }

        if (modifiers.offset().isPresent()) answers = answers.offset(modifiers.offset().get());
        if (modifiers.limit().isPresent()) answers = answers.limit(modifiers.limit().get());
        return answers;
    }

    private Disjunction filterUnanswerable(Disjunction disjunction) {
        return new Disjunction(iterate(disjunction.conjunctions()).filter(Conjunction::isAnswerable).toList());
    }

    private void inferAndValidateTypes(Disjunction disjunction) {
        logicMgr.typeInference().applyCombination(disjunction);
        logicMgr.expressionResolver().resolveExpressions(disjunction);
        if (!disjunction.isCoherent()) {
            Set<Conjunction> causes = incoherentConjunctions(disjunction);
            if (set(disjunction.conjunctions()).equals(causes)) {
                throw TypeDBException.of(INFERENCE_INCOHERENT_MATCH_PATTERN, disjunction);
            } else {
                throw TypeDBException.of(INFERENCE_INCOHERENT_MATCH_SUB_PATTERN, disjunction, causes);
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

    private void validateSorting(Disjunction disjunction, Sorting sorting) {
        Map<Identifier.Variable.Retrievable, HashSet<AttributeType>> sortAttrTypes = new HashMap<>();
        sorting.variables().forEach(id -> disjunction.conjunctions().forEach(conjunction -> {
            Variable variable = conjunction.variable(id);
            if (!variable.isValue()) { // value variables never have ambiguous value types
                HashSet<AttributeType> types = sortAttrTypes.computeIfAbsent(id, (i) -> new HashSet<>());
                variable.inferredTypes().forEach(label -> {
                    ThingType type = conceptMgr.getThingType(label.name());
                    if (type != null && type.isAttributeType()) types.add(type.asAttributeType());
                });
            }
        }));
        sortAttrTypes.forEach((var, attrTypes) -> {
            Optional<List<AttributeType>> incomparable = cartesian(list(iterate(attrTypes), iterate(attrTypes)))
                    .filter(list -> !list.get(0).getValueType().comparables().contains(list.get(1).getValueType())).first();
            if (incomparable.isPresent()) {
                throw TypeDBException.of(SORT_ATTRIBUTE_NOT_COMPARABLE, var, incomparable.get().get(0).getLabel(), incomparable.get().get(1).getLabel());
            }
        });
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
            if (iterate(vars).filter(v -> !v.isValue()).flatMap(v -> iterate(v.inferredTypes())).anyMatch(this::ruleConcludes)) {
                return true;
            }
            if (!negs.isEmpty() && iterate(negs).anyMatch(n -> mayReason(n.disjunction()))) return true;
        }
        return false;
    }

    private boolean ruleConcludes(Label type) {
        return logicMgr.rulesConcluding(type).hasNext() || logicMgr.rulesConcludingHas(type).hasNext();
    }

    private boolean isNativelySortable(Disjunction disjunction, Sorting sorting) {
        for (Conjunction conjunction : disjunction.conjunctions()) {
            for (Identifier.Variable.Retrievable id : sorting.variables()) {
                Variable variable = conjunction.variable(id);
                if (variable.isValue()) return false;
                if (variable.isThing() && iterate(variable.inferredTypes()).map(traversalEng.graph().schema()::getType)
                        .anyMatch(type -> type.isAttributeType() && !type.asType().valueType().isSorted())) {
                    return false;
                }
            }
        }
        return true;
    }

    private FunctionalIterator<? extends ConceptMap> eagerSort(FunctionalIterator<? extends ConceptMap> answers, Sorting sorting) {
        Comparator<ConceptMap> comparator = ConceptMap.Comparator.create(sorting);
        return iterate(answers.stream().sorted(comparator).iterator());
    }

    public FunctionalIterator<ConceptMap> executeReasoner(Disjunction disjunction, Filter filter, Context.Query context) {
        ReasonerProducer.Match producer = disjunction.conjunctions().size() == 1
                ? new ReasonerProducer.Match.Conjunction(disjunction.conjunctions().get(0), filter, context.options(), controllerRegistry, explainablesManager)
                : new ReasonerProducer.Match.Disjunction(disjunction, filter, context.options(), controllerRegistry, explainablesManager);
        return produce(producer, context.producer(), async1());
    }

    public FunctionalIterator<ConceptMap> executeTraversal(Disjunction disjunction, Context.Query context, Filter filter) {
        FunctionalIterator<ConceptMap> answers;
        FunctionalIterator<Conjunction> conjs = iterate(disjunction.conjunctions());
        if (!context.options().parallel()) answers = conjs.flatMap(conj -> iterator(conj, filter));
        else answers = produce(conjs.map(c -> producer(c, filter)).toList(), context.producer(), async1());
        if (disjunction.conjunctions().size() > 1) answers = answers.distinct();
        return answers;
    }

    public SortedIterator<ConceptMap.Sortable, Order.Asc> executeTraversalSorted(Disjunction disjunction, Filter filter,
                                                                                 Sorting sorting) {
        // TODO: parallelised sorted queries
        FunctionalIterator<Conjunction> conjs = iterate(disjunction.conjunctions());
        SortedIterator<ConceptMap.Sortable, Order.Asc> answers = conjs.mergeMap(conj -> iteratorSorted(conj, filter, sorting), ASC);
        if (disjunction.conjunctions().size() > 1) answers = answers.distinct();
        return answers;
    }

    private Producer<ConceptMap> producer(Conjunction conjunction, Filter filter) {
        assert conjunction.isCoherent();
        if (!conjunction.isAnswerable()) return Producers.empty();
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
        return iterator(bound(conjunction, bounds), Filter.create(conjunction.retrieves()));
    }

    private FunctionalIterator<ConceptMap> iterator(Conjunction conjunction, Filter filter) {
        assert conjunction.isCoherent();
        if (!conjunction.isAnswerable()) return empty();
        if (conjunction.negations().isEmpty()) {
            return traversalEng.iterator(conjunction.traversal(filter)).map(conceptMgr::conceptMap);
        } else {
            return traversalEng.iterator(conjunction.traversal()).map(conceptMgr::conceptMap)
                    .filter(ans -> !isNegated(ans, conjunction.negations()))
                    .map(conceptMap -> conceptMap.filter(filter)).distinct();
        }
    }

    private SortedIterator<ConceptMap.Sortable, Order.Asc> iteratorSorted(Conjunction conjunction,
                                                                          Filter filter, Sorting sorting) {
        ConceptMap.Sortable.Comparator comparator = ConceptMap.Comparator.create(sorting);
        if (conjunction.negations().isEmpty()) {
            return traversalEng.iterator(conjunction.traversal(filter, sorting))
                    .mapSorted(vertexMap -> conceptMgr.conceptMapOrdered(vertexMap, comparator), ASC);
        } else {
            return traversalEng.iterator(conjunction.traversal(Filter.create(list()), sorting))
                    .mapSorted(vertexMap -> conceptMgr.conceptMapOrdered(vertexMap, comparator), ASC)
                    .filter(ans -> !isNegated(ans, conjunction.negations()))
                    .mapSorted(conceptMap -> conceptMap.filter(filter), ASC).distinct();
        }
    }

    private Conjunction bound(Conjunction conjunction, ConceptMap bounds) {
        Conjunction clone = conjunction.clone();
        Map<Identifier.Variable.Retrievable, Either<Label, ByteArray>> converted = new HashMap<>();
        iterate(bounds.concepts().entrySet()).forEachRemaining(e -> {
            Either<Label, ByteArray> value;
            if (e.getValue().isType()) value = Either.first(e.getValue().asType().getLabel());
            else if (e.getValue().isThing()) value = Either.second(e.getValue().asThing().getIID());
            else if (e.getValue().isValue()) value = Either.second(e.getValue().asValue().getIID());
            else throw TypeDBException.of(ILLEGAL_STATE);
            converted.put(e.getKey(), value);
        });
        clone.bound(converted);
        converted.keySet().forEach(boundId -> {
            Variable var = clone.variable(boundId);
            if (var != null && var.isValue()) logicMgr.expressionResolver().resolveAssignment(var.asValue());
        });
        return clone;
    }

    public FunctionalIterator<Explanation> explain(long explainableId, Context.Query defaultContext) {
        Concludable explainableConcludable = explainablesManager.getConcludable(explainableId);
        ConceptMap explainableBounds = explainablesManager.getBounds(explainableId);
        return produce(
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
