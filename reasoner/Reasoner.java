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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.Type;
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
import com.vaticle.typeql.lang.pattern.variable.Reference;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_SUB_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.SORT_ATTRIBUTE_NOT_COMPARABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.SORT_VARIABLE_NOT_ATTRIBUTE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
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

    public FunctionalIterator<ConceptMap> execute(Disjunction disjunction, TypeQLMatch.Modifiers modifiers, Context.Query context) {
        inferAndValidateTypes(disjunction);
        FunctionalIterator<ConceptMap> answers;
        if (mayReason(disjunction, context)) answers = executeReasoner(disjunction, filter(modifiers.filter()), context);
        else answers = executeTraversal(disjunction, context, filter(modifiers.filter()));

        if (modifiers.sort().isPresent()) answers = sort(answers, modifiers.sort().get());
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

    private Set<Identifier.Variable.Retrievable> filter(List<UnboundVariable> typeQLVars) {
        Set<Identifier.Variable.Retrievable> names = new HashSet<>();
        iterate(typeQLVars).map(v -> v.reference().asName()).map(Identifier.Variable::of).forEachRemaining(names::add);
        return names;
    }

    private FunctionalIterator<ConceptMap> sort(FunctionalIterator<ConceptMap> answers, Sortable.Sorting sorting) {
        List<Reference.Name> sortVars = iterate(sorting.vars()).map(var -> var.reference().asName()).toList();
        Comparator<List<Attribute>> multiComparator = multiComparator(sortVars.size());
        Comparator<ConceptMap> comparator = (answer1, answer2) -> {
            List<Attribute> attributes1 = new ArrayList<>(sortVars.size());
            List<Attribute> attributes2 = new ArrayList<>(sortVars.size());
            for (Reference.Name var : sortVars) {
                try {
                    Attribute att1 = answer1.get(var).asAttribute();
                    Attribute att2 = answer2.get(var).asAttribute();
                    if (!att1.getType().getValueType().comparables().contains(att2.getType().getValueType())) {
                        throw TypeDBException.of(SORT_ATTRIBUTE_NOT_COMPARABLE, var);
                    }
                    attributes1.add(att1);
                    attributes2.add(att2);
                } catch (TypeDBException e) {
                    if (e.code().isPresent() || e.code().get().equals(INVALID_THING_CASTING.code())) {
                        throw TypeDBException.of(SORT_VARIABLE_NOT_ATTRIBUTE, var);
                    } else {
                        throw e;
                    }
                }
            }
            return multiComparator.compare(attributes1, attributes2);
        };
        comparator = (sorting.order() == TypeQLArg.Order.DESC) ? comparator.reversed() : comparator;
        return iterate(answers.stream().sorted(comparator).iterator());
    }

    private Comparator<List<Attribute>> multiComparator(int n) {
        Optional<Comparator<List<Attribute>>> multiComparator = IntStream.range(0, n)
                .mapToObj(i -> Comparator.comparing((List<Attribute> attrs) -> attrs.get(i), (att1, att2) -> {
                    if (att1.isString()) {
                        return att1.asString().getValue().compareToIgnoreCase(att2.asString().getValue());
                    } else if (att1.isBoolean()) {
                        return att1.asBoolean().getValue().compareTo(att2.asBoolean().getValue());
                    } else if (att1.isLong() && att2.isLong()) {
                        return att1.asLong().getValue().compareTo(att2.asLong().getValue());
                    } else if (att1.isDouble() || att2.isDouble()) {
                        Double double1 = att1.isLong() ? att1.asLong().getValue() : att1.asDouble().getValue();
                        Double double2 = att2.isLong() ? att2.asLong().getValue() : att2.asDouble().getValue();
                        return double1.compareTo(double2);
                    } else if (att1.isDateTime()) {
                        return (att1.asDateTime().getValue()).compareTo(att2.asDateTime().getValue());
                    } else {
                        throw TypeDBException.of(ILLEGAL_STATE);
                    }
                })).reduce(Comparator::thenComparing);
        assert multiComparator.isPresent();
        return multiComparator.get();
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

    private Producer<ConceptMap> producer(Conjunction conjunction, Set<Identifier.Variable.Retrievable> filter) {
        if (conjunction.negations().isEmpty()) {
            return traversalEng.producer(conjunction.traversal(filter), PARALLELISATION_FACTOR)
                    .map(conceptMgr::conceptMap);
        } else {
            return traversalEng.producer(conjunction.traversal(), PARALLELISATION_FACTOR)
                    .map(conceptMgr::conceptMap).filter(answer -> !iterate(conjunction.negations()).flatMap(
                            negation -> iterator(negation.disjunction(), answer)).hasNext()
                    ).map(answer -> answer.filter(filter)).distinct();
        }
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
