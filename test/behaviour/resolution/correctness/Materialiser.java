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

package com.vaticle.typedb.core.test.behaviour.resolution.correctness;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.resolution.correctness.BoundPattern.BoundConcludable;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Collections.singletonList;

public class Materialiser {

    private final Map<com.vaticle.typedb.core.logic.Rule, Rule> rules;
    private final RocksTransaction tx;
    private final Materialisations materialisations;

    private Materialiser(RocksSession session) {
        this.rules = new HashMap<>();
        this.tx = session.transaction(Arguments.Transaction.Type.WRITE, new Options.Transaction().infer(false));
        this.materialisations = new Materialisations();
    }

    public static Materialiser materialise(RocksSession session) {
        Materialiser materialiser = new Materialiser(session);
        materialiser.materialise();
        return materialiser;
    }

    private void materialise() {
        tx.logic().rules().forEachRemaining(rule -> this.rules.put(rule, new Rule(rule)));
        boolean reiterateRules = true;
        while (reiterateRules) {
            reiterateRules = false;
            for (Rule rule : this.rules.values()) {
                reiterateRules = reiterateRules || rule.materialise();
            }
        }
    }

    private FunctionalIterator<ConceptMap> traverse(Conjunction conjunction) {
        return tx.reasoner().executeTraversal(
                new Disjunction(singletonList(conjunction)),
                new Context.Query(tx.context(), new Options.Query()),
                iterate(conjunction.identifiers())
                        .filter(Identifier::isRetrievable)
                        .map(Variable::asRetrievable).toSet()
        );
    }

    void close() {
        tx.close();
    }

    public Map<Conjunction, FunctionalIterator<ConceptMap>> query(TypeQLMatch inferenceQuery) {
        // TODO: How do we handle disjunctions inside negations and negations in general?
        Disjunction disjunction = Disjunction.create(inferenceQuery.conjunction().normalise());
        tx.logic().typeResolver().resolve(disjunction);
        HashMap<Conjunction, FunctionalIterator<ConceptMap>> conjunctionAnswers = new HashMap<>();
        disjunction.conjunctions().forEach(conjunction -> conjunctionAnswers.put(conjunction, traverse(conjunction)));
        return conjunctionAnswers;
    }

    FunctionalIterator<Materialisation> concludableMaterialisations(BoundConcludable boundConcludable) {
        return materialisations.forConcludable(boundConcludable);
    }

    Optional<Materialisation> conditionMaterialisation(com.vaticle.typedb.core.logic.Rule rule, ConceptMap conditionAnswer) {
        if (!rules.containsKey(rule)) return Optional.empty();
        if (!rules.get(rule).conditionAnsMaterialisations.containsKey(conditionAnswer)) return Optional.empty();
        return materialisations.forCondition(rules.get(rule), conditionAnswer);
    }

    private class Rule {
        private final com.vaticle.typedb.core.logic.Rule rule;
        private final Concludable thenConcludable;
        private boolean requiresReiteration;
        private final Map<ConceptMap, Materialisation> conditionAnsMaterialisations;

        private Rule(com.vaticle.typedb.core.logic.Rule typeDBRule) {
            this.rule = typeDBRule;
            this.requiresReiteration = false;
            this.conditionAnsMaterialisations = new HashMap<>();

            Set<Concludable> concludables = Concludable.create(this.rule.then());
            assert concludables.size() == 1;
            // Use a concludable for the conclusion for the convenience of its isInferredAnswer method
            this.thenConcludable = iterate(concludables).next();
        }

        private boolean materialise() {
            // Get all the places where the rule condition is satisfied and materialise for each
            requiresReiteration = false;
            traverse(rule.when())
                    .forEachRemaining(conditionAns -> rule.conclusion()
                            .materialise(conditionAns, tx.traversal(), tx.concepts())
                            .map(conclusionAns -> new ConceptMap(filterRetrievable(conclusionAns)))
                            .filter(thenConcludable::isInferredAnswer)
                            .forEachRemaining(ans -> record(conditionAns, ans)));
            return requiresReiteration;
        }

        private void record(ConceptMap conditionAns, ConceptMap conclusionAns) {
            if (!conditionAnsMaterialisations.containsKey(conditionAns)) {
                Materialisation materialisation = new Materialisation(rule, conditionAns, conclusionAns);
                requiresReiteration = true;
                conditionAnsMaterialisations.put(conditionAns, materialisation);
                if (rule.conclusion().isIsa()) {
                    Concept owner = conclusionAns.get(rule.conclusion().asIsa().isa().owner().id());
                    materialisations.recordConcept(owner, materialisation);
                }
                if (rule.conclusion().isHas()) {
                    Thing owner = conclusionAns.get(rule.conclusion().asHas().has().owner().id()).asThing();
                    Attribute attribute =
                            conclusionAns.get(rule.conclusion().asHas().has().attribute().id()).asAttribute();
                    Pair<Thing, Attribute> has = new Pair<>(owner, attribute);
                    materialisations.recordHas(has, materialisation);
                }
            } else {
                assert conditionAnsMaterialisations.get(conditionAns).conclusionAnswer().equals(conclusionAns);
            }
        }

        private Map<Variable.Retrievable, Concept> filterRetrievable(Map<Variable, Concept> concepts) {
            Map<Variable.Retrievable, Concept> newMap = new HashMap<>();
            concepts.forEach((var, concept) -> {
                if (var.isRetrievable()) newMap.put(var.asRetrievable(), concept);
            });
            return newMap;
        }

    }

    private class Materialisations {
        private final Map<Concept, Set<Materialisation>> concept;
        private final Map<Pair<Thing, Attribute>, Set<Materialisation>> has;

        Materialisations() {
            has = new HashMap<>();
            concept = new HashMap<>();
        }

        FunctionalIterator<Materialisation> forConcludable(BoundConcludable boundConcludable) {
            FunctionalIterator<Materialisation> materialisations = empty();
            Optional<Concept> inferredConcept = boundConcludable.inferredConcept();
            if (inferredConcept.isPresent()) {
                materialisations = materialisations.link(forConcept(inferredConcept.get()));
            }
            Optional<Pair<Thing, Attribute>> inferredHas = boundConcludable.inferredHas();
            if (inferredHas.isPresent()) {
                materialisations = materialisations.link(forHas(inferredHas.get().first(), inferredHas.get().second()));
            }
            return materialisations;
        }

        private FunctionalIterator<Materialisation> forConcept(Concept toFetch) {
            assert concept.containsKey(toFetch);
            return iterate(concept.get(toFetch));
        }

        private FunctionalIterator<Materialisation> forHas(Thing owner, Attribute attribute) {
            Pair<Thing, Attribute> toFetch = new Pair<>(owner, attribute);
            assert has.containsKey(toFetch);
            return iterate(has.get(toFetch));
        }

        Optional<Materialisation> forCondition(Rule ruleRecorder, ConceptMap conditionAnswer) {
            if (!ruleRecorder.conditionAnsMaterialisations.containsKey(conditionAnswer)) return Optional.empty();
            return Optional.of(ruleRecorder.conditionAnsMaterialisations.get(conditionAnswer));
        }

        public void recordConcept(Concept owner, Materialisation materialisation) {
            concept.putIfAbsent(owner, new HashSet<>());
            concept.get(owner).add(materialisation);
        }

        public void recordHas(Pair<Thing, Attribute> hasEdge, Materialisation materialisation) {
            has.putIfAbsent(hasEdge, new HashSet<>());
            has.get(hasEdge).add(materialisation);
        }

    }

    static class Materialisation {

        private final com.vaticle.typedb.core.logic.Rule rule;
        private final ConceptMap conditionAnswer;
        private final ConceptMap conclusionAnswer;

        Materialisation(com.vaticle.typedb.core.logic.Rule rule, ConceptMap conditionAnswer, ConceptMap conclusionAnswer) {
            this.rule = rule;
            this.conditionAnswer = conditionAnswer;
            this.conclusionAnswer = conclusionAnswer;
        }

        ConceptMap conditionAnswer() {
            return conditionAnswer;
        }

        ConceptMap conclusionAnswer() {
            return conclusionAnswer;
        }

        com.vaticle.typedb.core.logic.Rule rule() {
            return rule;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Materialisation that = (Materialisation) o;
            return rule.equals(that.rule) &&
                    conditionAnswer.equals(that.conditionAnswer) &&
                    conclusionAnswer.equals(that.conclusionAnswer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rule, conditionAnswer, conclusionAnswer);
        }
    }

}
