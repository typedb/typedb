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
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.resolution.correctness.CorrectnessChecker.CompletenessException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

class CompletenessChecker {

    private final Materialiser materialiser;
    private final RocksSession session;
    private final Set<Pair<Conjunction, ConceptMap>> checked;

    private CompletenessChecker(Materialiser materialiser, RocksSession session) {
        this.materialiser = materialiser;
        this.session = session;
        this.checked = new HashSet<>();
    }

    static CompletenessChecker create(Materialiser materialiser, RocksSession session) {
        return new CompletenessChecker(materialiser, session);
    }

    void checkQuery(TypeQLMatch inferenceQuery) {
        materialiser.query(inferenceQuery).forEach((conjunction, answers) -> {
            answers.forEachRemaining(answer -> checkConjunction(BoundConjunction.create(conjunction, answer)));
        });
    }

    private void checkConjunction(BoundConjunction boundConjunction) {
        iterate(boundConjunction.boundConcludables()).forEachRemaining(boundConcludable -> {
            if (boundConcludable.isInferredAnswer()) {
                materialiser.concludableMaterialisations(boundConcludable).forEachRemaining(materialisation -> {
                    Pair<Conjunction, ConceptMap> check = new Pair<>(materialisation.rule().when(),
                                                                     materialisation.conditionAnswer());
                    // Materialisations record all possible paths that could be taken to infer a fact, so can contain
                    // cycles. When we detect we have explored the same state before, stop.
                    if (checked.contains(check)) return;
                    else checked.add(check);
                    checkConjunction(BoundConjunction.create(materialisation.rule().when(), materialisation.conditionAnswer()));
                    verifyConclusionReasoning(BoundConclusion.create(materialisation.rule().conclusion(), materialisation.conclusionAnswer()));
                });
                verifyConcludableReasoning(boundConcludable);
            }
        });
    }

    private void verifyConcludableReasoning(BoundConcludable boundConcludable) {
        BoundConcludable boundNonInferred = boundConcludable.removeInferredBound();
        validateNonInferred(boundNonInferred.fullyBound);
        validateNumConcludableAnswers(boundNonInferred.fullyBound, boundConcludable.concludable);
    }

    private void verifyConclusionReasoning(BoundConclusion boundConclusion) {
        BoundConclusion boundNonInferred = boundConclusion.removeInferredBound();
        validateNonInferred(boundNonInferred.fullyBound);
        validateNumConclusionAnswers(boundNonInferred.fullyBound, boundConclusion.conclusion);
    }

    private void validateNonInferred(BoundConjunction boundConjunction) {
        for (Concept concept : boundConjunction.bounds.concepts().values()) {
            if (concept.isThing() && concept.asThing().isInferred()) {
                throw new UnsupportedOperationException(
                        String.format("Completeness testing does not yet support more than one inferred concept " +
                                              "in a query tested against the reasoner. It becomes too " +
                                              "computationally expensive to verify the history of all inferences." +
                                              " Encountered when querying:\n%s", boundConjunction.fullyBound));
            }
        }
    }

    private void validateNumConcludableAnswers(BoundConjunction boundConjunction, Concludable concludable) {
        if (numReasonedAnswers(boundConjunction, concludable.retrieves()) == 0) {
            throw new CompletenessException(String.format("Completeness testing found a missing answer.\nExpected " +
                                                                  "one or more answers for the concludable (bound " +
                                                                  "with IIDs):\n%s\noriginal:\n%s",
                                                          boundConjunction.toString(),
                                                          concludable.pattern().toString()));
        }
    }

    private void validateNumConclusionAnswers(BoundConjunction boundConjunction, Rule.Conclusion conclusion) {
        int numAnswers = numReasonedAnswers(boundConjunction, conclusion.retrievableIds());
        if (numAnswers == 0) {
            throw new CompletenessException(String.format("Completeness testing found a missing answer.\nExpected " +
                                                                  "exactly one answer for the rule conclusion (bound " +
                                                                  "with IIDs):\n%s\n for rule \"%s\"",
                                                          boundConjunction.toString(), conclusion.rule().getLabel()));
        } else if (numAnswers > 1) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private int numReasonedAnswers(BoundConjunction boundConjunction, Set<Identifier.Variable.Retrievable> filter) {
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                       new Options.Transaction().infer(true))) {
            return tx.reasoner().executeReasoner(
                    new Disjunction(Collections.singletonList(boundConjunction.fullyBound)),
                    filter, new Context.Query(tx.context(), new Options.Query())).toList().size();
        }
    }

    static class BoundConjunction {
        private final Conjunction fullyBound;
        private final Conjunction original;
        private final ConceptMap bounds;

        private BoundConjunction(Conjunction fullyBound, Conjunction original, ConceptMap bounds) {
            this.fullyBound = fullyBound;
            this.original = original;
            this.bounds = bounds;
        }

        static BoundConjunction create(Conjunction conjunction, ConceptMap bounds) {
            Conjunction constrainedByIIDs = conjunction.clone();
            bounds.concepts().forEach((identifier, concept) -> {
                if (concept.isThing()) {
                    assert constrainedByIIDs.variable(identifier).isThing();
                    // Make sure this isn't the generating variable, we want to leave that unconstrained.
                    constrainedByIIDs.variable(identifier).asThing().iid(concept.asThing().getIID());
                }
            });
            return new BoundConjunction(constrainedByIIDs, conjunction, bounds);
        }

        Set<BoundConcludable> boundConcludables() {
            return iterate(Concludable.create(original)).map(concludable -> BoundConcludable.create(
                    concludable, bounds.filter(concludable.pattern().retrieves()))).toSet();
        }
    }

    static class BoundConcludable {
        private final BoundConjunction fullyBound;
        final Concludable concludable;
        final ConceptMap bounds;

        private BoundConcludable(BoundConjunction fullyBound, Concludable concludable, ConceptMap bounds) {
            this.fullyBound = fullyBound;
            this.concludable = concludable;
            this.bounds = bounds;
        }

        private static BoundConcludable create(Concludable original, ConceptMap bounds) {
            return new BoundConcludable(BoundConjunction.create(original.pattern(), bounds), original, bounds);
        }

        private boolean isInferredAnswer() {
            return concludable.isInferredAnswer(bounds);
        }

        Optional<Concept> inferredConcept() {
            if (concludable.isIsa() && isInferredAnswer()) {
                return Optional.of(bounds.get(concludable.asIsa().isa().owner().id()));
            } else if (concludable.isHas() && bounds.get(concludable.asHas().attribute().id()).asAttribute().isInferred()) {
                return Optional.of(bounds.get(concludable.asHas().attribute().id()).asAttribute());
            } else if (concludable.isAttribute() && isInferredAnswer()) {
                return Optional.of(bounds.get(concludable.asAttribute().attribute().id()).asAttribute());
            } else if (concludable.isRelation() && isInferredAnswer()) {
                return Optional.of(bounds.get(concludable.asRelation().relation().owner().id()).asRelation());
            } else if (concludable.isNegated()) {
                return Optional.empty();
            } else {
                return Optional.empty();
            }
        }

        Optional<Pair<Thing, Attribute>> inferredHas() {
            if (concludable.isHas() && isInferredAnswer()) {
                Thing owner = bounds.get(concludable.asHas().owner().id()).asThing();
                Attribute attribute = bounds.get(concludable.asHas().attribute().id()).asAttribute();
                return Optional.of(new Pair<>(owner, attribute));
            }
            return Optional.empty();
        }

        private BoundConcludable removeInferredBound() {
            // Remove the bound for the variable that the conclusion may generate
            Set<Identifier.Variable.Retrievable> nonGenerating = new HashSet<>(concludable.retrieves());
            if (concludable.generating().isPresent()) nonGenerating.remove(concludable.generating().get().id());
            return BoundConcludable.create(concludable, bounds.filter(nonGenerating));
        }

    }

    static class BoundConclusion {
        private final BoundConjunction fullyBound;
        private final Rule.Conclusion conclusion;
        private final ConceptMap bounds;

        BoundConclusion(BoundConjunction fullyBound, Rule.Conclusion conclusion, ConceptMap bounds) {
            this.fullyBound = fullyBound;
            this.conclusion = conclusion;
            this.bounds = bounds;
        }

        public static BoundConclusion create(Rule.Conclusion conclusion, ConceptMap conclusionAnswer) {
            return new BoundConclusion(BoundConjunction.create(conclusion.conjunction(), conclusionAnswer),
                                       conclusion, conclusionAnswer);
        }

        public BoundConclusion removeInferredBound() {
            Set<Identifier.Variable.Retrievable> nonGenerating = new HashSet<>(conclusion.retrievableIds());
            if (conclusion.generating().isPresent()) nonGenerating.remove(conclusion.generating().get().id());
            return BoundConclusion.create(conclusion, bounds.filter(nonGenerating));
        }
    }

}
