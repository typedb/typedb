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
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class BoundPattern {
    static class BoundConjunction {
        private final Conjunction fullyBound;
        private final Conjunction conjunction;
        private final ConceptMap bounds;

        private BoundConjunction(Conjunction fullyBound, Conjunction conjunction, ConceptMap bounds) {
            this.fullyBound = fullyBound;
            this.conjunction = conjunction;
            this.bounds = bounds;
            assert this.conjunction.retrieves().containsAll(bounds.concepts().keySet());
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
            return iterate(Concludable.create(conjunction)).map(concludable -> BoundConcludable.create(
                    concludable, bounds.filter(concludable.pattern().retrieves()))).toSet();
        }

        public Conjunction fullyBound() {
            return fullyBound;
        }

        public ConceptMap bounds() {
            return bounds;
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
            assert this.concludable.pattern().retrieves().containsAll(bounds.concepts().keySet());
        }

        private static BoundConcludable create(Concludable original, ConceptMap bounds) {
            return new BoundConcludable(BoundConjunction.create(original.pattern(), bounds), original, bounds);
        }

        boolean isInferredAnswer() {
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

        BoundConcludable removeInferredBound() {
            // Remove the bound for the variable that the conclusion may generate
            Set<Identifier.Variable.Retrievable> nonGenerating = new HashSet<>(concludable.retrieves());
            if (concludable.generating().isPresent()) nonGenerating.remove(concludable.generating().get().id());
            return BoundConcludable.create(concludable, bounds.filter(nonGenerating));
        }

        public BoundConjunction fullyBound() {
            return fullyBound;
        }

        public Concludable concludable() {
            return concludable;
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
            assert this.conclusion.conjunction().retrieves().containsAll(bounds.concepts().keySet());
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

        public BoundConjunction fullyBound() {
            return fullyBound;
        }

        public Rule.Conclusion conclusion() {
            return conclusion;
        }
    }
}
