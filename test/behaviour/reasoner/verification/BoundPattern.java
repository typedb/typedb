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

package com.vaticle.typedb.core.test.behaviour.reasoner.verification;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.constraint.common.Predicate;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import com.vaticle.typeql.lang.common.TypeQLToken;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class BoundPattern {
    static class BoundConjunction {
        private final Conjunction conjunction;
        private final Conjunction unboundConjunction;
        private final ConceptMap bounds;

        private BoundConjunction(Conjunction conjunction, Conjunction unboundConjunction, ConceptMap bounds) {
            this.conjunction = conjunction;
            this.unboundConjunction = unboundConjunction;
            this.bounds = bounds;
            assert this.unboundConjunction.retrieves().containsAll(bounds.concepts().keySet());
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
            return iterate(Concludable.create(unboundConjunction)).map(concludable -> BoundConcludable.create(
                    concludable, bounds.filter(concludable.pattern().retrieves()))).toSet();
        }

        public Conjunction conjunction() {
            return conjunction;
        }

        public ConceptMap bounds() {
            return bounds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BoundConjunction that = (BoundConjunction) o;
            return conjunction.equals(that.conjunction) &&
                    unboundConjunction.equals(that.unboundConjunction) &&
                    bounds.equals(that.bounds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conjunction, unboundConjunction, bounds);
        }

        @Override
        public String toString() {
            return "BoundConjunction[" + conjunction + "\n{" + bounds + "}]";
        }
    }

    static class BoundConcludable {
        private final BoundConjunction conjunction;
        private final Concludable unboundConcludable;

        private BoundConcludable(BoundConjunction conjunction, Concludable unboundConcludable) {
            this.conjunction = conjunction;
            this.unboundConcludable = unboundConcludable;
            assert this.unboundConcludable.pattern().retrieves().containsAll(conjunction.bounds().concepts().keySet());
        }

        private static BoundConcludable create(Concludable original, ConceptMap bounds) {
            return new BoundConcludable(BoundConjunction.create(original.pattern(), bounds), original);
        }

        boolean isInferredAnswer() {
            return unboundConcludable.isInferredAnswer(conjunction.bounds());
        }

        Optional<Thing> inferredConcept() {
            if (unboundConcludable.isIsa() && isInferredAnswer()) {
                return Optional.of(conjunction.bounds().get(unboundConcludable.asIsa().isa().owner().id()).asThing());
            } else if (unboundConcludable.isHas() &&
                    conjunction.bounds().get(unboundConcludable.asHas().attribute().id()).asAttribute().isInferred()) {
                return Optional.of(conjunction.bounds().get(unboundConcludable.asHas().attribute().id()).asAttribute());
            } else if (unboundConcludable.isAttribute() && isInferredAnswer()) {
                return Optional.of(
                        conjunction.bounds().get(unboundConcludable.asAttribute().attribute().id()).asAttribute());
            } else if (unboundConcludable.isRelation() && isInferredAnswer()) {
                return Optional.of(
                        conjunction.bounds().get(unboundConcludable.asRelation().relation().owner().id()).asRelation());
            } else if (unboundConcludable.isNegated()) {
                return Optional.empty();
            } else {
                return Optional.empty();
            }
        }

        Optional<Pair<Thing, Attribute>> inferredHas() {
            if (unboundConcludable.isHas() && isInferredAnswer()) {
                Thing owner = conjunction.bounds().get(unboundConcludable.asHas().owner().id()).asThing();
                Attribute attribute = conjunction.bounds()
                        .get(unboundConcludable.asHas().attribute().id()).asAttribute();
                return Optional.of(new Pair<>(owner, attribute));
            }
            return Optional.empty();
        }

        BoundConcludable removeInferredBound() {
            // Remove the bound for the variable that the conclusion may generate
            Set<Variable.Retrievable> nonGenerating = new HashSet<>(unboundConcludable.retrieves());
            nonGenerating.remove(unboundConcludable.generatingVariable().id());
            return BoundConcludable.create(unboundConcludable, conjunction.bounds().filter(nonGenerating));
        }

        public BoundConjunction pattern() {
            return conjunction;
        }

        public Concludable concludable() {
            return unboundConcludable;
        }
    }

    static class BoundConclusion {
        private final BoundConjunction conjunction;
        private final Rule.Conclusion unboundConclusion;
        private final Map<Variable, Concept> bounds;

        private BoundConclusion(BoundConjunction conjunction, Rule.Conclusion unboundConclusion, Map<Variable, Concept> bounds) {
            this.conjunction = conjunction;
            this.unboundConclusion = unboundConclusion;
            this.bounds = bounds;
            assert this.unboundConclusion.pattern().retrieves().containsAll(conjunction.bounds().concepts().keySet());
        }

        public static BoundConclusion create(Rule.Conclusion conclusion, Map<Variable, Concept> conclusionAnswer, @Nullable Attribute assignedValue) {
            return new BoundConclusion(BoundConjunction.create(
                    valueVariableAssignmentToAttributePredicate(conclusion, conclusionAnswer, assignedValue),
                    new ConceptMap(filterRetrievable(conclusionAnswer))
            ), conclusion, conclusionAnswer);
        }

        private static Conjunction valueVariableAssignmentToAttributePredicate(Rule.Conclusion conclusion, Map<Variable, Concept> conclusionAnswer, @Nullable Attribute assignedValue) {
            if (conclusion.isHasWithIsa() && conclusion.asHasWithIsa().value().predicate().isValueVar()) {
                Predicate.ValueVar val = conclusion.asHasWithIsa().value().predicate().asValueVar();
                assert val.predicate().equals(TypeQLToken.Predicate.Equality.EQ);

                Attribute assigned;
                if (assignedValue == null) {
                    assigned = conclusionAnswer.get(conclusion.asHasWithIsa().attribute().id()).asAttribute();
                } else assigned = assignedValue;
                assert assigned != null;

                ThingVariable newAttr = new ThingVariable(conclusion.asHas().attribute().id());
                if (assigned.isBoolean()) {
                    newAttr.predicate(new Predicate.Constant.Boolean(TypeQLToken.Predicate.Equality.EQ, assigned.asBoolean().getValue()));
                } else if (assigned.isDouble()) {
                    newAttr.predicate(new Predicate.Constant.Double(TypeQLToken.Predicate.Equality.EQ, assigned.asDouble().getValue()));
                } else if (assigned.isLong()) {
                    newAttr.predicate(new Predicate.Constant.Long(TypeQLToken.Predicate.Equality.EQ, assigned.asLong().getValue()));
                } else if (assigned.isString()) {
                    newAttr.predicate(new Predicate.Constant.String(TypeQLToken.Predicate.Equality.EQ, assigned.asString().getValue()));
                } else if (assigned.isDateTime()) {
                    newAttr.predicate(new Predicate.Constant.DateTime(TypeQLToken.Predicate.Equality.EQ, assigned.asDateTime().getValue()));
                } else throw TypeDBException.of(ILLEGAL_STATE);

                ThingVariable newOwner = new ThingVariable(conclusion.asHas().owner().id());
                newOwner.has(newAttr);
                return new Conjunction(set(newOwner, newAttr), list());
            } else return conclusion.pattern();
        }

        private static Map<Variable.Retrievable, Concept> filterRetrievable(Map<Variable, Concept> concepts) {
            Map<Variable.Retrievable, Concept> newMap = new HashMap<>();
            concepts.forEach((var, concept) -> {
                if (var.isRetrievable()) newMap.put(var.asRetrievable(), concept);
            });
            return newMap;
        }

        public BoundConclusion removeInferredBound() {
            Set<Variable.Retrievable> nonGenerating = new HashSet<>(unboundConclusion.retrievableIds());
            if (unboundConclusion.generating().isPresent())
                nonGenerating.remove(unboundConclusion.generating().get().id());
            Map<Variable, Concept> nonGeneratingBounds = new HashMap<>();
            bounds().forEach((v, c) -> {
                if (v.isRetrievable() && nonGenerating.contains(v.asRetrievable())) nonGeneratingBounds.put(v, c);
            });
            Attribute assignedValue = (unboundConclusion.isHasWithIsa()) ? bounds().get(unboundConclusion.generating().get().id()).asAttribute() : null;
            return BoundConclusion.create(unboundConclusion, nonGeneratingBounds, assignedValue);
        }

        public BoundConjunction pattern() {
            return conjunction;
        }

        public Rule.Conclusion conclusion() {
            return unboundConclusion;
        }

        public Map<Variable, Concept> bounds() {
            return bounds;
        }

        Optional<Thing> inferredThing() {
            if (unboundConclusion.isIsa()) {
                return Optional.of(conjunction.bounds().get(unboundConclusion.asIsa().isa().owner().id()).asThing());
            } else {
                return Optional.empty();
            }
        }

        public Optional<Pair<Thing, Attribute>> inferredHas() {
            if (unboundConclusion.isHas()) {
                Thing owner = conjunction.bounds().get(unboundConclusion.asHas().owner().id()).asThing();
                Attribute attribute = conjunction.bounds().get(unboundConclusion.asHas().attribute().id()).asAttribute();
                return Optional.of(new Pair<>(owner, attribute));
            } else {
                return Optional.empty();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BoundConclusion that = (BoundConclusion) o;
            return conjunction.equals(that.conjunction) &&
                    unboundConclusion.equals(that.unboundConclusion);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conjunction, unboundConclusion);
        }
    }

    static class BoundCondition {
        private final BoundConjunction conjunction;
        private final Rule.Condition.ConditionBranch unboundConditionBranch;

        private BoundCondition(BoundConjunction conjunction, Rule.Condition.ConditionBranch unboundConditionBranch) {
            this.conjunction = conjunction;
            this.unboundConditionBranch = unboundConditionBranch;
        }

        static BoundCondition create(Rule.Condition.ConditionBranch conditionBranch, ConceptMap conditionAnswer) {
            return new BoundCondition(BoundConjunction.create(conditionBranch.conjunction().pattern(), conditionAnswer), conditionBranch);
        }

        BoundConjunction conjunction() {
            return conjunction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BoundCondition that = (BoundCondition) o;
            return conjunction.equals(that.conjunction) &&
                    unboundConditionBranch.equals(that.unboundConditionBranch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conjunction, unboundConditionBranch);
        }
    }
}
