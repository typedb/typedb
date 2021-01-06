/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.logic;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.tool.ConstraintCopier;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.SystemReference;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.Reference;
import graql.lang.pattern.variable.ThingVariable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.logic.LogicManager.validateRuleStructureLabels;


public class Rule {

    private final LogicManager logicManager;
    private final RuleStructure structure;
    private final Conjunction when;
    private final Conjunction then;
    private final Set<Conclusion<?>> possibleConclusions; // TODO after restructuring Conclusions, we will have a single conclusion
    private final Set<Concludable<?>> requiredWhenConcludables;

    private Rule(LogicManager logicManager, RuleStructure structure) {
        this.logicManager = logicManager;
        this.structure = structure;
        // TODO enable when we have type hinting
//        this.when = logicManager.typeHinter().computeHintsExhaustive(whenPattern(structure.when()));
//        this.then = logicManager.typeHinter().computeHintsExhaustive(thenPattern(structure.then()));
        this.when = whenPattern(structure.when());
        this.then = thenPattern(structure.then());
        pruneThenTypeHints();
        this.possibleConclusions = buildConclusions(this.then, this.when.variables());
        this.requiredWhenConcludables = Concludable.create(this.when);
    }

    private Rule(GraphManager graphMgr, ConceptManager conceptMgr, LogicManager logicManager, String label,
                 graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        this.logicManager = logicManager;
        this.structure = graphMgr.schema().create(label, when, then);
        validateRuleStructureLabels(conceptMgr, this.structure);
        // TODO enable when we have type hinting
//        this.when = logicManager.typeHinter().computeHintsExhaustive(whenPattern(structure.when()));
//        this.then = logicManager.typeHinter().computeHintsExhaustive(thenPattern(structure.then()));
        this.when = whenPattern(structure.when());
        this.then = thenPattern(structure.then());
        validateSatisfiable();
        pruneThenTypeHints();

        this.possibleConclusions = buildConclusions(this.then, this.when.variables());
        this.requiredWhenConcludables = Concludable.create(this.when);
        validateCycles();
    }

    public static Rule of(LogicManager logicManager, RuleStructure structure) {
        return new Rule(logicManager, structure);
    }

    public static Rule of(GraphManager graphMgr, ConceptManager conceptMgr, LogicManager logicManager, String label,
                          graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        return new Rule(graphMgr, conceptMgr, logicManager, label, when, then);
    }

    public Set<Concludable<?>> whenConcludables() {
        return requiredWhenConcludables;
    }

    public Set<Conclusion<?>> possibleConclusions() {
        return possibleConclusions;
    }

    public Map<Identifier, Concept> putConclusion(ConceptMap whenConcepts) {
        // TODO
        return null;
    }

    public Conjunction when() {
        return when;
    }

    public String getLabel() {
        return structure.label();
    }

    public void setLabel(String label) {
        structure.label(label);
    }

    public boolean isDeleted() {
        return structure.isDeleted();
    }

    public void delete() {
        structure.delete();
    }

    public ThingVariable<?> getThenPreNormalised() {
        return structure.then();
    }

    public graql.lang.pattern.Conjunction<? extends Pattern> getWhenPreNormalised() {
        return structure.when();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        final Rule that = (Rule) object;
        return this.structure.equals(that.structure);
    }

    @Override
    public final int hashCode() {
        return structure.hashCode(); // does not need caching
    }

    void validateSatisfiable() {
        // TODO check that the rule has a set of satisfiable type hints. We may want to use the stream of combinations of types
        // TODO instead of the collapsed type hints on the `isa` and `sub` constraints
    }

    void validateCycles() {
        // TODO implement this when we have negation
        // TODO detect negated cycles in the rule graph
        // TODO use the new rule as a starting point
        // throw GraknException.of(ErrorMessage.RuleWrite.RULES_IN_NEGATED_CYCLE_NOT_STRATIFIABLE.message(rule));
    }

    /**
     * Remove type hints in the `then` pattern that are not valid in the `when` pattern
     */
    private void pruneThenTypeHints() {
        then.variables().stream().filter(variable -> variable.id().isNamedReference())
                .forEach(thenVar ->
                                 when.variables().stream()
                                         .filter(whenVar -> whenVar.id().equals(thenVar.id()))
                                         .filter(whenVar -> !(whenVar.isSatisfiable() && whenVar.resolvedTypes().isEmpty()))
                                         .findFirst().ifPresent(whenVar -> {
                                     if (thenVar.resolvedTypes().isEmpty() && thenVar.isSatisfiable()) {
                                         thenVar.addResolvedTypes(whenVar.resolvedTypes());
                                     } else thenVar.retainResolvedTypes(whenVar.resolvedTypes());
                                     if (thenVar.resolvedTypes().isEmpty()) thenVar.setSatisfiable(false);
                                 })
                );
    }

    private Set<Conclusion<?>> buildConclusions(Conjunction then, Set<Variable> when) {
        HashSet<Conclusion<?>> conclusions = new HashSet<>();
        then.variables().stream().flatMap(var -> var.constraints().stream()).filter(Constraint::isThing).map(Constraint::asThing)
                .map(constraint -> Conclusion.create(constraint, when)).forEach(conclusions::add);
        return conclusions;
    }

    private Conjunction whenPattern(graql.lang.pattern.Conjunction<? extends Pattern> conjunction) {
        return logicManager.typeResolver().resolveLabels(
                Conjunction.create(conjunction.normalise().patterns().get(0)));
    }

    private Conjunction thenPattern(ThingVariable<?> thenVariable) {
        return logicManager.typeResolver().resolveLabels(
                new Conjunction(VariableRegistry.createFromThings(list(thenVariable)).variables(), set()));
    }

    public abstract static class Conclusion<CONSTRAINT extends Constraint> {

        private final CONSTRAINT constraint;

        private Conclusion(CONSTRAINT constraint, Set<Variable> whenContext) {
            this.constraint = constraint;
            copyAdditionalConstraints(whenContext, new HashSet<>(this.constraint.variables()));
        }

        public CONSTRAINT constraint() {
            return constraint;
        }

        public static Conclusion<?> create(ThingConstraint constraint, Set<Variable> whenContext) {
            if (constraint.isRelation()) return Relation.create(constraint.asRelation(), whenContext);
            else if (constraint.isHas()) return Has.create(constraint.asHas(), whenContext);
            else if (constraint.isIsa()) return Isa.create(constraint.asIsa(), whenContext);
            else if (constraint.isValue()) return Value.create(constraint.asValue(), whenContext);
            else throw GraknException.of(ILLEGAL_STATE);
        }

        public abstract Map<Identifier, Concept> putConclusion(ConceptMap whenConcepts, TraversalEngine traversalEng, ConceptManager conceptMgr);

        public boolean isRelation() {
            return false;
        }

        public boolean isHas() {
            return false;
        }

        public boolean isIsa() {
            return false;
        }

        public boolean isValue() {
            return false;
        }

        public Relation asRelation() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Relation.class));
        }

        public Has asHas() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.class));
        }

        public Isa asIsa() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Isa.class));
        }

        public Value asValue() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Value.class));
        }

        private void copyAdditionalConstraints(Set<Variable> fromVars, Set<Variable> toVars) {
            Map<Variable, Variable> nonAnonFromVarsMap = fromVars.stream()
                    .filter(variable -> !variable.id().reference().isAnonymous())
                    .collect(Collectors.toMap(e -> e, e -> e)); // Create a map for efficient lookups
            toVars.stream().filter(variable -> !variable.id().reference().isAnonymous())
                    .forEach(copyTo -> {
                        if (nonAnonFromVarsMap.containsKey(copyTo)) {
                            Variable copyFrom = nonAnonFromVarsMap.get(copyTo);
                            if (copyTo.isThing() && copyFrom.isThing()) {
                                ConstraintCopier.copyIsaAndValues(copyFrom.asThing(), copyTo.asThing());
                            } else if (copyTo.isType() && copyFrom.isType()) {
                                ConstraintCopier.copyLabelSubAndValueType(copyFrom.asType(), copyTo.asType());
                            } else throw GraknException.of(ILLEGAL_STATE);
                        }
                    });
        }

        public static class Relation extends Conclusion<RelationConstraint> {

            public Relation(RelationConstraint constraint, Set<Variable> whenContext) {
                super(constraint, whenContext);
            }

            @Override
            public Map<Identifier, Concept> putConclusion(ConceptMap whenConcepts, TraversalEngine traversalEng, ConceptManager conceptMgr) {
                Identifier relationTypeIdentifier = constraint().owner().isa().get().type().id();
                RelationType relationType = relationType(whenConcepts, conceptMgr);
                Set<RolePlayer> players = new HashSet<>();
                constraint().players().forEach(rp -> players.add(new RolePlayer(rp, relationType, whenConcepts)));

                Optional<grakn.core.concept.thing.Relation> relationInstance = relationExists(relationType, players, traversalEng, conceptMgr);
                Map<Identifier, Concept> thenConcepts = new HashMap<>();
                if (relationInstance.isPresent()) {
                    thenConcepts.put(relationTypeIdentifier, relationInstance.get());
                } else {
                    grakn.core.concept.thing.Relation relation = insertConclusion(relationType, players);
                    thenConcepts.put(relationTypeIdentifier, relation);
                }
                players.forEach(rp -> {
                    thenConcepts.putIfAbsent(rp.roleTypeIdentifier, rp.roleType);
                    thenConcepts.putIfAbsent(rp.playerIdentifier, rp.player);
                });
                return thenConcepts;
            }

            private grakn.core.concept.thing.Relation insertConclusion(RelationType relationType, Set<RolePlayer> players) {
                grakn.core.concept.thing.Relation relation = relationType.create(true);
                players.forEach(rp -> relation.addPlayer(rp.roleType, rp.player, true));
                return relation;
            }

            private Optional<grakn.core.concept.thing.Relation> relationExists(RelationType relationType, Set<RolePlayer> players,
                                                                               TraversalEngine traversalEng, ConceptManager conceptMgr) {
                Traversal traversal = new Traversal();
                Identifier.Variable relationId = Identifier.Variable.of(SystemReference.of(0));
                traversal.isa(relationId, Identifier.Variable.label(relationType.getLabel().name()), false);
                players.forEach(rp -> {
                    // note: NON-transitive role player types - we require an exact role being played
                    traversal.rolePlayer(relationId, rp.playerIdentifier, set(rp.roleType.getLabel()), rp.repetition);
                    traversal.iid(rp.playerIdentifier, rp.player.getIID());
                });
                ResourceIterator<ConceptMap> iterator = traversalEng.iterator(traversal).map(conceptMgr::conceptMap);
                if (iterator.hasNext()) return Optional.of(iterator.next().get(SystemReference.of(0)).asRelation());
                else return Optional.empty();
            }

            private static class RolePlayer {
                private final Identifier roleTypeIdentifier;
                private final RoleType roleType;
                private final Identifier.Variable playerIdentifier;
                private final Thing player;
                private final int repetition;

                public RolePlayer(RelationConstraint.RolePlayer rp, RelationType scope, ConceptMap whenConcepts) {
                    assert rp.roleType().isPresent();
                    roleTypeIdentifier = rp.roleType().get().id();
                    if (rp.roleType().get().reference().isName()) {
                        roleType = whenConcepts.get(rp.roleType().get().reference().asName()).asRoleType();
                    } else {
                        assert rp.roleType().get().reference().isLabel();
                        roleType = scope.getRelates(rp.roleType().get().label().get().properLabel().name());
                    }
                    assert whenConcepts.contains(rp.player().reference().asName());
                    playerIdentifier = Identifier.Variable.of(rp.player().reference().asName());
                    player = whenConcepts.get(rp.player().reference().asName()).asThing();
                    repetition = rp.repetition();
                }
            }

            private RelationType relationType(ConceptMap whenConcepts, ConceptManager conceptMgr) {
                assert constraint().owner().isa().isPresent();
                TypeVariable type = constraint().owner().isa().get().type();
                if (type.reference().isName()) {
                    Reference.Name typeReference = type.reference().asName();
                    assert whenConcepts.contains(typeReference) && whenConcepts.get(typeReference).isRelationType();
                    return whenConcepts.get(typeReference).asRelationType();
                } else {
                    assert type.reference().isLabel();
                    return conceptMgr.getRelationType(type.label().get().label());
                }
            }

            public static Relation create(RelationConstraint constraint, Set<Variable> whenContext) {
                return new Relation(ConstraintCopier.copyConstraint(constraint), whenContext);
            }

            @Override
            public boolean isRelation() {
                return true;
            }

            @Override
            public Relation asRelation() {
                return this;
            }
        }

        public static class Has extends Conclusion<HasConstraint> {

            public Has(HasConstraint constraint, Set<Variable> whenContext) {
                super(constraint, whenContext);
            }

            @Override
            public Map<Identifier, Concept> putConclusion(ConceptMap whenConcepts, TraversalEngine traversalEng, ConceptManager conceptMgr) {
                Identifier.Variable ownerId = constraint().owner().id();
                assert whenConcepts.contains(ownerId.reference().asName()) && whenConcepts.get(ownerId.reference().asName()).isThing();
                Thing owner = whenConcepts.get(ownerId.reference().asName()).asThing();
                Map<Identifier, Concept> thenConcepts = new HashMap<>();
                Attribute attribute;
                if (constraint().attribute().reference().isName()) {
                    assert whenConcepts.contains(constraint().attribute().reference().asName()) &&
                            whenConcepts.get(constraint().attribute().reference().asName()).isAttribute();
                    attribute = whenConcepts.get(constraint().attribute().reference().asName()).asAttribute();
                } else {
                    assert constraint().attribute().reference().isAnonymous();
                    attribute = getOrCreateAttribute(conceptMgr);
                    TypeVariable declaredType = constraint().attribute().isa().get().type();
                    Identifier declaredTypeIdentifier = declaredType.id();
                    AttributeType attrType = conceptMgr.getAttributeType(declaredType.label().get().properLabel().name());
                    assert attrType.equals(attribute.getType());
                    thenConcepts.put(declaredTypeIdentifier, attrType);
                }
                owner.setHas(attribute, true);
                thenConcepts.put(constraint().attribute().id(), attribute);
                thenConcepts.put(constraint().owner().id(), owner);
                return thenConcepts;
            }

            public static Has create(HasConstraint constraint, Set<Variable> whenContext) {
                return new Has(ConstraintCopier.copyConstraint(constraint), whenContext);
            }

            @Override
            public boolean isHas() {
                return true;
            }

            @Override
            public Has asHas() {
                return this;
            }

            private Attribute getOrCreateAttribute(ConceptManager conceptMgr) {
                assert constraint().attribute().isa().isPresent()
                        && constraint().attribute().isa().get().type().label().isPresent()
                        && constraint().attribute().value().size() == 1
                        && constraint().attribute().value().iterator().next().isValueIdentity();
                Label attributeTypeLabel = constraint().attribute().isa().get().type().label().get().properLabel();
                AttributeType attributeType = conceptMgr.getAttributeType(attributeTypeLabel.name());
                assert attributeType != null;
                ValueConstraint<?> value = constraint().attribute().value().iterator().next();
                if (value.isBoolean()) return attributeType.asBoolean().put(value.asBoolean().value(), true);
                else if (value.isDateTime()) return attributeType.asDateTime().put(value.asDateTime().value(), true);
                else if (value.isDouble()) return attributeType.asDouble().put(value.asDouble().value(), true);
                else if (value.isLong()) return attributeType.asLong().put(value.asLong().value(), true);
                else if (value.isString()) return attributeType.asString().put(value.asString().value(), true);
                else throw GraknException.of(ILLEGAL_STATE);
            }
        }

        public static class Isa extends Conclusion<IsaConstraint> {

            public Isa(IsaConstraint constraint, Set<Variable> whenContext) {
                super(constraint, whenContext);
            }

            @Override
            public Map<Identifier, Concept> putConclusion(ConceptMap whenConcepts, TraversalEngine traversalEng, ConceptManager conceptMgr) {
                return null;
            }

            public static Isa create(IsaConstraint constraint, Set<Variable> whenContext) {
                return new Isa(ConstraintCopier.copyConstraint(constraint), whenContext);
            }

            @Override
            public boolean isIsa() {
                return true;
            }

            @Override
            public Isa asIsa() {
                return this;
            }
        }

        public static class Value extends Conclusion<ValueConstraint<?>> {

            Value(ValueConstraint<?> constraint, Set<Variable> whenContext) {
                super(constraint, whenContext);
            }

            @Override
            public Map<Identifier, Concept> putConclusion(ConceptMap whenConcepts, TraversalEngine traversalEng, ConceptManager conceptMgr) {
                return null;
            }

            public static Value create(ValueConstraint<?> constraint, Set<Variable> whenContext) {
                return new Value(ConstraintCopier.copyConstraint(constraint), whenContext);
            }

            @Override
            public boolean isValue() {
                return true;
            }

            @Override
            public Value asValue() {
                return this;
            }
        }

    }
}
