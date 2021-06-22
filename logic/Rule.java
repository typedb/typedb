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

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.structure.RuleStructure;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.Negation;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ThingConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.INVALID_NEGATION_CONTAINS_DISJUNCTION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_CAN_HAVE_INVALID_CONCLUSION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_THEN_CANNOT_BE_SATISFIED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_THEN_INVALID_VALUE_ASSIGNMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_WHEN_CANNOT_BE_SATISFIED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.COLON;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_CLOSE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.CURLY_OPEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.NEW_LINE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SEMICOLON;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Schema.RULE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Schema.THEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Schema.WHEN;


public class Rule {

    private static final Logger LOG = LoggerFactory.getLogger(Rule.class);

    // note: as `Rule` is cached between transactions, we cannot hold any transaction-bound objects such as Managers
    private final RuleStructure structure;
    private final Conjunction when;
    private final Conjunction then;
    private final Conclusion conclusion;
    private final Condition condition;

    private Rule(RuleStructure structure, LogicManager logicMgr) {
        this.structure = structure;
        this.then = thenPattern(structure.then(), logicMgr);
        this.when = whenPattern(structure.when(), structure.then(), logicMgr);
        pruneThenResolvedTypes();
        this.conclusion = Conclusion.create(this);
        this.condition = Condition.create(this);
    }

    public static Rule of(LogicManager logicMgr, RuleStructure structure) {
        return new Rule(structure, logicMgr);
    }

    public static Rule of(String label, com.vaticle.typeql.lang.pattern.Conjunction<? extends Pattern> when, com.vaticle.typeql.lang.pattern.variable.ThingVariable<?> then,
                          GraphManager graphMgr, ConceptManager conceptMgr, LogicManager logicMgr) {
        RuleStructure structure = graphMgr.schema().rules().create(label, when, then);
        Rule rule = new Rule(structure, logicMgr);
        rule.conclusion().index();
        rule.validate(logicMgr, conceptMgr);
        return rule;
    }

    public Conclusion conclusion() {
        return conclusion;
    }

    public Condition condition() {
        return condition;
    }

    public Conjunction when() {
        return when;
    }

    public Conjunction then() {
        return then;
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
        conclusion().unindex();
        structure.delete();
    }

    public com.vaticle.typeql.lang.pattern.variable.ThingVariable<?> getThenPreNormalised() {
        return structure.then();
    }

    public com.vaticle.typeql.lang.pattern.Conjunction<? extends Pattern> getWhenPreNormalised() {
        return structure.when();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        Rule that = (Rule) object;
        return this.structure.equals(that.structure);
    }

    @Override
    public final int hashCode() {
        return structure.hashCode(); // does not need caching
    }

    public void validate(LogicManager logicMgr, ConceptManager conceptMgr) {
        validateSatisfiable();
        this.conclusion.validate(logicMgr, conceptMgr);
    }

    private void validateSatisfiable() {
        if (!when.isCoherent()) throw TypeDBException.of(RULE_WHEN_CANNOT_BE_SATISFIED, structure.label(), when);
        if (!then.isCoherent()) throw TypeDBException.of(RULE_THEN_CANNOT_BE_SATISFIED, structure.label(), then);
    }


    /**
     * Remove type hints in the `then` pattern that are not valid in the `when` pattern
     */
    private void pruneThenResolvedTypes() {
        then.variables().stream().filter(variable -> variable.id().isName())
                .forEach(thenVar -> {
                    Variable whenVar = when.variable(thenVar.id());
                    thenVar.retainResolvedTypes(whenVar.resolvedTypes());
                    if (thenVar.resolvedTypes().isEmpty()) then.setCoherent(false);
                });
    }

    private Conjunction whenPattern(com.vaticle.typeql.lang.pattern.Conjunction<? extends Pattern> conjunction,
                                    com.vaticle.typeql.lang.pattern.variable.ThingVariable<?> then, LogicManager logicMgr) {
        Disjunction when = Disjunction.create(conjunction.normalise(), VariableRegistry.createFromThings(list(then)));
        assert when.conjunctions().size() == 1;

        if (iterate(when.conjunctions().get(0).negations()).filter(neg -> neg.disjunction().conjunctions().size() != 1).hasNext()) {
            throw TypeDBException.of(INVALID_NEGATION_CONTAINS_DISJUNCTION, getLabel());
        }

        logicMgr.typeResolver().resolve(when);
        return when.conjunctions().get(0);
    }

    private Conjunction thenPattern(com.vaticle.typeql.lang.pattern.variable.ThingVariable<?> thenVariable, LogicManager logicMgr) {
        Conjunction conj = new Conjunction(VariableRegistry.createFromThings(list(thenVariable)).variables(), set());
        logicMgr.typeResolver().resolveVariables(conj, true);
        return conj;
    }

    public void reindex() {
        conclusion().unindex();
        conclusion().index();
    }


    @Override
    public String toString() {
        return "" + RULE + SPACE + getLabel() + COLON + NEW_LINE + WHEN + SPACE + CURLY_OPEN + NEW_LINE + when + NEW_LINE +
                CURLY_CLOSE + SPACE + THEN + SPACE + CURLY_OPEN + NEW_LINE + then + NEW_LINE + CURLY_CLOSE + SEMICOLON;
    }

    public static class Condition {

        private final Rule rule;
        private Set<Concludable> concludablesTriggeringRules;
        private Set<Concludable> negatedConcludablesTriggeringRules;

        Condition(Rule rule) {
            this.rule = rule;
            this.concludablesTriggeringRules = null;
            this.negatedConcludablesTriggeringRules = null;
        }

        public static Condition create(Rule rule) {
            return new Condition(rule);
        }

        public Rule rule() {
            return rule;
        }

        public Set<Concludable> concludablesTriggeringRules(ConceptManager conceptMgr, LogicManager logicMgr) {
            if (concludablesTriggeringRules == null) { // only acquire lock if required
                synchronized (this) { // only compute concludables once
                    if (concludablesTriggeringRules == null) {
                        concludablesTriggeringRules = iterate(Concludable.create(rule.when()))
                                .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
                    }
                }
            }
            return concludablesTriggeringRules;
        }

        Set<Concludable> negatedConcludablesTriggeringRules(ConceptManager conceptMgr, LogicManager logicMgr) {
            synchronized (this) { // can be more contentious as only used for validation
                if (negatedConcludablesTriggeringRules == null) {
                    negatedConcludablesTriggeringRules = concludables(rule.when().negations())
                            .filter(c -> c.getApplicableRules(conceptMgr, logicMgr).hasNext()).toSet();
                }
            }
            return negatedConcludablesTriggeringRules;
        }

        private FunctionalIterator<Concludable> concludables(Set<Negation> negations) {
            return iterate(negations)
                    .flatMap(neg -> {
                        assert neg.disjunction().conjunctions().size() == 1;
                        return iterate(neg.disjunction().conjunctions());
                    }).flatMap(conjunction -> {
                        assert conjunction.negations().isEmpty();
                        return iterate(Concludable.create(conjunction));
                    });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Condition that = (Condition) o;
            return rule.equals(that.rule);
        }

        @Override
        public int hashCode() {
            return rule.hashCode();
        }

        @Override
        public String toString() {
            return "Rule[" + rule.getLabel() + "] Condition " + rule.when;
        }
    }

    public static abstract class Conclusion {

        private final Rule rule;
        private final Set<Identifier.Variable.Retrievable> retrievableIds;

        Conclusion(Rule rule, Set<Identifier.Variable.Retrievable> retrievableIds) {
            this.rule = rule;
            this.retrievableIds = retrievableIds;
        }

        public static Conclusion create(Rule rule) {
            Optional<Relation> r = Relation.of(rule);
            if ((r).isPresent()) return r.get();
            Optional<Has.Explicit> e = Has.Explicit.of(rule);
            if (e.isPresent()) return e.get();
            Optional<Has.Variable> v = Has.Variable.of(rule);
            if (v.isPresent()) return v.get();
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public Conjunction conjunction() {
            return rule().then();
        }

        /**
         * Perform a put operation on the `then` of the rule. This may insert a new fact, or return an iterator of existing ones
         *
         * @param whenConcepts - the concepts that satisfy the `when` of the rule. All named `then` variables must be in this map
         * @param traversalEng - used to perform a traversal to find preexisting conclusions
         * @param conceptMgr   - used to insert the conclusion if it doesn't already exist
         * @return - all possible conclusions: there may be multiple preexisting satisfactory conclusions, we return all
         */
        public abstract FunctionalIterator<Map<Identifier.Variable, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                                          ConceptManager conceptMgr);

        public Rule rule() { return rule; }

        public abstract Optional<ThingVariable> generating();

        public Set<Identifier.Variable.Retrievable> retrievableIds() {
            return retrievableIds;
        }

        abstract void index();

        abstract void unindex();

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

        public boolean isExplicitHas() {
            return false;
        }

        public boolean isVariableHas() {
            return false;
        }

        public Relation asRelation() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Relation.class));
        }

        public Has asHas() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Has.class));
        }

        public Isa asIsa() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Isa.class));
        }

        public Value asValue() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Value.class));
        }

        public Has.Variable asVariableHas() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Has.Variable.class));
        }

        public Has.Explicit asExplicitHas() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Has.Explicit.class));
        }

        public void validate(LogicManager logicMgr, ConceptManager conceptMgr) {
            validateInsertable(logicMgr);
        }

        private void validateInsertable(LogicManager logicMgr) {
            FunctionalIterator<Map<Identifier.Variable.Name, Label>> whenCombinations = logicMgr.typeResolver().namedCombinations(rule.when, false);
            Set<Map<Identifier.Variable.Name, Label>> allowedThenCombinations = logicMgr.typeResolver().namedCombinations(rule.then, true).toSet();

            whenCombinations.forEachRemaining(nameLabelMap -> {
                if (allowedThenCombinations.stream().noneMatch(thenMap -> nameLabelMap.entrySet().containsAll(thenMap.entrySet())))
                    throw TypeDBException.of(RULE_CAN_HAVE_INVALID_CONCLUSION, rule.structure.label(), nameLabelMap.toString());
            });
        }

        public interface Isa {
            IsaConstraint isa();
        }

        public interface Value {
            ValueConstraint<?> value();
        }

        @Override
        public String toString() {
            return "Rule[" + rule.getLabel() + "] Conclusion " + rule.then;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Conclusion that = (Conclusion) o;
            return rule.equals(that.rule);
        }

        @Override
        public int hashCode() {
            return rule.hashCode();
        }

        public static class Relation extends Conclusion implements Isa {

            private final RelationConstraint relation;
            private final IsaConstraint isa;

            public Relation(RelationConstraint relation, IsaConstraint isa, Rule rule) {
                super(rule, retrievableIds(relation, isa));
                this.relation = relation;
                this.isa = isa;
            }

            public static Optional<Relation> of(Rule rule) {
                return iterate(rule.then().variables()).filter(Variable::isThing).map(Variable::asThing)
                        .flatMap(variable -> iterate(variable.constraints())
                                .filter(ThingConstraint::isRelation)
                                .map(constraint -> {
                                    assert constraint.owner().isa().isPresent();
                                    return new Relation(constraint.asRelation(), variable.isa().get(), rule);
                                })).first();
            }

            private static Set<Identifier.Variable.Retrievable> retrievableIds(RelationConstraint relation, IsaConstraint isa) {
                Set<Identifier.Variable.Retrievable> ids = new HashSet<>();
                assert isa.owner().equals(relation.owner());
                ids.add(relation.owner().id());
                relation.players().forEach(rp -> {
                    ids.add(rp.player().id());
                    if (rp.roleType().isPresent() && rp.roleType().get().id().isRetrievable()) {
                        ids.add(rp.roleType().get().id().asRetrievable());
                    }
                });
                if (isa.type().id().isRetrievable()) ids.add(isa.type().id().asRetrievable());
                return ids;
            }

            @Override
            public FunctionalIterator<Map<Identifier.Variable, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                                     ConceptManager conceptMgr) {
                Identifier.Variable relationTypeIdentifier = isa().type().id();
                RelationType relationType = relationType(whenConcepts, conceptMgr);
                FunctionalIterator<com.vaticle.typedb.core.concept.thing.Relation> existingRelations = matchRelation(
                        relationType, whenConcepts, traversalEng, conceptMgr
                );
                if (existingRelations.hasNext()) {
                    return existingRelations.map(rel -> {
                        Map<Identifier.Variable, Concept> thenConcepts = new HashMap<>();
                        thenConcepts.put(relationTypeIdentifier, relationType);
                        thenConcepts.put(isa().owner().id(), rel);
                        relation().players().forEach(rp -> {
                            thenConcepts.putIfAbsent(rp.roleType().get().id(), getRole(rp, relationType, whenConcepts));
                            thenConcepts.putIfAbsent(rp.player().id(), whenConcepts.get(rp.player().id()));
                        });
                        return thenConcepts;
                    });
                } else {
                    return insert(relationType, whenConcepts, conceptMgr);
                }
            }

            private FunctionalIterator<Map<Identifier.Variable, Concept>> insert(RelationType relationType, ConceptMap whenConcepts, ConceptManager conceptMgr) {
                Map<Identifier.Variable, Concept> thenConcepts = new HashMap<>();
                thenConcepts.put(isa().type().id(), relationType);
                com.vaticle.typedb.core.concept.thing.Relation relation = relationType.create(true);
                thenConcepts.put(isa().owner().id(), relation);
                relation().players().forEach(rp -> {
                    RoleType role = getRole(rp, relationType, whenConcepts);
                    Thing player = whenConcepts.get(rp.player().id()).asThing();
                    relation.addPlayer(role, player, true);
                    thenConcepts.putIfAbsent(rp.roleType().get().id(), role);
                    thenConcepts.putIfAbsent(rp.player().id(), player);
                });
                return Iterators.single(thenConcepts);
            }

            @Override
            public Optional<ThingVariable> generating() {
                return Optional.of(relation.owner());
            }

            @Override
            void index() {
                Variable relation = relation().owner();
                Set<Label> possibleRelationTypes = relation.resolvedTypes();
                possibleRelationTypes.forEach(rule().structure::indexConcludesVertex);
            }

            @Override
            void unindex() {
                Variable relation = relation().owner();
                Set<Label> possibleRelationTypes = relation.resolvedTypes();
                possibleRelationTypes.forEach(rule().structure::unindexConcludesVertex);
            }

            public RelationConstraint relation() {
                return relation;
            }

            @Override
            public IsaConstraint isa() {
                return isa;
            }

            @Override
            public boolean isIsa() {
                return true;
            }

            @Override
            public boolean isRelation() {
                return true;
            }

            public Isa asIsa() {
                return this;
            }

            @Override
            public Relation asRelation() {
                return this;
            }

            private FunctionalIterator<com.vaticle.typedb.core.concept.thing.Relation> matchRelation(RelationType relationType, ConceptMap whenConcepts,
                                                                                                     TraversalEngine traversalEng, ConceptManager conceptMgr) {
                Traversal traversal = new Traversal();
                Identifier.Variable.Retrievable relationId = relation().owner().id();
                traversal.types(relationId, set(relationType.getLabel()));
                Set<Identifier.Variable> playersWithIIDs = new HashSet<>();
                relation().players().forEach(rp -> {
                    Identifier.Variable.Retrievable playerId = rp.player().id();
                    assert rp.roleType().isPresent() && rp.roleType().get().label().isPresent() && whenConcepts.contains(playerId);
                    traversal.rolePlayer(relationId, playerId, set(getRole(rp, relationType, whenConcepts).getLabel()), rp.repetition());
                    if (!playersWithIIDs.contains(playerId)) {
                        traversal.iid(playerId, whenConcepts.get(playerId).asThing().getIID());
                        playersWithIIDs.add(playerId);
                    }
                });
                return traversalEng.iterator(traversal).map(conceptMgr::conceptMap)
                        .map(conceptMap -> conceptMap.get(relationId).asRelation());
            }

            private RelationType relationType(ConceptMap whenConcepts, ConceptManager conceptMgr) {
                if (isa().type().reference().isName()) {
                    Reference.Name typeReference = isa().type().reference().asName();
                    assert whenConcepts.contains(typeReference) && whenConcepts.get(typeReference).isRelationType();
                    return whenConcepts.get(typeReference).asRelationType();
                } else {
                    assert isa().type().reference().isLabel();
                    return conceptMgr.getRelationType(isa().type().label().get().label());
                }
            }

            private RoleType getRole(RelationConstraint.RolePlayer rp, RelationType scope, ConceptMap whenConcepts) {
                if (rp.roleType().get().reference().isName()) {
                    return whenConcepts.get(rp.roleType().get().reference().asName()).asRoleType();
                } else {
                    assert rp.roleType().get().reference().isLabel();
                    return scope.getRelates(rp.roleType().get().label().get().properLabel().name());
                }
            }
        }

        public static abstract class Has extends Conclusion {

            private final HasConstraint has;

            Has(HasConstraint has, Rule rule) {
                super(rule, set(has.owner().id(), has.attribute().id()));
                this.has = has;
            }

            public HasConstraint has() {
                return has;
            }

            @Override
            public Has asHas() {
                return this;
            }

            @Override
            public boolean isHas() {
                return true;
            }

            public static class Explicit extends Has implements Isa, Value {

                private final IsaConstraint isa;
                private final ValueConstraint<?> value;

                private Explicit(HasConstraint has, IsaConstraint isa, ValueConstraint<?> value, Rule rule) {
                    super(has, rule);
                    this.isa = isa;
                    this.value = value;
                }

                public static Optional<Explicit> of(Rule rule) {
                    return iterate(rule.then().variables()).filter(com.vaticle.typedb.core.pattern.variable.Variable::isThing)
                            .map(com.vaticle.typedb.core.pattern.variable.Variable::asThing)
                            .flatMap(variable -> iterate(variable.constraints()).filter(ThingConstraint::isHas)
                                    .filter(constraint -> constraint.asHas().attribute().id().reference().isAnonymous())
                                    .map(constraint -> {
                                        assert constraint.asHas().attribute().isa().isPresent();
                                        assert constraint.asHas().attribute().isa().get().type().label().isPresent();
                                        assert constraint.asHas().attribute().value().size() == 1;
                                        return new Has.Explicit(constraint.asHas(), constraint.asHas().attribute().isa().get(),
                                                                constraint.asHas().attribute().value().iterator().next(),
                                                                rule);
                                    })).first();
                }

                @Override
                public void validate(LogicManager logicMgr, ConceptManager conceptMgr) {
                    super.validate(logicMgr, conceptMgr);
                    validateAssignableValue(conceptMgr);
                }

                private void validateAssignableValue(ConceptManager conceptMgr) {
                    Label attributeTypeLabel = isa().type().label().get().properLabel();
                    AttributeType attributeType = conceptMgr.getAttributeType(attributeTypeLabel.name());
                    assert attributeType != null;
                    AttributeType.ValueType attrTypeValueType = attributeType.getValueType();
                    ValueConstraint<?> value = has().attribute().value().iterator().next();
                    if (!AttributeType.ValueType.of(value.value().getClass()).assignables().contains(attrTypeValueType)) {
                        throw TypeDBException.of(RULE_THEN_INVALID_VALUE_ASSIGNMENT, rule().getLabel(),
                                                 value.value().getClass().getSimpleName(),
                                                 attributeType.getValueType().getValueClass().getSimpleName());
                    }
                }

                @Override
                public FunctionalIterator<Map<Identifier.Variable, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                                         ConceptManager conceptMgr) {
                    Identifier.Variable.Retrievable ownerId = has().owner().id();
                    assert whenConcepts.contains(ownerId) && whenConcepts.get(ownerId).isThing();
                    Map<Identifier.Variable, Concept> thenConcepts = new HashMap<>();
                    Thing owner = whenConcepts.get(ownerId.reference().asName()).asThing();
                    Attribute attribute = putAttribute(conceptMgr);
                    owner.setHas(attribute, true);
                    TypeVariable declaredType = has().attribute().isa().get().type();
                    Identifier.Variable declaredTypeId = declaredType.id();
                    AttributeType attrType = conceptMgr.getAttributeType(declaredType.label().get().properLabel().name());
                    assert attrType.equals(attribute.getType());
                    thenConcepts.put(declaredTypeId, attrType);
                    thenConcepts.put(has().attribute().id(), attribute);
                    thenConcepts.put(has().owner().id(), owner);
                    return Iterators.single(thenConcepts);
                }

                @Override
                public Optional<ThingVariable> generating() {
                    return Optional.of(has().attribute());
                }

                @Override
                void index() {
                    com.vaticle.typedb.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(label -> {
                        rule().structure.indexConcludesVertex(label);
                        rule().structure.indexConcludesEdgeTo(label);
                    });
                }

                @Override
                void unindex() {
                    com.vaticle.typedb.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(label -> {
                        rule().structure.unindexConcludesVertex(label);
                        rule().structure.unindexConcludesEdgeTo(label);
                    });
                }

                @Override
                public boolean isExplicitHas() {
                    return true;
                }

                @Override
                public Has.Explicit asExplicitHas() {
                    return this;
                }

                @Override
                public IsaConstraint isa() {
                    return isa;
                }

                @Override
                public boolean isIsa() {
                    return true;
                }

                @Override
                public boolean isValue() {
                    return true;
                }

                @Override
                public Isa asIsa() {
                    return this;
                }

                @Override
                public Value asValue() {
                    return this;
                }

                @Override
                public ValueConstraint<?> value() {
                    return value;
                }

                private Attribute putAttribute(ConceptManager conceptMgr) {
                    assert has().attribute().isa().isPresent()
                            && has().attribute().isa().get().type().label().isPresent()
                            && has().attribute().value().size() == 1
                            && has().attribute().value().iterator().next().isValueIdentity();
                    Label attributeTypeLabel = isa().type().label().get().properLabel();
                    AttributeType attrType = conceptMgr.getAttributeType(attributeTypeLabel.name());
                    assert attrType != null;
                    ValueConstraint<?> value = has().attribute().value().iterator().next();
                    if (attrType.isDateTime()) return attrType.asDateTime().put(value.asDateTime().value(), true);
                    else if (attrType.isBoolean()) return attrType.asBoolean().put(value.asBoolean().value(), true);
                    else if (attrType.isDouble()) return attrType.asDouble().put(value.asDouble().value(), true);
                    else if (attrType.isLong()) return attrType.asLong().put(value.asLong().value(), true);
                    else if (attrType.isString()) return attrType.asString().put(value.asString().value(), true);
                    else throw TypeDBException.of(ILLEGAL_STATE);
                }

            }

            public static class Variable extends Has {

                private Variable(HasConstraint hasConstraint, Rule rule) {
                    super(hasConstraint, rule);
                }

                public static Optional<Variable> of(Rule rule) {
                    return iterate(rule.then().variables()).filter(com.vaticle.typedb.core.pattern.variable.Variable::isThing)
                            .map(com.vaticle.typedb.core.pattern.variable.Variable::asThing)
                            .flatMap(variable -> iterate(variable.constraints()).filter(ThingConstraint::isHas)
                                    .filter(constraint -> constraint.asHas().attribute().id().isName())
                                    .map(constraint -> {
                                        assert !constraint.asHas().attribute().isa().isPresent();
                                        assert constraint.asHas().attribute().value().size() == 0;
                                        return new Has.Variable(constraint.asHas(), rule);
                                    })).first();
                }

                @Override
                public FunctionalIterator<Map<Identifier.Variable, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                                         ConceptManager conceptMgr) {
                    Identifier.Variable.Retrievable ownerId = has().owner().id();
                    assert whenConcepts.contains(ownerId) && whenConcepts.get(ownerId).isThing();
                    Thing owner = whenConcepts.get(ownerId).asThing();
                    Map<Identifier.Variable, Concept> thenConcepts = new HashMap<>();
                    assert whenConcepts.contains(has().attribute().id())
                            && whenConcepts.get(has().attribute().id()).isAttribute();
                    Attribute attr = whenConcepts.get(has().attribute().id()).asAttribute();
                    owner.setHas(attr, true);
                    thenConcepts.put(has().attribute().id(), attr);
                    thenConcepts.put(has().owner().id(), owner);
                    return Iterators.single(thenConcepts);
                }

                @Override
                public Optional<ThingVariable> generating() {
                    return Optional.empty();
                }

                @Override
                void index() {
                    com.vaticle.typedb.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(rule().structure::indexConcludesEdgeTo);
                }

                @Override
                void unindex() {
                    com.vaticle.typedb.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(rule().structure::unindexConcludesEdgeTo);
                }

                @Override
                public boolean isVariableHas() {
                    return true;
                }

                @Override
                public Variable asVariableHas() {
                    return this;
                }
            }

        }
    }
}
