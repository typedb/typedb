/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.common.iterator.Iterators;
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
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.Negation;
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
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_CONJUNCTION;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.INVALID_NEGATION;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.INVALID_NEGATION_CONTAINS_DISJUNCTION;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.RULE_CANNOT_BE_SATISFIED;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.RULE_CAN_IMPLY_UNINSERTABLE_RESULTS;
import static grakn.core.common.iterator.Iterators.iterate;
import static graql.lang.common.GraqlToken.Char.COLON;
import static graql.lang.common.GraqlToken.Char.CURLY_CLOSE;
import static graql.lang.common.GraqlToken.Char.CURLY_OPEN;
import static graql.lang.common.GraqlToken.Char.NEW_LINE;
import static graql.lang.common.GraqlToken.Char.SEMICOLON;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Schema.RULE;
import static graql.lang.common.GraqlToken.Schema.THEN;
import static graql.lang.common.GraqlToken.Schema.WHEN;


public class Rule {

    // note: as `Rule` is cached between transactions, we cannot hold any transaction-bound objects such as Managers

    private final RuleStructure structure;
    private final Conjunction when;
    private final Conjunction then;
    private final Conclusion conclusion;

    private Rule(LogicManager logicMgr, RuleStructure structure) {
        this.structure = structure;
        this.when = whenPattern(structure.when(), logicMgr);
        this.then = thenPattern(structure.then(), logicMgr);
        pruneThenResolvedTypes();
        this.conclusion = Conclusion.create(this.then);
    }

    private Rule(GraphManager graphMgr, LogicManager logicMgr, String label,
                 graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        this.structure = graphMgr.schema().rules().create(label, when, then);
        this.when = whenPattern(structure.when(), logicMgr);
        this.then = thenPattern(structure.then(), logicMgr);
        pruneThenResolvedTypes();
        validateSatisfiable();
        validateInsertable(logicMgr);
        this.conclusion = Conclusion.create(this.then);
        validateCycles();
        this.conclusion.index(this);
    }

    public static Rule of(LogicManager logicMgr, RuleStructure structure) {
        return new Rule(logicMgr, structure);
    }

    public static Rule of(GraphManager graphMgr, LogicManager logicMgr, String label,
                          graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        return new Rule(graphMgr, logicMgr, label, when, then);
    }

    public Conclusion conclusion() {
        return conclusion;
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
        conclusion().unindex(this);
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

        Rule that = (Rule) object;
        return this.structure.equals(that.structure);
    }

    @Override
    public final int hashCode() {
        return structure.hashCode(); // does not need caching
    }

    public void validateSatisfiable() {
        Stream.concat(then.variables().stream(), when.variables().stream()).forEach(variable -> {
            if (!variable.isSatisfiable())
                throw GraknException.of(RULE_CANNOT_BE_SATISFIED, structure.label(), variable.reference().toString());
        });
    }

    public void validateInsertable(LogicManager logicMgr) {
        ResourceIterator<Map<Reference.Name, Label>> possibleWhenPerms = logicMgr.typeResolver().combinations(when, false);
        Set<Map<Reference.Name, Label>> possibleThenSet = logicMgr.typeResolver().combinations(then, true).toSet();

        possibleWhenPerms.forEachRemaining(nameLabelMap -> {
            if (possibleThenSet.stream().noneMatch(thenMap -> nameLabelMap.entrySet().containsAll(thenMap.entrySet())))
                throw GraknException.of(RULE_CAN_IMPLY_UNINSERTABLE_RESULTS, structure.label(), nameLabelMap.toString());
        });
    }

    @Override
    public String toString() {
        return "" + RULE + SPACE + getLabel() + COLON + NEW_LINE + WHEN + SPACE + CURLY_OPEN + NEW_LINE + when + NEW_LINE +
                CURLY_CLOSE + SPACE + THEN + SPACE + CURLY_OPEN + NEW_LINE + then + NEW_LINE + CURLY_CLOSE + SEMICOLON;
    }

    void validateCycles() {
        // TODO: implement this when we have negation
        // TODO: detect negated cycles in the rule graph
        // TODO: use the new rule as a starting point
        // throw GraknException.of(ErrorMessage.RuleWrite.RULES_IN_NEGATED_CYCLE_NOT_STRATIFIABLE.message(rule));
    }

    /**
     * Remove type hints in the `then` pattern that are not valid in the `when` pattern
     */
    private void pruneThenResolvedTypes() {
        // TODO: name is inconsistent with elsewhere
        then.variables().stream().filter(variable -> variable.id().isName())
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

    private Conjunction whenPattern(graql.lang.pattern.Conjunction<? extends Pattern> conjunction, LogicManager logicMgr) {
        Conjunction conj = Conjunction.create(conjunction.normalise().patterns().get(0));

        // TODO: remove this when we fully implement negation and don't have to ban it in rules
//        if (!conj.negations().isEmpty()) {
//            throw GraknException.of(INVALID_NEGATION, getLabel());
//        }

        if (iterate(conj.negations()).filter(neg -> neg.disjunction().conjunctions().size() != 1).hasNext()) {
            throw GraknException.of(INVALID_NEGATION_CONTAINS_DISJUNCTION, getLabel());
        }

        logicMgr.typeResolver().resolve(conj, set());
        for (Negation negation : conj.negations()) {
            assert negation.disjunction().conjunctions().size() == 1;
            for (Conjunction c : negation.disjunction().conjunctions()) {
                logicMgr.typeResolver().resolve(c, set(conj));
            }
        }
        return conj;
    }

    private Conjunction thenPattern(ThingVariable<?> thenVariable, LogicManager logicMgr) {
        // TODO: when applying the type resolver, we should be using _insert semantics_ during the type resolution!!!
        Conjunction conj = new Conjunction(VariableRegistry.createFromThings(list(thenVariable)).variables(), set());
        logicMgr.typeResolver().resolve(conj, set());
        return conj;
    }

    public void reindex() {
        conclusion().unindex(this);
        conclusion().index(this);
    }

    public static abstract class Conclusion {

        public static Conclusion create(Conjunction then) {
            Optional<Relation> r = Relation.of(then);
            if ((r).isPresent()) return r.get();
            Optional<Has.Explicit> e = Has.Explicit.of(then);
            if (e.isPresent()) return e.get();
            Optional<Has.Variable> v = Has.Variable.of(then);
            if (v.isPresent()) return v.get();
            throw GraknException.of(ILLEGAL_STATE);
        }

        /**
         * Perform a put operation on the `then` of the rule. This may insert a new fact, or return an iterator of existing ones
         *
         * @param whenConcepts - the concepts that satisfy the `when` of the rule. All named `then` variables must be in this map
         * @param traversalEng - used to perform a traversal to find preexisting conclusions
         * @param conceptMgr   - used to insert the conclusion if it doesn't already exist
         * @return - all possible conclusions: there may be multiple preexisting satisfactory conclusions, we return all
         */
        public abstract ResourceIterator<Map<Identifier, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                               ConceptManager conceptMgr);

        abstract void index(Rule rule);

        abstract void unindex(Rule rule);

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

        public Has.Variable asVariableHas() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.Variable.class));
        }

        public Has.Explicit asExplicitHas() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Has.Explicit.class));
        }

        public interface Isa {
            IsaConstraint isa();
        }

        public interface Value {
            ValueConstraint<?> value();
        }

        public static class Relation extends Conclusion implements Isa {

            private final RelationConstraint relation;
            private final IsaConstraint isa;

            public static Optional<Relation> of(Conjunction conjunction) {
                return Iterators.iterate(conjunction.variables()).filter(Variable::isThing).map(Variable::asThing)
                        .flatMap(variable -> Iterators.iterate(variable.constraints())
                                .filter(ThingConstraint::isRelation)
                                .map(constraint -> {
                                    assert constraint.owner().isa().isPresent();
                                    return new Relation(constraint.asRelation(), variable.isa().get());
                                })).first();
            }

            public Relation(RelationConstraint relation, IsaConstraint isa) {
                this.relation = relation;
                this.isa = isa;
            }

            @Override
            public ResourceIterator<Map<Identifier, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                          ConceptManager conceptMgr) {
                Identifier relationTypeIdentifier = isa().type().id();
                RelationType relationType = relationType(whenConcepts, conceptMgr);
                Set<RolePlayer> players = new HashSet<>();
                relation().players().forEach(rp -> players.add(new RolePlayer(rp, relationType, whenConcepts)));
                ResourceIterator<grakn.core.concept.thing.Relation> existingRelations = matchRelation(
                        relationType, players, traversalEng, conceptMgr
                );

                if (existingRelations.hasNext()) {
                    return existingRelations.map(rel -> {
                        Map<Identifier, Concept> thenConcepts = new HashMap<>();
                        thenConcepts.put(relationTypeIdentifier, relationType);
                        thenConcepts.put(isa().owner().id(), rel);
                        players.forEach(rp -> {
                            thenConcepts.putIfAbsent(rp.roleTypeIdentifier, rp.roleType);
                            thenConcepts.putIfAbsent(rp.playerIdentifier, rp.player);
                        });
                        return thenConcepts;
                    });
                } else {
                    Map<Identifier, Concept> thenConcepts = new HashMap<>();
                    thenConcepts.put(relationTypeIdentifier, relationType);
                    grakn.core.concept.thing.Relation relation = insertRelation(relationType, players);
                    thenConcepts.put(isa().owner().id(), relation);
                    players.forEach(rp -> {
                        thenConcepts.putIfAbsent(rp.roleTypeIdentifier, rp.roleType);
                        thenConcepts.putIfAbsent(rp.playerIdentifier, rp.player);
                    });
                    return Iterators.single(thenConcepts);
                }
            }

            @Override
            void index(Rule rule) {
                Variable relation = relation().owner();
                Set<Label> possibleRelationTypes = relation.resolvedTypes();
                possibleRelationTypes.forEach(rule.structure::indexConcludesVertex);
            }

            @Override
            void unindex(Rule rule) {
                Variable relation = relation().owner();
                Set<Label> possibleRelationTypes = relation.resolvedTypes();
                possibleRelationTypes.forEach(rule.structure::unindexConcludesVertex);
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

            private grakn.core.concept.thing.Relation insertRelation(RelationType relationType, Set<RolePlayer> players) {
                grakn.core.concept.thing.Relation relation = relationType.create(true);
                players.forEach(rp -> relation.addPlayer(rp.roleType, rp.player, true));
                return relation;
            }

            private ResourceIterator<grakn.core.concept.thing.Relation> matchRelation(RelationType relationType, Set<RolePlayer> players,
                                                                                      TraversalEngine traversalEng, ConceptManager conceptMgr) {
                Traversal traversal = new Traversal();
                SystemReference relationRef = SystemReference.of(0);
                Identifier.Variable relationId = Identifier.Variable.of(relationRef);
                traversal.isa(relationId, Identifier.Variable.label(relationType.getLabel().name()), false);
                Set<Identifier.Variable> playersWithIds = new HashSet<>();
                players.forEach(rp -> {
                    // note: NON-transitive role player types - we require an exact role being played
                    traversal.rolePlayer(relationId, rp.playerIdentifier, set(rp.roleType.getLabel()), rp.repetition);
                    if (!playersWithIds.contains(rp.playerIdentifier)) {
                        traversal.iid(rp.playerIdentifier, rp.player.getIID());
                        playersWithIds.add(rp.playerIdentifier);
                    }
                });
                return traversalEng.iterator(traversal).map(conceptMgr::conceptMap)
                        .map(conceptMap -> conceptMap.get(relationRef).asRelation());
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

        }

        public static abstract class Has extends Conclusion {

            private final HasConstraint has;

            Has(HasConstraint has) {
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

                private Explicit(HasConstraint has, IsaConstraint isa, ValueConstraint<?> value) {
                    super(has);
                    this.isa = isa;
                    this.value = value;
                }

                public static Optional<Explicit> of(Conjunction conjunction) {
                    return Iterators.iterate(conjunction.variables()).filter(grakn.core.pattern.variable.Variable::isThing)
                            .map(grakn.core.pattern.variable.Variable::asThing)
                            .flatMap(variable -> Iterators.iterate(variable.constraints()).filter(ThingConstraint::isHas)
                                    .filter(constraint -> constraint.asHas().attribute().id().reference().isAnonymous())
                                    .map(constraint -> {
                                        assert constraint.asHas().attribute().isa().isPresent();
                                        assert constraint.asHas().attribute().isa().get().type().label().isPresent();
                                        assert constraint.asHas().attribute().value().size() == 1;
                                        return new Has.Explicit(constraint.asHas(), constraint.asHas().attribute().isa().get(),
                                                                constraint.asHas().attribute().value().iterator().next());
                                    })).first();
                }

                @Override
                public ResourceIterator<Map<Identifier, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                              ConceptManager conceptMgr) {
                    Identifier.Variable ownerId = has().owner().id();
                    assert whenConcepts.contains(ownerId.reference().asName()) && whenConcepts.get(ownerId.reference().asName()).isThing();
                    Thing owner = whenConcepts.get(ownerId.reference().asName()).asThing();
                    Map<Identifier, Concept> thenConcepts = new HashMap<>();
                    Attribute attribute = getOrCreateAttribute(conceptMgr);
                    owner.setHas(attribute, true);
                    TypeVariable declaredType = has().attribute().isa().get().type();
                    Identifier declaredTypeIdentifier = declaredType.id();
                    AttributeType attrType = conceptMgr.getAttributeType(declaredType.label().get().properLabel().name());
                    assert attrType.equals(attribute.getType());
                    thenConcepts.put(declaredTypeIdentifier, attrType);
                    thenConcepts.put(has().attribute().id(), attribute);
                    thenConcepts.put(has().owner().id(), owner);
                    return Iterators.single(thenConcepts);
                }

                @Override
                void index(Rule rule) {
                    grakn.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(label -> {
                        rule.structure.indexConcludesVertex(label);
                        rule.structure.indexConcludesEdgeTo(label);
                    });
                }

                @Override
                void unindex(Rule rule) {
                    grakn.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(label -> {
                        rule.structure.unindexConcludesVertex(label);
                        rule.structure.unindexConcludesEdgeTo(label);
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


                private Attribute getOrCreateAttribute(ConceptManager conceptMgr) {
                    assert has().attribute().isa().isPresent()
                            && has().attribute().isa().get().type().label().isPresent()
                            && has().attribute().value().size() == 1
                            && has().attribute().value().iterator().next().isValueIdentity();
                    Label attributeTypeLabel = has().attribute().isa().get().type().label().get().properLabel();
                    AttributeType attributeType = conceptMgr.getAttributeType(attributeTypeLabel.name());
                    assert attributeType != null;
                    ValueConstraint<?> value = has().attribute().value().iterator().next();
                    if (value.isBoolean()) return attributeType.asBoolean().put(value.asBoolean().value(), true);
                    else if (value.isDateTime())
                        return attributeType.asDateTime().put(value.asDateTime().value(), true);
                    else if (value.isDouble()) return attributeType.asDouble().put(value.asDouble().value(), true);
                    else if (value.isLong()) return attributeType.asLong().put(value.asLong().value(), true);
                    else if (value.isString()) return attributeType.asString().put(value.asString().value(), true);
                    else throw GraknException.of(ILLEGAL_STATE);
                }

            }

            public static class Variable extends Has {

                private Variable(HasConstraint hasConstraint) {
                    super(hasConstraint);
                }

                public static Optional<Variable> of(Conjunction conjunction) {
                    return Iterators.iterate(conjunction.variables()).filter(grakn.core.pattern.variable.Variable::isThing)
                            .map(grakn.core.pattern.variable.Variable::asThing)
                            .flatMap(variable -> Iterators.iterate(variable.constraints()).filter(ThingConstraint::isHas)
                                    .filter(constraint -> constraint.asHas().attribute().id().isName())
                                    .map(constraint -> {
                                        assert !constraint.asHas().attribute().isa().isPresent();
                                        assert constraint.asHas().attribute().value().size() == 0;
                                        return new Has.Variable(constraint.asHas());
                                    })).first();
                }

                @Override
                public ResourceIterator<Map<Identifier, Concept>> materialise(ConceptMap whenConcepts, TraversalEngine traversalEng,
                                                                              ConceptManager conceptMgr) {
                    Identifier.Variable ownerId = has().owner().id();
                    assert whenConcepts.contains(ownerId.reference().asName())
                            && whenConcepts.get(ownerId.reference().asName()).isThing();
                    Thing owner = whenConcepts.get(ownerId.reference().asName()).asThing();
                    Map<Identifier, Concept> thenConcepts = new HashMap<>();
                    assert whenConcepts.contains(has().attribute().reference().asName())
                            && whenConcepts.get(has().attribute().reference().asName()).isAttribute();
                    Attribute attribute = whenConcepts.get(has().attribute().reference().asName()).asAttribute();
                    owner.setHas(attribute, true);
                    thenConcepts.put(has().attribute().id(), attribute);
                    thenConcepts.put(has().owner().id(), owner);
                    return Iterators.single(thenConcepts);
                }

                @Override
                void index(Rule rule) {
                    grakn.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(rule.structure::indexConcludesEdgeTo);
                }

                @Override
                void unindex(Rule rule) {
                    grakn.core.pattern.variable.Variable attribute = has().attribute();
                    Set<Label> possibleAttributeHas = attribute.resolvedTypes();
                    possibleAttributeHas.forEach(rule.structure::unindexConcludesEdgeTo);
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
