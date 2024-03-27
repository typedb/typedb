/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
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
import com.vaticle.typedb.core.logic.resolvable.ResolvableConjunction;
import com.vaticle.typedb.core.logic.resolvable.ResolvableDisjunction;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.constraint.common.Predicate;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.PredicateConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ThingConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.Reference;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.pattern.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_CONCLUSION_ILLEGAL_INSERT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_THEN_INCOHERENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_THEN_INSERTS_ABSTRACT_TYPES;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_THEN_INVALID_VALUE_ASSIGNMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_WHEN_INCOHERENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_WHEN_UNANSWERABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.RuleWrite.RULE_WHEN_UNANSWERABLE_BRANCH;
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
    private final Disjunction when;
    private final Conjunction then;
    private final Conclusion conclusion;
    private final Condition condition;

    private Rule(RuleStructure structure, LogicManager logicMgr) {
        this.structure = structure;
        this.when = whenPattern(structure.when(), logicMgr);
        this.then = thenPattern(structure.then(), logicMgr);
        this.conclusion = Conclusion.create(this);
        this.condition = Condition.create(this);
    }

    public static Rule of(LogicManager logicMgr, RuleStructure structure) {
        return new Rule(structure, logicMgr);
    }

    public static Rule of(String label, com.vaticle.typeql.lang.pattern.Conjunction<? extends Pattern> when,
                          com.vaticle.typeql.lang.pattern.statement.ThingStatement<?> then,
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

    public Disjunction when() {
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

    public com.vaticle.typeql.lang.pattern.statement.ThingStatement<?> getThenPreNormalised() {
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
        iterate(when.conjunctions())
                .flatMap(c -> iterate(c.variables())).flatMap(v -> iterate(v.constraints()))
                .filter(c -> c.isType() && c.asType().isLabel() && c.asType().asLabel().properLabel().scope().isPresent())
                // only validate labels that are used outside of relation constraints - this allows role type aliases in relations
                .filter(label -> label.owner().constraining().isEmpty() || iterate(label.owner().constraining()).anyMatch(c -> !(c.isThing() && c.asThing().isRelation())))
                .forEachRemaining(c -> conceptMgr.validateNotRoleTypeAlias(c.asType().asLabel().properLabel()));
        validateSatisfiable();
        this.conclusion.validate(logicMgr, conceptMgr);
    }

    private void validateSatisfiable() {
        if (!when.isCoherent()) throw TypeDBException.of(RULE_WHEN_INCOHERENT, structure.label(), when);
        for (Conjunction whenBranch : when.conjunctions()) {
            if (!whenBranch.isAnswerable()) {
                ErrorMessage errorMessage = when.conjunctions().size() > 1 ? RULE_WHEN_UNANSWERABLE_BRANCH : RULE_WHEN_UNANSWERABLE;
                LOG.warn(errorMessage.message(structure.label(), whenBranch));
            }
        }
        if (!then.isCoherent()) throw TypeDBException.of(RULE_THEN_INCOHERENT, structure.label(), then);
    }

    private Disjunction whenPattern(com.vaticle.typeql.lang.pattern.Conjunction<? extends Pattern> conjunction,
                                    LogicManager logicMgr) {
        // create the When pattern with a reservation of 1 anonymous variable that may be used in the Then pattern
        Disjunction when = Disjunction.create(conjunction.normalise(), VariableRegistry.createReservedAnonymous(1));
        logicMgr.typeInference().applyCombination(when);
        logicMgr.expressionResolver().resolveExpressions(when);
        return when;
    }

    private Conjunction thenPattern(com.vaticle.typeql.lang.pattern.statement.ThingStatement<?> thenStatement, LogicManager logicMgr) {
        Conjunction conj = new Conjunction(VariableRegistry.createFromThings(list(thenStatement)).variables(), list());
        Map<Identifier.Variable.Name, Set<Label>> whenTypes = new HashMap<>();
        iterate(conj.variables()).filter(var -> !var.isValue()).map(Variable::id).filter(Identifier::isName).forEachRemaining(thenVar -> {
            Set<Label> whenTypesForVar = iterate(when.conjunctions()).flatMap(cj -> iterate(cj.variable(thenVar).inferredTypes())).toSet();
            whenTypes.put(thenVar.asName(), whenTypesForVar);
        });
        logicMgr.typeInference().applyCombination(conj, whenTypes, true);
        return conj;
    }

    public void getSyntax(StringBuilder builder) {
        builder.append(NEW_LINE).append(RULE).append(SPACE)
                .append(getLabel()).append(COLON).append(SPACE).append(WHEN)
                .append(getWhenPreNormalised())
                .append(SPACE).append(THEN).append(SPACE)
                .append(new com.vaticle.typeql.lang.pattern.Conjunction<>(list(getThenPreNormalised())))
                .append(SEMICOLON).append(NEW_LINE);
    }

    @Override
    public String toString() {
        return "" + RULE + SPACE + getLabel() + COLON + NEW_LINE + WHEN + SPACE + CURLY_OPEN + NEW_LINE + when + NEW_LINE +
                CURLY_CLOSE + SPACE + THEN + SPACE + CURLY_OPEN + NEW_LINE + then + NEW_LINE + CURLY_CLOSE + SEMICOLON;
    }

    public static class Condition {
        private final Rule rule;
        private final ResolvableDisjunction disjunction;
        private final Set<ConditionBranch> branches;

        public Condition(Rule rule) {
            this.rule = rule;
            this.disjunction = ResolvableDisjunction.of(rule.when);
            this.branches = iterate(disjunction.conjunctions()).map(c -> new ConditionBranch(rule, c)).toSet();
        }

        public static Condition create(Rule rule) {
            return new Condition(rule);
        }

        public Set<ConditionBranch> branches() {
            return branches;
        }

        public ResolvableDisjunction disjunction() {
            return disjunction;
        }

        public Rule rule() {
            return rule;
        }

        @Override
        public String toString() {
            return "Rule[" + rule.getLabel() + "] Condition: " + disjunction.pattern();
        }

        @Override
        public int hashCode() {
            return rule.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Condition that = (Condition) o;
            return rule.equals(that.rule);
        }

        public static class ConditionBranch {

            private final Rule rule;
            private final ResolvableConjunction conjunction;
            private final int hash;

            ConditionBranch(Rule rule, ResolvableConjunction conjunction) {
                this.rule = rule;
                this.conjunction = conjunction;
                this.hash = Objects.hash(rule, conjunction);
            }

            public Rule rule() {
                return rule;
            }

            public ResolvableConjunction conjunction() {
                return conjunction;
            }

            @Override
            public String toString() {
                return "Rule[" + rule.getLabel() + "] ConditionBranch: " + conjunction.pattern();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                final ConditionBranch that = (ConditionBranch) o;
                return rule.equals(that.rule) && conjunction.equals(that.conjunction);
            }

            @Override
            public int hashCode() {
                return this.hash;
            }
        }
    }

    public static abstract class Conclusion {

        private final Rule rule;
        private final Set<Identifier.Variable.Retrievable> retrievableIds;
        private final ResolvableConjunction conjunction;

        Conclusion(Rule rule, Set<Identifier.Variable.Retrievable> retrievableIds) {
            this.rule = rule;
            this.conjunction = ResolvableConjunction.of(rule.then());
            this.retrievableIds = retrievableIds;
        }

        public static Conclusion create(Rule rule) {
            Optional<Relation> r = Relation.of(rule);
            if ((r).isPresent()) return r.get();
            Optional<Has.WithIsa> e = Has.WithIsa.of(rule);
            if (e.isPresent()) return e.get();
            Optional<Has.WithoutIsa> v = Has.WithoutIsa.of(rule);
            if (v.isPresent()) return v.get();
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public ResolvableConjunction conjunction() {
            return conjunction;
        }

        public Conjunction pattern() {
            return rule().then();
        }

        public abstract Materialisable materialisable(ConceptMap whenConcepts, ConceptManager conceptMgr);

        Optional<Map<Identifier.Variable, Concept>> materialiseAndBind(
                ConceptMap whenConcepts, TraversalEngine traversalEng, ConceptManager conceptMgr
        ) {
            return Materialiser.materialise(materialisable(whenConcepts, conceptMgr), traversalEng, conceptMgr)
                    .map(materialisation -> materialisation.bindToConclusion(this, whenConcepts));
        }

        public Rule rule() {
            return rule;
        }

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

        public boolean isHasWithIsa() {
            return false;
        }

        public boolean isHasWithoutIsa() {
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

        public Has.WithIsa asHasWithIsa() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Has.WithIsa.class));
        }

        public Has.WithoutIsa asHasWithoutIsa() {
            throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Has.WithoutIsa.class));
        }

        public void validate(LogicManager logicMgr, ConceptManager conceptMgr) {
            validateInsertable(logicMgr, conceptMgr);
        }

        private void validateInsertable(LogicManager logicMgr, ConceptManager conceptMgr) {
            // All types (labelled or variabilised) must represent exclusively concrete types
            if (isIsa() && iterate(asIsa().isa().type().inferredTypes())
                    .anyMatch(t -> conceptMgr.getThingType(t.name()).isAbstract())) {
                throw TypeDBException.of(
                        RULE_THEN_INSERTS_ABSTRACT_TYPES, rule.structure.label(), rule.then, asIsa().isa().type().inferredTypes()
                );
            }

            Conjunction clonedThen = rule.then.clone();
            logicMgr.typeInference().applyCombination(clonedThen, true);
            iterate(rule.when.conjunctions()).forEachRemaining(conj -> {
                Set<Identifier.Variable.Name> sharedIDs = iterate(rule.then.retrieves())
                        .filter(id -> id.isName() && conj.retrieves().contains(id))
                        .filter(id -> !conj.variable(id).isValue())
                        .map(Identifier.Variable::asName).toSet();
                FunctionalIterator<Map<Identifier.Variable.Name, Label>> whenPermutations = logicMgr.typeInference()
                        .getPermutations(conj, false, sharedIDs);
                Set<Map<Identifier.Variable.Name, Label>> insertableThenPermutations = logicMgr.typeInference()
                        .getPermutations(rule.then, true, sharedIDs).toSet();
                whenPermutations.forEachRemaining(nameLabelMap -> {
                    if (!insertableThenPermutations.contains(nameLabelMap)) {
                        throw TypeDBException.of(RULE_CONCLUSION_ILLEGAL_INSERT, rule.structure.label(), nameLabelMap.toString());
                    }
                });
            });
        }

        public interface Isa {
            IsaConstraint isa();
        }

        public interface Value {
            PredicateConstraint value();
        }

        public void reindex() {
            unindex();
            index();
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
            public Materialisable materialisable(ConceptMap whenConcepts, ConceptManager conceptMgr) {
                RelationType relationType = relationType(whenConcepts, conceptMgr);
                Map<Pair<RoleType, Thing>, Integer> players = new HashMap<>();
                relation().players().forEach(rp -> {
                    RoleType role = roleType(rp, relationType, whenConcepts);
                    Thing player = whenConcepts.get(rp.player().id()).asThing();
                    Pair<RoleType, Thing> pair = new Pair<>(role, player);
                    players.merge(pair, 1, (k, v) -> v + 1);
                });
                return new Materialisable(relationType, players);
            }

            private RelationType relationType(ConceptMap whenConcepts, ConceptManager conceptMgr) {
                if (isa().type().reference().isName()) {
                    Reference.Name.Concept typeReference = isa().type().reference().asName().asConcept();
                    assert whenConcepts.contains(typeReference) && whenConcepts.get(typeReference).isRelationType();
                    return whenConcepts.get(typeReference).asRelationType();
                } else {
                    assert isa().type().reference().isLabel();
                    return conceptMgr.getRelationType(isa().type().label().get().label());
                }
            }

            private static RoleType roleType(RelationConstraint.RolePlayer rp, RelationType scope,
                                             ConceptMap whenConcepts) {
                if (rp.roleType().get().reference().isName()) {
                    return whenConcepts.get(rp.roleType().get().reference().asName()).asRoleType();
                } else {
                    assert rp.roleType().get().reference().isLabel();
                    return scope.getRelates(rp.roleType().get().label().get().properLabel().name());
                }
            }

            public Map<Identifier.Variable, Concept> thenConcepts(
                    com.vaticle.typedb.core.concept.thing.Relation relation, ConceptMap whenConcepts) {
                RelationType relationType = relation.getType();
                Map<Identifier.Variable, Concept> thenConcepts = new HashMap<>();
                thenConcepts.put(isa().type().id(), relationType);
                thenConcepts.put(isa().owner().id(), relation);
                relation().players().forEach(rp -> {
                    RoleType role = roleType(rp, relationType, whenConcepts);
                    Thing player = whenConcepts.get(rp.player().id()).asThing();
                    thenConcepts.putIfAbsent(rp.roleType().get().id(), role);
                    thenConcepts.putIfAbsent(rp.player().id(), player);
                });
                return thenConcepts;
            }

            @Override
            public Optional<ThingVariable> generating() {
                return Optional.of(relation.owner());
            }

            @Override
            void index() {
                Variable relation = relation().owner();
                Set<Label> possibleRelationTypes = relation.inferredTypes();
                possibleRelationTypes.forEach(rule().structure::indexConcludesVertex);
            }

            @Override
            void unindex() {
                Variable relation = relation().owner();
                Set<Label> possibleRelationTypes = relation.inferredTypes();
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

            public static class Materialisable extends Conclusion.Materialisable {

                private final RelationType relationType;
                private final Map<Pair<RoleType, Thing>, Integer> players;

                public Materialisable(RelationType relationType, Map<Pair<RoleType, Thing>, Integer> players) {
                    this.relationType = relationType;
                    this.players = players;
                }

                public RelationType relationType() {
                    return relationType;
                }

                public Map<Pair<RoleType, Thing>, Integer> players() {
                    return players;
                }

                @Override
                public boolean isRelation() {
                    return true;
                }

                @Override
                public Materialisable asRelation() {
                    return this;
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Materialisable relation = (Materialisable) o;
                    return relationType.equals(relation.relationType) &&
                            players.equals(relation.players);
                }

                @Override
                public int hashCode() {
                    return Objects.hash(relationType, players);
                }

                @Override
                public String toString() {
                    return "Relation{" +
                            "relationType=" + relationType +
                            ", players=" + players +
                            '}';
                }
            }
        }

        public static abstract class Has extends Conclusion {

            private final ThingVariable owner;
            private final ThingVariable attribute;

            Has(HasConstraint has, Rule rule, Set<Identifier.Variable.Retrievable> retrievableIds) {
                super(rule, retrievableIds);
                this.owner = has.owner();
                this.attribute = has.attribute();
            }

            public ThingVariable owner() {
                return owner;
            }

            public ThingVariable attribute() {
                return attribute;
            }

            @Override
            void index() {
                attribute().inferredTypes().forEach(rule().structure::indexConcludesEdgeTo);
            }

            @Override
            void unindex() {
                attribute().inferredTypes().forEach(rule().structure::unindexConcludesEdgeTo);
            }

            @Override
            public Has asHas() {
                return this;
            }

            @Override
            public boolean isHas() {
                return true;
            }

            public static class WithIsa extends Has implements Isa, Value {

                private final IsaConstraint isa;
                private final PredicateConstraint value;

                private WithIsa(HasConstraint has, IsaConstraint isa, PredicateConstraint value, Set<Identifier.Variable.Retrievable> retrievableIds, Rule rule) {
                    super(has, rule, retrievableIds);
                    this.isa = isa;
                    this.value = value;
                }

                public static Optional<WithIsa> of(Rule rule) {
                    return iterate(rule.then().variables()).filter(com.vaticle.typedb.core.pattern.variable.Variable::isThing)
                            .map(com.vaticle.typedb.core.pattern.variable.Variable::asThing)
                            .flatMap(variable -> iterate(variable.constraints()).filter(ThingConstraint::isHas)
                                    .filter(constraint -> constraint.asHas().attribute().id().reference().isAnonymous())
                                    .filter(constraint -> !constraint.asHas().attribute().predicates().isEmpty())
                                    .filter(constraint -> iterate(constraint.asHas().attribute().predicates()).next().predicate().predicate().equals(TypeQLToken.Predicate.Equality.EQ))
                                    .map(constraint -> {
                                        assert constraint.asHas().attribute().isa().isPresent();
                                        assert constraint.asHas().attribute().isa().get().type().label().isPresent();
                                        assert constraint.asHas().attribute().predicates().size() == 1;
                                        PredicateConstraint predicateConstraint = constraint.asHas().attribute().predicates().iterator().next();
                                        Set<Identifier.Variable.Retrievable> retrievableIds = new HashSet<>();
                                        retrievableIds.add(constraint.asHas().owner().id());
                                        retrievableIds.add(constraint.asHas().attribute().id());
                                        if (predicateConstraint.predicate().isValueVar())
                                            retrievableIds.add(predicateConstraint.predicate().asValueVar().value().id());
                                        return new WithIsa(constraint.asHas(), constraint.asHas().attribute().isa().get(),
                                                predicateConstraint, retrievableIds, rule);
                                    })).first();
                }

                @Override
                public void validate(LogicManager logicMgr, ConceptManager conceptMgr) {
                    super.validate(logicMgr, conceptMgr);
                    validateAssignableValue(conceptMgr);
                }

                private void validateAssignableValue(ConceptManager conceptMgr) {
                    AttributeType attributeType = conceptMgr.getAttributeType(isa().type().label().get().properLabel().name());
                    assert attributeType != null;
                    AttributeType.ValueType attrTypeValueType = attributeType.getValueType();
                    Predicate<?> value = attribute().predicates().iterator().next().predicate();
                    if (value.isValueVar()) {
                        rule().when().conjunctions().forEach(conj -> {
                            com.vaticle.typedb.core.pattern.variable.Variable whenVar = conj.variable(value.asValueVar().value().id());
                            assert whenVar != null && whenVar.isValue();
                            if (!whenVar.asValue().valueType().assignables().contains(attrTypeValueType.encoding())) {
                                throw TypeDBException.of(RULE_THEN_INVALID_VALUE_ASSIGNMENT, rule().getLabel(),
                                        whenVar.asValue().valueType().name(),
                                        attributeType.getValueType().getValueClass().getSimpleName());
                            }
                        });
                    } else {
                        if (!AttributeType.ValueType.of(value.value().getClass()).assignables().contains(attrTypeValueType)) {
                            throw TypeDBException.of(RULE_THEN_INVALID_VALUE_ASSIGNMENT, rule().getLabel(),
                                    value.value().getClass().getSimpleName(),
                                    attributeType.getValueType().getValueClass().getSimpleName());
                        }
                    }
                }

                @Override
                public Materialisable materialisable(ConceptMap whenConcepts, ConceptManager conceptMgr) {
                    Identifier.Variable.Retrievable ownerId = owner().id();
                    assert whenConcepts.contains(ownerId) && whenConcepts.get(ownerId).isThing();
                    Thing owner = whenConcepts.get(ownerId.reference().asName()).asThing();

                    assert isa().type().label().isPresent()
                            && attribute().predicates().size() == 1
                            && attribute().predicates().iterator().next().predicate().predicate().equals(TypeQLToken.Predicate.Equality.EQ);
                    AttributeType attrType = conceptMgr.getAttributeType(isa().type().label().get().properLabel().name());
                    assert attrType != null;
                    PredicateConstraint value = attribute().predicates().iterator().next();
                    if (value.predicate().isValueVar())
                        return new Materialisable(owner, attrType, new PredicateConstraint(attribute(), toConstantPredicate(whenConcepts.get(value.predicate().asValueVar().value().id()).asValue())));
                    else if (value.predicate().isConstant()) return new Materialisable(owner, attrType, value);
                    else throw TypeDBException.of(ILLEGAL_STATE);
                }

                private Predicate<?> toConstantPredicate(com.vaticle.typedb.core.concept.value.Value<?> value) {
                    if (value.isBoolean())
                        return new Predicate.Constant.Boolean(TypeQLToken.Predicate.Equality.EQ, value.asBoolean().value());
                    else if (value.isLong())
                        return new Predicate.Constant.Long(TypeQLToken.Predicate.Equality.EQ, value.asLong().value());
                    else if (value.isDouble())
                        return new Predicate.Constant.Double(TypeQLToken.Predicate.Equality.EQ, value.asDouble().value());
                    else if (value.isString())
                        return new Predicate.Constant.String(TypeQLToken.Predicate.Equality.EQ, value.asString().value());
                    else if (value.isDateTime())
                        return new Predicate.Constant.DateTime(TypeQLToken.Predicate.Equality.EQ, value.asDateTime().value());
                    else throw TypeDBException.of(ILLEGAL_STATE);
                }

                public Map<Identifier.Variable, Concept> thenConcepts(Attribute attribute, AttributeType attrType,
                                                                      ConceptMap whenConcepts) {
                    Map<Identifier.Variable, Concept> thenConcepts = new HashMap<>();
                    thenConcepts.put(owner().id(), whenConcepts.get(owner().id()).asThing());
                    thenConcepts.put(attribute().id(), attribute);
                    thenConcepts.put(isa().type().id(), attrType);
                    return thenConcepts;
                }

                @Override
                public Optional<ThingVariable> generating() {
                    return Optional.of(attribute());
                }

                @Override
                void index() {
                    super.index();
                    attribute().inferredTypes().forEach(label -> rule().structure.indexConcludesVertex(label));
                }

                @Override
                void unindex() {
                    super.unindex();
                    attribute().inferredTypes().forEach(label -> rule().structure.unindexConcludesVertex(label));
                }

                @Override
                public boolean isHasWithIsa() {
                    return true;
                }

                @Override
                public WithIsa asHasWithIsa() {
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
                public PredicateConstraint value() {
                    return value;
                }

                public static class Materialisable extends Conclusion.Materialisable {

                    private final Thing owner;
                    private final AttributeType attrType;
                    private final PredicateConstraint value;

                    public Materialisable(Thing owner, AttributeType attrType, PredicateConstraint value) {
                        this.owner = owner;
                        this.attrType = attrType;
                        this.value = value;
                    }

                    @Override
                    public boolean isHasWithIsa() {
                        return true;
                    }

                    @Override
                    public Materialisable asHasWithIsa() {
                        return this;
                    }

                    public Thing owner() {
                        return owner;
                    }

                    public AttributeType attrType() {
                        return attrType;
                    }

                    public PredicateConstraint value() {
                        return value;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (this == o) return true;
                        if (o == null || getClass() != o.getClass()) return false;
                        Materialisable withIsa = (Materialisable) o;
                        return owner.equals(withIsa.owner) && attrType.equals(withIsa.attrType) &&
                                value.equals(withIsa.value);
                    }

                    @Override
                    public int hashCode() {
                        return Objects.hash(owner, attrType, value);
                    }

                    @Override
                    public String toString() {
                        return "Materialisable.Has.WithIsa{" + "owner=" + owner + ", attrType=" +
                                attrType + ", value=" + value + '}';
                    }
                }
            }

            public static class WithoutIsa extends Has {

                private WithoutIsa(HasConstraint hasConstraint, Rule rule) {
                    super(hasConstraint, rule, set(hasConstraint.owner().id(), hasConstraint.attribute().id()));
                }

                public static Optional<WithoutIsa> of(Rule rule) {
                    return iterate(rule.then().variables()).filter(com.vaticle.typedb.core.pattern.variable.Variable::isThing)
                            .map(com.vaticle.typedb.core.pattern.variable.Variable::asThing)
                            .flatMap(variable -> iterate(variable.constraints()).filter(ThingConstraint::isHas)
                                    .filter(constraint -> constraint.asHas().attribute().id().isName())
                                    .map(constraint -> new WithoutIsa(constraint.asHas(), rule)))
                            .first();
                }

                @Override
                public Conclusion.Materialisable materialisable(ConceptMap whenConcepts, ConceptManager conceptMgr) {
                    assert whenConcepts.contains(owner().id()) && whenConcepts.get(owner().id()).isThing();
                    Thing owner = whenConcepts.get(owner().id()).asThing();
                    assert whenConcepts.contains(attribute().id()) && whenConcepts.get(attribute().id()).isAttribute();
                    Attribute attr = whenConcepts.get(attribute().id()).asAttribute();
                    return new Materialisable(owner, attr);
                }

                public Map<Identifier.Variable, Concept> thenConcepts(ConceptMap whenConcepts) {
                    Map<Identifier.Variable, Concept> thenConcepts = new HashMap<>();
                    thenConcepts.put(attribute().id(), whenConcepts.get(attribute().id()).asAttribute());
                    thenConcepts.put(owner().id(), whenConcepts.get(owner().id()).asThing());
                    return thenConcepts;
                }

                @Override
                public Optional<ThingVariable> generating() {
                    return Optional.empty();
                }

                @Override
                public boolean isHasWithoutIsa() {
                    return true;
                }

                @Override
                public WithoutIsa asHasWithoutIsa() {
                    return this;
                }

                public static class Materialisable extends Conclusion.Materialisable {

                    private final Thing owner;
                    private final Attribute attribute;

                    public Materialisable(Thing owner, Attribute attribute) {
                        this.owner = owner;
                        this.attribute = attribute;
                    }

                    @Override
                    public boolean isHasWithoutIsa() {
                        return true;
                    }

                    @Override
                    public Materialisable asHasWithoutIsa() {
                        return this;
                    }

                    public Thing owner() {
                        return owner;
                    }

                    public Attribute attribute() {
                        return attribute;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (this == o) return true;
                        if (o == null || getClass() != o.getClass()) return false;
                        Materialisable variable = (Materialisable) o;
                        return owner.equals(variable.owner) && attribute.equals(variable.attribute);
                    }

                    @Override
                    public int hashCode() {
                        return Objects.hash(owner, attribute);
                    }

                    @Override
                    public String toString() {
                        return "Materialisable.Has.WithoutIsa{" + "owner=" + owner + ", attribute=" + attribute + '}';
                    }
                }
            }
        }

        public static class Materialisable {

            public boolean isRelation() {
                return false;
            }

            public Relation.Materialisable asRelation() {
                throw TypeDBException.of(ILLEGAL_CAST);
            }

            public boolean isHasWithIsa() {
                return false;
            }

            public Has.WithIsa.Materialisable asHasWithIsa() {
                throw TypeDBException.of(ILLEGAL_CAST);
            }

            public boolean isHasWithoutIsa() {
                return false;
            }

            public Has.WithoutIsa.Materialisable asHasWithoutIsa() {
                throw TypeDBException.of(ILLEGAL_CAST);
            }
        }
    }
}
