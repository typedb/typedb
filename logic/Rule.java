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
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    private final Conclusion conclusion;
    private final Set<Concludable<?>> requiredWhenConcludables;

    private Rule(LogicManager logicManager, RuleStructure structure) {
        this.logicManager = logicManager;
        this.structure = structure;
        // TODO enable when we have type hinting
//        this.when = logicManager.typeHinter().computeHintsExhaustive(whenPattern(structure.when()));
//        this.then = logicManager.typeHinter().computeHintsExhaustive(thenPattern(structure.then()));
        this.when = whenPattern(structure.when());
        this.then = thenPattern(structure.then());
        pruneThenResolvedTypes();
        this.conclusion = Conclusion.create(this.then);
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
        pruneThenResolvedTypes();

        this.conclusion = Conclusion.create(this.then);
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

    public Conclusion conclusion() {
        return conclusion;
    }

    public Map<Identifier, Concept> putConclusion(ConceptMap whenAnswer) {
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
    private void pruneThenResolvedTypes() {
        // TODO name is inconsistent with elsewhere
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

    private Conjunction whenPattern(graql.lang.pattern.Conjunction<? extends Pattern> conjunction) {
        return logicManager.typeResolver().resolveLabels(
                Conjunction.create(conjunction.normalise().patterns().get(0)));
    }

    private Conjunction thenPattern(ThingVariable<?> thenVariable) {
        return logicManager.typeResolver().resolveLabels(
                new Conjunction(VariableRegistry.createFromThings(list(thenVariable)).variables(), set()));
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

        public abstract Map<Identifier, Concept> putConclusion(ConceptManager conceptMgr);

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
            public Map<Identifier, Concept> putConclusion(ConceptManager conceptMgr) {
                /*
                get Relation Type
                create instance of relation type
                get each RolePlayer in the RelationConstraint
                 */

                return null;
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

            @Override
            public Map<Identifier, Concept> putConclusion(ConceptManager conceptMgr) {
                return null;
            }

            public static class Explicit extends Has implements Isa, Value {

                private final IsaConstraint isa;
                private final ValueConstraint<?> value;

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

                Explicit(HasConstraint has, IsaConstraint isa, ValueConstraint<?> value) {
                    super(has);
                    this.isa = isa;
                    this.value = value;
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
                public Isa asIsa() {
                    return this;
                }

                @Override
                public ValueConstraint<?> value() {
                    return value;
                }
            }

            public static class Variable extends Has {

                public static Optional<Variable> of(Conjunction conjunction) {
                    return Iterators.iterate(conjunction.variables()).filter(grakn.core.pattern.variable.Variable::isThing)
                            .map(grakn.core.pattern.variable.Variable::asThing)
                            .flatMap(variable -> Iterators.iterate(variable.constraints()).filter(ThingConstraint::isHas)
                                    .filter(constraint -> constraint.asHas().attribute().id().isNamedReference())
                                    .map(constraint -> {
                                        assert !constraint.asHas().attribute().isa().isPresent();
                                        assert constraint.asHas().attribute().value().size() == 0;
                                        return new Has.Variable(constraint.asHas());
                                    })).first();
                }

                Variable(HasConstraint hasConstraint) {
                    super(hasConstraint);
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
