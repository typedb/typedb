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
import grakn.core.concept.ConceptManager;
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
import grakn.core.pattern.constraint.thing.RelationConstraint.RolePlayer;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.pattern.Pattern;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.logic.LogicManager.validateRuleStructureLabels;


public class Rule {

    private final LogicManager logicManager;
    private final RuleStructure structure;
    private final Conjunction when;
    private final Conjunction then;
    private final Set<Conclusion<?, ?>> possibleConclusions;
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
                 graql.lang.pattern.Conjunction<? extends Pattern> when, graql.lang.pattern.variable.ThingVariable<?> then) {
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
                          graql.lang.pattern.Conjunction<? extends Pattern> when, graql.lang.pattern.variable.ThingVariable<?> then) {
        return new Rule(graphMgr, conceptMgr, logicManager, label, when, then);
    }

    public Set<Concludable<?>> whenConcludables() {
        return requiredWhenConcludables;
    }

    public Set<Conclusion<?, ?>> possibleConclusions() {
        return possibleConclusions;
    }

    public ResourceIterator<Rule> findApplicableRulesPositive() {
        // TODO find applicable rules from each non-negated Concludables
        return null;
    }

    public ResourceIterator<Rule> findApplicableRulesNegative() {
        // TODO find applicable rules from negated Concludables
        return null;
    }

    public Conjunction when() {
        return when;
    }

    Conjunction then() {
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
        structure.delete();
    }

    public graql.lang.pattern.variable.ThingVariable<?> getThenPreNormalised() {
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
        then.variables().stream().filter(variable -> variable.identifier().isNamedReference())
                .forEach(thenVar ->
                        when.variables().stream()
                                .filter(whenVar -> whenVar.identifier().equals(thenVar.identifier()))
                                .filter(whenVar -> !(whenVar.isSatisfiable() && whenVar.resolvedTypes().isEmpty()))
                                .findFirst().ifPresent(whenVar -> {
                            if (thenVar.resolvedTypes().isEmpty() && thenVar.isSatisfiable()) {
                                thenVar.addResolvedTypes(whenVar.resolvedTypes());
                            } else thenVar.retainResolvedTypes(whenVar.resolvedTypes());
                            if (thenVar.resolvedTypes().isEmpty()) thenVar.setSatisfiable(false);
                        })
                );
    }

    private Set<Conclusion<?, ?>> buildConclusions(Conjunction then, Set<Variable> constraintContext) {
        HashSet<Conclusion<?, ?>> conclusions = new HashSet<>();
        then.variables().stream().flatMap(var -> var.constraints().stream()).filter(Constraint::isThing).map(Constraint::asThing)
                .map(constraint -> Conclusion.create(constraint, constraintContext)).forEach(conclusions::add);
        return conclusions;
    }

    private Conjunction whenPattern(graql.lang.pattern.Conjunction<? extends Pattern> conjunction) {
        return Conjunction.create(conjunction.normalise().patterns().get(0));
    }

    private Conjunction thenPattern(graql.lang.pattern.variable.ThingVariable<?> thenVariable) {
        Set<Variable> unnamedThenVars = VariableRegistry.createFromThings(list(thenVariable)).variables();
        return new Conjunction(nameThenVars(unnamedThenVars), set());
    }

    private Set<Variable> nameThenVars(Set<Variable> variables) {
        for (Variable variable : variables) {
            if (variable.isThing() && !variable.asThing().has().isEmpty()){
                if (variable.asThing().has().iterator().next().attribute().reference().isName()) return variables;
                return nameHasThenConcludableVars(variable.asThing());
            }
            if (variable.isThing() && !variable.asThing().relation().isEmpty())
                return nameRelationThenConcludableVars(variable.asThing());
        }
        throw GraknException.of(ILLEGAL_STATE);
    }

    private Set<Variable> nameRelationThenConcludableVars(ThingVariable oldOwner) {
        Set<Variable> namedVariables = new HashSet<>();
        RelationConstraint relationConstraint = oldOwner.relation().iterator().next();
        ThingVariable namedOwner = ThingVariable.createTemp("rel_owner");
        namedVariables.add(namedOwner);
        assert oldOwner.isa().isPresent();
        IsaConstraint isaConstraint = oldOwner.isa().get();
        TypeVariable namedType = nameType(isaConstraint.type(), "rel_type");
        namedOwner.isa(namedType, false);
        namedVariables.add(namedType);

        List<RolePlayer> namedRolePlayers = new ArrayList<>();
        assert !relationConstraint.players().isEmpty();
        int uniqueRoleID = 0;
        for (RolePlayer rolePlayer : relationConstraint.players()) {
            RolePlayer namedRolePlayer = nameRolePLayer(rolePlayer, uniqueRoleID);
            namedVariables.add(namedRolePlayer.player());
            namedVariables.add(namedRolePlayer.roleType().get());
            namedRolePlayers.add(namedRolePlayer);
            uniqueRoleID++;
        }

        namedOwner.relation(namedRolePlayers);
        namedVariables.add(namedOwner);
        return namedVariables;
    }

    private RolePlayer nameRolePLayer(RolePlayer rolePlayer, int uniqueRoleID) {
        assert rolePlayer.roleType().isPresent();
        ThingVariable playerVar;
        if (rolePlayer.roleType().get().reference().isName()) return rolePlayer;
        else playerVar = rolePlayer.player();
        TypeVariable namedRoleType = nameType(rolePlayer.roleType().get(), "role_" + uniqueRoleID);
        return new RolePlayer(namedRoleType, playerVar);
    }

    private Set<Variable> nameHasThenConcludableVars(ThingVariable oldOwner) {
        HasConstraint hasConstraint = oldOwner.has().iterator().next();
        ThingVariable attribute = hasConstraint.attribute();
        Set<Variable> namedVariables = new HashSet<>();
        ThingVariable namedAttribute = nameAttribute(attribute);
        assert namedAttribute.isa().isPresent();
        namedVariables.add(namedAttribute);
        namedVariables.add(namedAttribute.isa().get().type());
        namedVariables.add(namedAttribute.value().iterator().next().asVariable().value());
        ThingVariable namedOwner = ThingVariable.of(oldOwner.identifier());
        namedOwner.has(namedAttribute);
        namedVariables.add(namedOwner);
        return namedVariables;
    }

    private ThingVariable nameAttribute(ThingVariable attribute) {
        assert attribute.isa().isPresent();
        IsaConstraint isaConstraint = attribute.isa().get();
        TypeVariable namedType = nameType(isaConstraint.type(), "attr_type");
        assert attribute.value().size() == 1;
        ValueConstraint<?> valueConstraint = attribute.value().iterator().next();
        ThingVariable namedAttr = ThingVariable.createTemp("attr");
        ThingVariable namedValue = nameValue(valueConstraint);
        namedAttr.valueVariable(valueConstraint.predicate(), namedValue);
        namedAttr.isa(namedType, false);
        return namedAttr;
    }

    private TypeVariable nameType(TypeVariable typeVariable, String tempName) {
        if (typeVariable.reference().isName()) return typeVariable;
        TypeVariable namedType = TypeVariable.createTemp(tempName);
        assert typeVariable.label().isPresent();
        namedType.label(typeVariable.label().get().properLabel());
        return namedType;
    }

    private ThingVariable nameValue(ValueConstraint<?> valueConstraint) {
        if (valueConstraint.isVariable()) return valueConstraint.asVariable().value();
        ThingVariable namedValue = ThingVariable.createTemp("value");
        namedValue.value(valueConstraint.predicate(), valueConstraint.value());
        return namedValue;
    }

    public abstract static class Conclusion<CONSTRAINT extends Constraint, U extends Conclusion<CONSTRAINT, U>> {

        private final CONSTRAINT constraint;

        private Conclusion(CONSTRAINT constraint, Set<Variable> constraintContext) {
            this.constraint = constraint;
            copyAdditionalConstraints(constraintContext, new HashSet<>(this.constraint.variables()));
        }

        public CONSTRAINT constraint() {
            return constraint;
        }

        public static Conclusion<?, ?> create(ThingConstraint constraint, Set<Variable> constraintContext) {
            if (constraint.isRelation()) return Relation.create(constraint.asRelation(), constraintContext);
            else if (constraint.isHas()) return Has.create(constraint.asHas(), constraintContext);
            else if (constraint.isIsa()) return Isa.create(constraint.asIsa(), constraintContext);
            else if (constraint.isValue()) return Value.create(constraint.asValue(), constraintContext);
            else throw GraknException.of(ILLEGAL_STATE);
        }

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
                    .filter(variable -> !variable.identifier().reference().isAnonymous())
                    .collect(Collectors.toMap(e -> e, e -> e)); // Create a map for efficient lookups
            toVars.stream().filter(variable -> !variable.identifier().reference().isAnonymous())
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

        public static class Relation extends Conclusion<RelationConstraint, Conclusion.Relation> {

            public Relation(RelationConstraint constraint, Set<Variable> constraintContext) {
                super(constraint, constraintContext);
            }


            public static Relation create(RelationConstraint constraint, Set<Variable> constraintContext) {
                return new Relation(ConstraintCopier.copyConstraint(constraint), constraintContext);
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

        public static class Has extends Conclusion<HasConstraint, Conclusion.Has> {

            public Has(HasConstraint constraint, Set<Variable> constraintContext) {
                super(constraint, constraintContext);
            }

            public static Has create(HasConstraint constraint, Set<Variable> constraintContext) {
                return new Has(ConstraintCopier.copyConstraint(constraint), constraintContext);
            }

            @Override
            public boolean isHas() {
                return true;
            }

            @Override
            public Has asHas() {
                return this;
            }
        }

        public static class Isa extends Conclusion<IsaConstraint, Conclusion.Isa> {

            public Isa(IsaConstraint constraint, Set<Variable> constraintContext) {
                super(constraint, constraintContext);
            }

            public static Isa create(IsaConstraint constraint, Set<Variable> constraintContext) {
                return new Isa(ConstraintCopier.copyConstraint(constraint), constraintContext);
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

        public static class Value extends Conclusion<ValueConstraint<?>, Conclusion.Value> {

            Value(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
                super(constraint, constraintContext);
            }

            public static Value create(ValueConstraint<?> constraint, Set<Variable> constraintContext) {
                return new Value(ConstraintCopier.copyConstraint(constraint), constraintContext);
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
