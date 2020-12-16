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
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.logic.concludable.ThenConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint.RolePlayer;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.pattern.variable.SystemReference;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.Pattern;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.logic.LogicManager.validateRuleStructureLabels;


public class Rule {

    private final LogicManager logicManager;
    private final RuleStructure structure;
    private final Conjunction when;
    private final Conjunction then;
    private final Set<ThenConcludable<?, ?>> possibleThenConcludables;
    private final Set<ConjunctionConcludable<?, ?>> requiredWhenConcludables;

    private Rule(LogicManager logicManager, RuleStructure structure) {
        this.logicManager = logicManager;
        this.structure = structure;
        // TODO enable when we have type hinting
//        this.when = logicManager.typeHinter().computeHintsExhaustive(whenPattern(structure.when()));
//        this.then = logicManager.typeHinter().computeHintsExhaustive(thenPattern(structure.then()));
        this.when = whenPattern(structure.when());
        this.then = thenPattern(structure.then());
        pruneThenTypeHints();
        this.possibleThenConcludables = buildThenConcludables(this.then, this.when.variables());
        this.requiredWhenConcludables = ConjunctionConcludable.create(this.when);
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

        this.possibleThenConcludables = buildThenConcludables(this.then, this.when.variables());
        this.requiredWhenConcludables = ConjunctionConcludable.create(this.when);
        validateCycles();
    }

    public static Rule of(LogicManager logicManager, RuleStructure structure) {
        return new Rule(logicManager, structure);
    }

    public static Rule of(GraphManager graphMgr, ConceptManager conceptMgr, LogicManager logicManager, String label,
                          graql.lang.pattern.Conjunction<? extends Pattern> when, graql.lang.pattern.variable.ThingVariable<?> then) {
        return new Rule(graphMgr, conceptMgr, logicManager, label, when, then);
    }

    public Set<ConjunctionConcludable<?, ?>> whenConcludables() {
        return requiredWhenConcludables;
    }

    public Set<ThenConcludable<?, ?>> possibleThenConcludables() {
        return possibleThenConcludables;
    }

    public ResourceIterator<Rule> findApplicableRulesPositive() {
        // TODO find applicable rules from each non-negated ConjunctionConcludables
        return null;
    }

    public ResourceIterator<Rule> findApplicableRulesNegative() {
        // TODO find applicable rules from negated ConjunctionConcludables
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
        then.variables().stream().filter(var -> var.identifier().isNamedReference())
                .forEach(thenVar -> {
                    Optional<Variable> whenVar = when.variables().stream().filter(var -> var.identifier().equals(thenVar.identifier())).findFirst();
                    if (whenVar.isPresent() && whenVar.get().isThing()) {
                        assert thenVar.isThing();
                        whenVar.get().asThing().isa().ifPresent(whenIsa -> thenVar.asThing().isa().ifPresent(
                                thenIsa -> thenIsa.retainHints(whenIsa.getTypeHints())));
                    } else if (whenVar.isPresent() && whenVar.get().isType()) {
                        assert thenVar.isType();
                        whenVar.get().asType().sub().ifPresent(whenSub -> thenVar.asType().sub().ifPresent(
                                thenSub -> thenSub.retainHints(whenSub.getTypeHints())));
                    }
                });
    }

    private Set<ThenConcludable<?, ?>> buildThenConcludables(Conjunction then, Set<Variable> constraintContext) {
        HashSet<ThenConcludable<?, ?>> thenConcludables = new HashSet<>();
        then.variables().stream().flatMap(var -> var.constraints().stream()).filter(Constraint::isThing).map(Constraint::asThing)
                .flatMap(constraint -> ThenConcludable.of(constraint, constraintContext).stream()).forEach(thenConcludables::add);
        return thenConcludables;
    }

    private Conjunction whenPattern(graql.lang.pattern.Conjunction<? extends Pattern> conjunction) {
        return Conjunction.create(conjunction.normalise().patterns().get(0));
    }

    private Conjunction thenPattern(graql.lang.pattern.variable.ThingVariable<?> thenVariable) {
        Set<Variable> rawThenVars = VariableRegistry.createFromThings(list(thenVariable)).variables();
        return new Conjunction(nameThenVars(rawThenVars), set());
    }

    private Set<Variable> nameThenVars(Set<Variable> variables) {
        for (Variable variable : variables) {
            if (variable.isThing() && !variable.asThing().has().isEmpty()){
                if (variable.asThing().has().iterator().next().attribute().reference().isName()) return variables;
                return nameHasVar(variable.asThing());
            }
            if (variable.isThing() && !variable.asThing().relation().isEmpty())
                return nameRelationVar(variable.asThing());
        }
        throw GraknException.of(ILLEGAL_STATE);
    }

    private Set<Variable> nameRelationVar(ThingVariable relVariable) {
        Set<Variable> register = new HashSet<>();
        RelationConstraint relationConstraint = relVariable.relation().iterator().next();
        ThingVariable namedOwner = ThingVariable.of(Identifier.Variable.of(new SystemReference("rel_owner")));
        register.add(namedOwner);
        assert relVariable.isa().isPresent();
        IsaConstraint isaConstraint = relVariable.isa().get();
        TypeVariable namedType = nameType(isaConstraint.type(), "rel_type");
        namedOwner.isa(namedType, false);
        register.add(namedType);

        List<RolePlayer> namedRolePlayers = new ArrayList<>();
        assert !relationConstraint.players().isEmpty();
        int uniqueRoleID = 0;
        for (RolePlayer rolePlayer : relationConstraint.players()) {
            RolePlayer namedRolePlayer = nameRolePLayer(rolePlayer, uniqueRoleID);
            register.add(namedRolePlayer.player());
            register.add(namedRolePlayer.roleType().get());
            namedRolePlayers.add(namedRolePlayer);
            uniqueRoleID++;
        }

        namedOwner.relation(namedRolePlayers);
        register.add(namedOwner);
        return register;
    }

    private RolePlayer nameRolePLayer(RolePlayer rolePlayer, int uniqueRoleID) {
        assert rolePlayer.roleType().isPresent();
        ThingVariable playerVar;
        if (rolePlayer.roleType().get().reference().isName()) return rolePlayer;
        else playerVar = rolePlayer.player();
        TypeVariable namedRoleType = nameType(rolePlayer.roleType().get(), "role_" + uniqueRoleID);
        return new RolePlayer(namedRoleType, playerVar);
    }

    private Set<Variable> nameHasVar(ThingVariable hasVariable) {
        HasConstraint hasConstraint = hasVariable.has().iterator().next();
        ThingVariable attribute = hasConstraint.attribute();
        Set<Variable> register = new HashSet<>();
        ThingVariable namedAttribute = nameAttribute(attribute);
        assert namedAttribute.isa().isPresent();
        register.add(namedAttribute);
        register.add(namedAttribute.isa().get().type());
        register.add(namedAttribute.value().iterator().next().asVariable().value());
        ThingVariable newOwner = ThingVariable.of(hasVariable.identifier());
        newOwner.has(namedAttribute);
        register.add(newOwner);
        return register;
    }

    private ThingVariable nameAttribute(ThingVariable attribute) {
        assert attribute.isa().isPresent();
        IsaConstraint isaConstraint = attribute.isa().get();
        TypeVariable namedType = nameType(isaConstraint.type(), "attr_type");
        assert attribute.value().size() == 1;
        ValueConstraint<?> valueConstraint = attribute.value().iterator().next();
        ThingVariable newAttr = ThingVariable.of(Identifier.Variable.of(new SystemReference("attr")));
        ThingVariable namedValue = nameValue(valueConstraint);
        newAttr.valueVariable(valueConstraint.predicate(), namedValue);
        newAttr.isa(namedType, false);
        return newAttr;
    }

    private TypeVariable nameType(TypeVariable typeVariable, String tempName) {
        if (typeVariable.reference().isName()) return typeVariable;
        TypeVariable namedType = TypeVariable.of(Identifier.Variable.of(new SystemReference(tempName)));
        assert typeVariable.label().isPresent();
        namedType.label(typeVariable.label().get().properLabel());
        return namedType;
    }

    private ThingVariable nameValue(ValueConstraint<?> valueConstraint) {
        if (valueConstraint.isVariable()) return valueConstraint.asVariable().value();
        ThingVariable newValue = ThingVariable.of(Identifier.Variable.of(new SystemReference("value")));
        newValue.value(valueConstraint.predicate(), valueConstraint.value());
        return newValue;
    }

}
