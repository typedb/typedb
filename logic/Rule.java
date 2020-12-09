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

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.RelationType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.graph.util.Encoding;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.logic.concludable.ThenConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Negation;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.common.exception.GraqlException;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.TYPES_NOT_FOUND;


public class Rule {

    private final ConceptManager conceptMgr;
    private final LogicManager logicManager;
    private final RuleStructure structure;
    private final Conjunction when;
    private final Conjunction then;
    private final Set<ThenConcludable<?, ?>> possibleThenConcludables;
    private final Set<ConjunctionConcludable<?, ?>> requiredWhenConcludables;

    private Rule(ConceptManager conceptMgr, LogicManager logicManager, RuleStructure structure) {
        this.conceptMgr = conceptMgr;
        this.logicManager = logicManager;
        this.structure = structure;
        this.when = logicManager.typeHinter().computeHintsExhaustive(whenPattern(structure.when()));
        this.then = logicManager.typeHinter().computeHintsExhaustive(thenPattern(structure.then()));
        pruneThenTypeHints();
        this.possibleThenConcludables = buildThenConcludables(this.then, this.when.variables());
        this.requiredWhenConcludables = ConjunctionConcludable.create(this.when);
    }

    private Rule(GraphManager graphMgr, ConceptManager conceptMgr, LogicManager logicManager, String label,
                 graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        graql.lang.pattern.schema.Rule.validate(label, when, then);
        this.conceptMgr = conceptMgr;
        this.logicManager = logicManager;
        this.structure = graphMgr.schema().create(label, when, then);

        Conjunction whenConjunction = whenPattern(structure.when());
        Conjunction thenConjunction = thenPattern(structure.then());
        validateLabelsExist(whenConjunction, thenConjunction);

        this.when = logicManager.typeHinter().computeHintsExhaustive(whenConjunction);
        this.then = logicManager.typeHinter().computeHintsExhaustive(thenConjunction);
        validateRuleSatisfiable();
        pruneThenTypeHints();

        this.possibleThenConcludables = buildThenConcludables(this.then, this.when.variables());
        this.requiredWhenConcludables = ConjunctionConcludable.create(this.when);
    }

    public static Rule of(ConceptManager conceptMgr, LogicManager logicManager, RuleStructure structure) {
        return new Rule(conceptMgr, logicManager, structure);
    }

    public static Rule of(GraphManager graphMgr, ConceptManager conceptMgr, LogicManager logicManager, String label,
                          graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        return new Rule(graphMgr, conceptMgr, logicManager, label, when, then);
    }

    public Set<ConjunctionConcludable<?, ?>> body() {
        return requiredWhenConcludables;
    }

    public Set<ThenConcludable<?, ?>> head() {
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

    boolean isCommitted() {
        return structure.status().equals(Encoding.Status.COMMITTED);
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

    private Conjunction thenPattern(ThingVariable<?> thenVariable) {
        return new Conjunction(VariableRegistry.createFromThings(list(thenVariable)).variables(), set());
    }

    private void validateRuleSatisfiable() {
        // TODO check that the rule has a set of satisfiable type hints. We may want to use the stream of combinations of types
        // TODO instead of the collapsed type hints on the `isa` and `sub` constraints
    }

    private void validateLabelsExist(Conjunction whenConjunction, Conjunction thenConjunction) {
        Stream<Label> whenPositiveLabels = getTypeLabels(whenConjunction.variables().stream());
        Stream<Label> whenNegativeLabels = getTypeLabels(whenConjunction.negations().stream().flatMap(this::negationVariables));
        Stream<Label> thenLabels = getTypeLabels(thenConjunction.variables().stream());
        Set<String> invalidLabels = invalidLabels(Stream.of(whenPositiveLabels, whenNegativeLabels, thenLabels).flatMap(Function.identity()));
        if (!invalidLabels.isEmpty()) {
            throw GraqlException.of(TYPES_NOT_FOUND.message(getLabel(), String.join(", ", invalidLabels)));
        }
    }

    private Stream<Label> getTypeLabels(Stream<Variable> variables) {
        return variables.filter(Variable::isType).map(variable -> variable.asType().label())
                .filter(Optional::isPresent).map(labelConstraint -> labelConstraint.get().properLabel());
    }

    private Stream<Variable> negationVariables(Negation ruleNegation) {
        assert ruleNegation.disjunction().conjunctions().size() == 1;
        return ruleNegation.disjunction().conjunctions().iterator().next().variables().stream();
    }

    private Set<String> invalidLabels(Stream<Label> labels) {
        return labels.filter(label -> {
            if (label.scope().isPresent()) {
                RelationType scope = conceptMgr.getRelationType(label.scope().get());
                if (scope == null) return false;
                return scope.getRelates(label.name()) == null;
            } else {
                return conceptMgr.getType(label.name()) == null;
            }
        }).map(Label::scopedName).collect(Collectors.toSet());
    }
}
