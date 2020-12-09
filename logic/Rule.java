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
import grakn.core.common.parameters.Label;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.RelationType;
import grakn.core.graph.GraphManager;
import grakn.core.graph.structure.RuleStructure;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.logic.concludable.HeadConcludable;
import grakn.core.logic.tool.TypeHinter;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Negation;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.RuleWrite.TYPES_NOT_FOUND;


public class Rule {

    private final ConceptManager conceptMgr;
    private final RuleStructure structure;
    private final Conjunction when;
    private final Set<Constraint> then;
    private final Set<Label> positiveWhenTypeHints;
    private final Set<Label> negativeWhenTypeHints;
    private final Set<Label> positiveThenTypeHints;
    private final Set<HeadConcludable<?, ?>> head;
    private final Set<ConjunctionConcludable<?, ?>> body;

    private Rule(ConceptManager conceptMgr, RuleStructure structure, TypeHinter typeHinter) {
        this.conceptMgr = conceptMgr;
        this.structure = structure;
        // TODO we should merge `when` and `then`, then compute type hints, then copy them into the `then`
        // TODO re-enable type hints once traversal engine is completed
//        this.when = typeHinter.computeHintsExhaustive(whenPattern(structure.when()));
        this.when = whenPattern(structure.when());
        this.then = thenConstraints(getThenPreNormalised());
        positiveWhenTypeHints = positiveTypeHints(this.when);
        negativeWhenTypeHints = negativeTypeHints(this.when);
        positiveThenTypeHints = positiveTypeHints(this.then);

        this.head = createHead(this.then, this.when.variables());
        this.body = ConjunctionConcludable.of(this.when);
    }

    private Rule(GraphManager graphMgr, ConceptManager conceptMgr, TypeHinter typeHinter, String label,
                 graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        graql.lang.pattern.schema.Rule.validate(label, when, then);
        this.conceptMgr = conceptMgr;
        this.structure = graphMgr.schema().create(label, when, then);
        // TODO we should merge `when` and `then`, then compute type hints, then copy them into the `then`
        // TODO re-enable type hints once traversal engine is completed
//        this.when = typeHinter.computeHintsExhaustive(whenPattern(structure.when()));
        this.when = whenPattern(structure.when());
        this.then = thenConstraints(structure.then());
        validateLabelsExist();

        positiveWhenTypeHints = positiveTypeHints(this.when);
        negativeWhenTypeHints = negativeTypeHints(this.when);
        positiveThenTypeHints = positiveTypeHints(this.then);
        validateRuleSatisfiable();

        this.head = createHead(this.then, this.when.variables());
        this.body = ConjunctionConcludable.of(this.when);
    }

    private Set<Label> positiveTypeHints(Conjunction conjunction) {
        // TODO
        return null;
    }

    private Set<Label> negativeTypeHints(Conjunction conjunction) {
        // TODO
        return null;
    }

    private Set<Label> positiveTypeHints(Set<Constraint> constraints) {
        // TODO
        return null;
    }

    public static Rule of(ConceptManager conceptMgr, RuleStructure structure, TypeHinter typeHinter) {
        return new Rule(conceptMgr, structure, typeHinter);
    }

    public static Rule of(ConceptManager conceptMgr, GraphManager graphMgr, TypeHinter typeHinter, String label,
                          graql.lang.pattern.Conjunction<? extends Pattern> when, ThingVariable<?> then) {
        return new Rule(graphMgr, conceptMgr, typeHinter, label, when, then);
    }

    public Set<ConjunctionConcludable<?, ?>> body() {
        return body;
    }

    public Set<HeadConcludable<?, ?>> head() {
        return head;
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


    private Set<HeadConcludable<?, ?>> createHead(Set<Constraint> thenConstraints, Set<Variable> constraintContext) {
        HashSet<HeadConcludable<?, ?>> thenConcludables = new HashSet<>();
        thenConstraints.stream().filter(Constraint::isThing).map(Constraint::asThing)
                .flatMap(constraint -> HeadConcludable.of(constraint, constraintContext).stream()).forEach(thenConcludables::add);
        return thenConcludables;
    }

    private Conjunction whenPattern(graql.lang.pattern.Conjunction<? extends Pattern> conjunction) {
        return Conjunction.create(conjunction.normalise().patterns().get(0));
    }

    private Set<Constraint> thenConstraints(ThingVariable<?> thenVariable) {
        return VariableRegistry.createFromThings(list(thenVariable)).variables().stream().flatMap(
                variable -> variable.constraints().stream()).collect(Collectors.toSet());
    }

    private void validateRuleSatisfiable() {
        // TODO check that the rule has a set of satisfiable type hints. We may want to use the stream of combinations of types
        // TODO instead of the collapsed type hints on the `isa` and `sub` constraints
    }

    private void validateLabelsExist() {
        Stream<Label> whenPositiveLabels = getTypeLabels(when.variables().stream());
        Stream<Label> whenNegativeLabels = getTypeLabels(when.negations().stream().flatMap(this::negationVariables));
        Stream<Label> thenLabels = getTypeLabels(then.stream().flatMap(constraint -> constraint.variables().stream()));
        Set<String> invalidLabels = invalidLabels(Stream.of(whenPositiveLabels, whenNegativeLabels, thenLabels).flatMap(Function.identity()));
        if (!invalidLabels.isEmpty()) {
            throw GraknException.of(TYPES_NOT_FOUND, getLabel(), String.join(", ", invalidLabels));
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
