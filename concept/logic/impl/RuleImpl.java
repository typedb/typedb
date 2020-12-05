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

package grakn.core.concept.logic.impl;

import grakn.core.concept.logic.Rule;
import grakn.core.graph.GraphManager;
import grakn.core.graph.logic.RuleLogic;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Negation;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.common.exception.GraqlException;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;


public class RuleImpl implements Rule {

    private final GraphManager graphMgr;
    private final RuleLogic logic;
    private Conjunction when;
    private Set<Constraint> then;

    private RuleImpl(GraphManager graphMgr, RuleLogic logic) {
        this.graphMgr = graphMgr;
        this.logic = logic;
        when = Conjunction.create(getWhenPreNormalised().normalise().patterns().get(0));
        then = VariableRegistry.createFromThings(list(getThenPreNormalised())).variables().stream().flatMap(
                variable -> variable.constraints().stream()).collect(Collectors.toSet());
    }

    public static RuleImpl of(GraphManager graphMgr, RuleLogic logic) {
        return new RuleImpl(graphMgr, logic);
    }

    public static RuleImpl of(GraphManager graphMgr, String label,
                              graql.lang.pattern.Conjunction<? extends graql.lang.pattern.Pattern> when,
                              graql.lang.pattern.variable.ThingVariable<?> then) {
        graql.lang.pattern.schema.Rule.validate(label, when, then);
        RuleLogic logic = graphMgr.schema().create(label, when, then);
        RuleImpl rule = new RuleImpl(graphMgr, logic);
        rule.validateLabelsExist();
        return rule;
    }

    @Override
    public String getLabel() {
        return logic.label();
    }

    @Override
    public void setLabel(String label) {
        logic.label(label);
    }

    @Override
    public graql.lang.pattern.Conjunction<? extends graql.lang.pattern.Pattern> getWhenPreNormalised() {
        return logic.when();
    }

    @Override
    public graql.lang.pattern.variable.ThingVariable<?> getThenPreNormalised() {
        return logic.then();
    }

    @Override
    public Conjunction when() {
        return when;
    }

    @Override
    public Set<Constraint> then() {
        return then;
    }

    @Override
    public boolean isDeleted() {
        return logic.isDeleted();
    }

    @Override
    public void delete() {
        logic.delete();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        final RuleImpl that = (RuleImpl) object;
        return this.logic.equals(that.logic);
    }

    @Override
    public final int hashCode() {
        return logic.hashCode(); // does not need caching
    }

    private void validateLabelsExist() {
        Stream<String> whenPositiveLabels = getTypeLabels(when.variables().stream());
        Stream<String> whenNegativeLabels = getTypeLabels(when.negations().stream().flatMap(this::negationVariables));
        Stream<String> thenLabels = getTypeLabels(then.stream().flatMap(constraint -> constraint.variables().stream()));
        Set<String> missingLabels = Stream.of(whenPositiveLabels, whenNegativeLabels, thenLabels).flatMap(Function.identity())
                .filter(label -> graphMgr.schema().getType(label) == null).collect(Collectors.toSet());
        if (!missingLabels.isEmpty()) {
            throw GraqlException.of(TYPE_NOT_FOUND.message(getLabel(), String.join(", ", missingLabels)));
        }
    }

    private Stream<String> getTypeLabels(Stream<Variable> variables) {
        return variables.filter(Variable::isType).map(variable -> variable.asType().label())
                .filter(Optional::isPresent).map(labelConstraint -> labelConstraint.get().scopedLabel());
    }

    private Stream<Variable> negationVariables(Negation ruleNegation) {
        assert ruleNegation.disjunction().conjunctions().size() == 1;
        return ruleNegation.disjunction().conjunctions().iterator().next().variables().stream();
    }
}
