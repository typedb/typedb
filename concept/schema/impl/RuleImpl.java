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

package grakn.core.concept.schema.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.schema.Rule;
import grakn.core.concept.type.impl.RelationTypeImpl;
import grakn.core.concept.type.impl.RoleTypeImpl;
import grakn.core.concept.type.impl.TypeImpl;
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.RuleVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Negation;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.type.LabelConstraint;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import graql.lang.common.exception.GraqlException;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_CONSTRAINT_TYPE_VARIABLE;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ISA_MANY;
import static grakn.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static grakn.core.graph.util.Encoding.Edge.Rule.CONCLUSION;
import static grakn.core.graph.util.Encoding.Edge.Rule.CONDITION_NEGATIVE;
import static grakn.core.graph.util.Encoding.Edge.Rule.CONDITION_POSITIVE;

public class RuleImpl implements Rule {

    private final GraphManager graphMgr;
    private final RuleVertex vertex;
    private Conjunction when;
    private Set<Variable> then;

    private RuleImpl(final GraphManager graphMgr, final RuleVertex vertex) {
        this.graphMgr = graphMgr;
        this.vertex = vertex;
    }

    private RuleImpl(final GraphManager graphMgr, final String label,
                     final graql.lang.pattern.Conjunction<? extends graql.lang.pattern.Pattern> when,
                     final graql.lang.pattern.variable.ThingVariable<?> then) {
        graql.lang.pattern.schema.Rule.validate(label, when, then);
        this.graphMgr = graphMgr;
        this.vertex = graphMgr.schema().create(label, when, then);
        putPositiveConditions();
        putNegativeConditions();
        putConclusions();
        validateLabelsExist();
    }

    public static RuleImpl of(final GraphManager graphMgr, final RuleVertex vertex) {
        return new RuleImpl(graphMgr, vertex);
    }

    public static RuleImpl of(final GraphManager graphMgr, final String label,
                              final graql.lang.pattern.Conjunction<? extends graql.lang.pattern.Pattern> when,
                              final graql.lang.pattern.variable.ThingVariable<?> then) {
        return new RuleImpl(graphMgr, label, when, then);
    }

    private void putPositiveConditions() {
        vertex.outs().delete(CONDITION_POSITIVE);
        when().variables().stream().flatMap(v -> v.constraints().stream())
                .filter(constraint -> constraint.isType() && constraint.asType().isLabel())
                .forEach(constraint -> putCondition(constraint.asType().asLabel(), CONDITION_POSITIVE));
    }

    private void putNegativeConditions() {
        vertex.outs().delete(CONDITION_NEGATIVE);
        when().negations().stream()
                .flatMap(this::negationVariables).flatMap(variable -> variable.constraints().stream())
                .filter(constraint -> constraint.isType() && constraint.asType().isLabel())
                .forEach(constraint -> putCondition(constraint.asType().asLabel(), CONDITION_NEGATIVE));
    }

    private void putCondition(final LabelConstraint constraint, final Encoding.Edge.Rule encoding) {
        if (constraint.scope().isPresent()) {
            TypeVertex relation = graphMgr.schema().getType(constraint.scope().get());
            TypeVertex role = graphMgr.schema().getType(constraint.label(), constraint.scope().get());
            if (role == null) throw GraknException.of(TYPE_NOT_FOUND.message(constraint.scopedLabel()));
            vertex.outs().put(encoding, relation);
            vertex.outs().put(encoding, role);
        } else {
            TypeVertex type = graphMgr.schema().getType(constraint.label());
            if (type == null) throw GraknException.of(TYPE_NOT_FOUND.message(constraint.label()));
            vertex.outs().put(encoding, type);
        }
    }

    private void putConclusions() {
        vertex.outs().delete(CONCLUSION);
        // TODO: @flyingsilverfin to revise this logic. It's a big wonky.
        //       First you filter out TypeConstraints, then you retrieved it again via constraint.asIsa().type()
        then().stream().flatMap(var -> var.constraints().stream())
                .filter(Constraint::isThing).map(Constraint::asThing).forEach(constraint -> {
            Set<IsaConstraint> isaConstraints;
            if (constraint.isHas() && !(isaConstraints = constraint.asHas().attribute().isa()).isEmpty()) {
                if (isaConstraints.size() > 1) throw GraknException.of(THING_ISA_MANY);
                else putConclusion(isaConstraints.iterator().next().type().label().orElseThrow(
                        () -> GraknException.of(THING_CONSTRAINT_TYPE_VARIABLE)
                ));
            } else if (constraint.isIsa()) putConclusion(constraint.asIsa().type().label().orElseThrow(
                    () -> GraknException.of(THING_CONSTRAINT_TYPE_VARIABLE)
            ));
            else if (constraint.isRelation()) putRelationConclusion(constraint.asRelation());
        });
    }

    private void putConclusion(LabelConstraint labelConstraint) {
        TypeVertex type = graphMgr.schema().getType(labelConstraint.label());
        if (type == null) throw GraknException.of(TYPE_NOT_FOUND.message(type));
        vertex.outs().put(CONCLUSION, type);
    }

    private void putRelationConclusion(RelationConstraint relation) {
        String relationLabel = relation.owner().isa().iterator().next().type().label().get().label();
        TypeVertex relationVertex = graphMgr.schema().getType(relationLabel);
        if (relationVertex == null) throw GraknException.of(TYPE_NOT_FOUND.message(relationLabel));
        RelationTypeImpl relationConcept = RelationTypeImpl.of(graphMgr, relationVertex);
        relation.asRelation().players().forEach(player -> {
            if (player.roleType().isPresent() && player.roleType().get().label().isPresent()) {
                final String roleLabel = player.roleType().get().label().get().label();
                RoleTypeImpl role = relationConcept.getRelates(roleLabel);
                if (role == null)
                    throw GraknException.of(TYPE_NOT_FOUND.message(player.roleType().get().label().get().scopedLabel()));
                vertex.outs().put(CONCLUSION, role.vertex);
            }
        });
    }

    @Override
    public String getLabel() {
        return vertex.label();
    }

    @Override
    public void setLabel(final String label) {
        vertex.label(label);
    }

    @Override
    public graql.lang.pattern.Conjunction<? extends graql.lang.pattern.Pattern> getWhenPreNormalised() {
        return vertex.when();
    }

    @Override
    public graql.lang.pattern.variable.ThingVariable<?> getThenPreNormalised() {
        return vertex.then();
    }

    @Override
    public Conjunction when() {
        if (when == null) {
            when = Conjunction.create(getWhenPreNormalised().normalise().patterns().get(0));
        }
        return when;
    }

    @Override
    public Set<Variable> then() {
        if (then == null) {
            then = VariableRegistry.createFromThings(list(getThenPreNormalised())).variables();
        }
        return then;
    }

    private void validateLabelsExist() {
        Stream<String> whenPositiveLabels = getTypeLabels(when.variables().stream());
        Stream<String> whenNegativeLabels = getTypeLabels(when.negations().stream().flatMap(this::negationVariables));
        Stream<String> thenLabels = getTypeLabels(then.stream());
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

    @Override
    public Stream<TypeImpl> positiveConditionTypes() {
        return vertex.outs().edge(CONDITION_POSITIVE).to().map(v -> TypeImpl.of(graphMgr, v)).stream();
    }

    @Override
    public Stream<TypeImpl> negativeConditionTypes() {
        return vertex.outs().edge(CONDITION_NEGATIVE).to().map(v -> TypeImpl.of(graphMgr, v)).stream();
    }

    @Override
    public Stream<TypeImpl> conclusionTypes() {
        return vertex.outs().edge(CONCLUSION).to().map(v -> TypeImpl.of(graphMgr, v)).stream();
    }

    @Override
    public boolean isDeleted() {
        return vertex.isDeleted();
    }

    @Override
    public void delete() {
        vertex.delete();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        final RuleImpl that = (RuleImpl) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode(); // does not need caching
    }
}
