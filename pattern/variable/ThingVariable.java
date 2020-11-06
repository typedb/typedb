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

package grakn.core.pattern.variable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.constraint.thing.HasConstraint;
import grakn.core.pattern.constraint.thing.IIDConstraint;
import grakn.core.pattern.constraint.thing.IsConstraint;
import grakn.core.pattern.constraint.thing.IsaConstraint;
import grakn.core.pattern.constraint.thing.RelationConstraint;
import grakn.core.pattern.constraint.thing.ThingConstraint;
import grakn.core.pattern.constraint.thing.ValueConstraint;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlToken;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static java.util.stream.Collectors.toSet;

public class ThingVariable extends Variable {

    private IIDConstraint iidConstraint;
    private final Map<GraqlToken.Comparator, List<ValueConstraint<?>>> valueConstraints;
    private final Set<IsaConstraint> isaConstraints;
    private final Set<IsConstraint> isConstraints;
    private final Set<RelationConstraint> relationConstraints;
    private final Set<HasConstraint> hasConstraints;
    private final Set<ThingConstraint> constraints;

    ThingVariable(final Identifier.Variable identifier) {
        super(identifier);
        this.isaConstraints = new HashSet<>();
        this.isConstraints = new HashSet<>();
        this.valueConstraints = new HashMap<>();
        this.relationConstraints = new HashSet<>();
        this.hasConstraints = new HashSet<>();
        this.constraints = new HashSet<>();
    }

    ThingVariable constrainThing(final List<graql.lang.pattern.constraint.ThingConstraint> constraints,
                                 final VariableRegistry registry) {
        constraints.forEach(constraint -> this.constrain(ThingConstraint.of(this, constraint, registry)));
        return this;
    }

    ThingVariable constrainConcept(final List<graql.lang.pattern.constraint.ConceptConstraint> constraints,
                                   final VariableRegistry registry) {
        constraints.forEach(constraint -> this.constrain(ThingConstraint.of(this, constraint, registry)));
        return this;
    }

    private void constrain(final ThingConstraint constraint) {
        constraints.add(constraint);
        if (constraint.isIID()) iidConstraint = constraint.asIID();
        else if (constraint.isIsa()) isaConstraints.add(constraint.asIsa());
        else if (constraint.isIs()) isConstraints.add(constraint.asIs());
        else if (constraint.isRelation()) relationConstraints.add(constraint.asRelation());
        else if (constraint.isHas()) hasConstraints.add(constraint.asHas());
        else if (constraint.isValue()) valueConstraints.computeIfAbsent(
                constraint.asValue().comparator(), c -> new ArrayList<>()
        ).add(constraint.asValue());
        else throw GraknException.of(ILLEGAL_STATE);
    }

    public Optional<IIDConstraint> iid() {
        return Optional.ofNullable(iidConstraint);
    }

    public Set<IsaConstraint> isa() {
        return isaConstraints;
    }

    public Set<IsConstraint> is() {
        return isConstraints;
    }

    public Set<ValueConstraint<?>> value() {
        return valueConstraints.values().stream().flatMap(Collection::stream).collect(toSet());
    }

    public Set<RelationConstraint> relation() {
        return relationConstraints;
    }

    public Set<HasConstraint> has() {
        return hasConstraints;
    }

    @Override
    public Set<ThingConstraint> constraints() {
        return constraints;
    }

    @Override
    public boolean isThing() {
        return true;
    }

    @Override
    public ThingVariable asThing() {
        return this;
    }
}
