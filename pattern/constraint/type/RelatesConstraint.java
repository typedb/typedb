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

package grakn.core.pattern.constraint.type;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableCloner;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.TypeRead.OVERRIDDEN_TYPES_IN_TRAVERSAL;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.AS;
import static graql.lang.common.GraqlToken.Constraint.RELATES;

public class RelatesConstraint extends TypeConstraint {

    private final TypeVariable roleType;
    private final TypeVariable overriddenRoleType;
    private final int hash;

    public RelatesConstraint(TypeVariable owner, TypeVariable roleType, @Nullable TypeVariable overriddenRoleType) {
        super(owner, roleTypes(roleType, overriddenRoleType));
        if (roleType == null) throw new NullPointerException("Null role");
        assert roleType.reference().isName() ||
                (roleType.label().isPresent() && roleType.label().get().scope().isPresent());
        assert overriddenRoleType == null || overriddenRoleType.reference().isName() ||
                (overriddenRoleType.label().isPresent() && overriddenRoleType.label().get().scope().isPresent());
        this.roleType = roleType;
        this.overriddenRoleType = overriddenRoleType;
        this.hash = Objects.hash(RelatesConstraint.class, this.owner, this.roleType, this.overriddenRoleType);
        roleType.constraining(this);
        if (overriddenRoleType != null) overriddenRoleType.constraining(this);
    }

    static RelatesConstraint of(TypeVariable owner, graql.lang.pattern.constraint.TypeConstraint.Relates constraint,
                                VariableRegistry registry) {
        TypeVariable roleType = registry.register(constraint.role());
        TypeVariable overriddenRoleType = constraint.overridden().map(registry::register).orElse(null);
        return new RelatesConstraint(owner, roleType, overriddenRoleType);
    }

    static RelatesConstraint of(TypeVariable owner, RelatesConstraint constraint, VariableCloner cloner) {
        TypeVariable roleType = cloner.clone(constraint.role());
        TypeVariable overriddenRoleType = constraint.overridden().map(cloner::clone).orElse(null);
        return new RelatesConstraint(owner, roleType, overriddenRoleType);
    }

    public TypeVariable role() {
        return roleType;
    }

    public Optional<TypeVariable> overridden() {
        return Optional.ofNullable(overriddenRoleType);
    }

    @Override
    public void addTo(Traversal traversal) {
        if (overridden().isPresent()) throw GraknException.of(OVERRIDDEN_TYPES_IN_TRAVERSAL);
        traversal.relates(owner.id(), roleType.id());
    }

    @Override
    public boolean isRelates() {
        return true;
    }

    @Override
    public RelatesConstraint asRelates() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelatesConstraint that = (RelatesConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.roleType.equals(that.roleType) &&
                Objects.equals(this.overriddenRoleType, that.overriddenRoleType));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder syntax = new StringBuilder();
        syntax.append(RELATES).append(SPACE).append(roleType.referenceSyntax());
        if (overriddenRoleType != null)
            syntax.append(SPACE).append(AS).append(SPACE).append(overriddenRoleType.referenceSyntax());
        return syntax.toString();
    }

    @Override
    public RelatesConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).relates(
                cloner.cloneVariable(roleType),
                overriddenRoleType == null ? null : cloner.cloneVariable(overriddenRoleType)
        );
    }

    private static Set<TypeVariable> roleTypes(TypeVariable roleType, TypeVariable overriddenRoleType) {
        return overriddenRoleType == null ? set(roleType) : set(roleType, overriddenRoleType);
    }
}
