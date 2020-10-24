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

package grakn.core.pattern.constraint.type;

import grakn.core.pattern.variable.TypeVariable;
import grakn.core.traversal.Traversal;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static grakn.common.collection.Collections.list;

public class LabelConstraint extends TypeConstraint {

    private final String label;
    private final String scope;
    private final int hash;
    private List<Traversal> traversals;

    private LabelConstraint(final TypeVariable owner, @Nullable final String scope, final String label) {
        super(owner);
        if (label == null) throw new NullPointerException("Null label");
        this.scope = scope;
        this.label = label;
        this.hash = Objects.hash(LabelConstraint.class, this.owner, this.scope, this.label);
    }

    public static LabelConstraint of(final TypeVariable owner, final graql.lang.pattern.constraint.TypeConstraint.Label constraint) {
        return new LabelConstraint(owner, constraint.scope().orElse(null), constraint.label());
    }

    public Optional<String> scope() {
        return Optional.ofNullable(scope);
    }

    public String label() {
        return label;
    }

    public String scopedLabel() {
        return (scope != null ? scope + ":" : "") + label;
    }

    @Override
    public List<Traversal> traversals() {
        if (traversals == null) traversals = list(Traversal.Property.Label.of(owner.reference(), label, scope));
        return traversals;
    }

    @Override
    public boolean isLabel() {
        return true;
    }

    @Override
    public LabelConstraint asLabel() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final LabelConstraint that = (LabelConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.label.equals(that.label) &&
                Objects.equals(this.scope, that.scope));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
