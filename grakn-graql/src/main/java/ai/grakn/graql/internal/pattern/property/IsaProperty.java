/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;

/**
 * Represents the {@code isa} property on a {@link Thing}.
 * <p>
 * This property can be queried and inserted.
 * </p>
 * <p>
 * THe property is defined as a relationship between an {@link Thing} and a {@link Type}.
 * </p>
 * <p>
 * When matching, any subtyping is respected. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it follows that {@code $bob isa person} and {@code bob isa entity}.
 * </p>
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class IsaProperty extends AbstractIsaProperty {

    public static final String NAME = "isa";

    public static IsaProperty of(VarPatternAdmin type) {
        return new AutoValue_IsaProperty(type, Graql.var());
    }

    public abstract Var directTypeVar();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(
                EquivalentFragmentSets.isa(this, start, directTypeVar(), true),
                EquivalentFragmentSets.sub(this, directTypeVar(), type().var())
        );
    }

    @Override
    protected final VarPattern varPatternForAtom(Var varName, Var typeVariable) {
        return varName.isa(typeVariable);
    }

    // TODO: These are overridden so we ignore `directType`, which ideally shouldn't be necessary
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IsaProperty) {
            IsaProperty that = (IsaProperty) o;
            return this.type().equals(that.type());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.type().hashCode();
        return h;
    }

}
