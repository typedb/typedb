/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.internal.pattern.property;

import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.Var;
import grakn.core.graql.VarPattern;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;

/**
 * Represents the {@code isa-explicit} property on a {@link Thing}.
 * <p>
 * This property can be queried and inserted.
 * </p>
 * <p>
 * THe property is defined as a relationship between an {@link Thing} and a {@link Type}.
 * </p>
 * <p>
 * When matching, any subtyping is ignored. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it only follows {@code $bob isa man}, not {@code bob isa entity}.
 * </p>
 *
 */
@AutoValue
public abstract class IsaExplicitProperty extends AbstractIsaProperty {

    public static final String NAME = "isa!";

    public static IsaExplicitProperty of(VarPatternAdmin directType) {
        return new AutoValue_IsaExplicitProperty(directType);
    }

    @Override
    public boolean isExplicit() { return true;}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(
                EquivalentFragmentSets.isa(this, start, type().var(), true)
        );
    }

    @Override
    protected final VarPattern varPatternForAtom(Var varName, Var typeVariable) {
        return varName.isaExplicit(typeVariable);
    }
}
