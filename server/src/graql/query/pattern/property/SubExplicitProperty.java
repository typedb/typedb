/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;

/**
 * Represents the {@code sub!} property on a {@link Type}.
 *
 * This property can be queried or inserted.
 *
 * This property relates a {@link Type} and another {@link Type}. It indicates
 * that every instance of the left type is also an instance of the right type.
 * When matching, subtyping is ignored:
 * if
 * `man sub person`,
 * `person sub entity`,
 * then
 * `$x sub! entity` only returns `person` but not `man`
 *
 */
@AutoValue
public abstract class SubExplicitProperty extends AbstractSubProperty {

    public static final String NAME = "sub!";

    public static SubExplicitProperty of(Statement superType) {
        return new AutoValue_SubExplicitProperty(superType);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isExplicit() { return true;}

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(EquivalentFragmentSets.sub(this, start, superType().var(), true));
    }
}
