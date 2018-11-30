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

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;

/**
 * Represents the {@code sub!} property on a {@link Type}.
 * This property can be queried or inserted.
 * This property relates a {@link Type} and another {@link Type}. It indicates
 * that every instance of the left type is also an instance of the right type.
 * When matching, subtyping is ignored:
 * if
 * `man sub person`,
 * `person sub entity`,
 * then
 * `$x sub! entity` only returns `person` but not `man`
 */
public class SubExplicitProperty extends AbstractSubProperty {

    public static final String NAME = "sub!";

    private final Statement superType;

    public SubExplicitProperty(Statement superType) {
        if (superType == null) {
            throw new NullPointerException("Null superType");
        }
        this.superType = superType;
    }

    @Override
    public Statement superType() {
        return superType;
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

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof SubExplicitProperty) {
            SubExplicitProperty that = (SubExplicitProperty) o;
            return (this.superType.equals(that.superType()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.superType.hashCode();
        return h;
    }
}
