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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;

/**
 * Represents the {@code isa} property on a Thing.
 * This property can be queried and inserted.
 * THe property is defined as a relationship between an Thing and a Type.
 * When matching, any subtyping is respected. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it follows that {@code $bob isa person} and {@code bob isa entity}.
 */
public class IsaProperty extends IsaAbstractProperty {

    public static final String NAME = "isa";
    private final Statement type;

    public IsaProperty(Statement type) {
        if (type == null) {
            throw new NullPointerException("Null type");
        }
        this.type = type;
    }

    @Override
    public Statement type() {
        return type;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        Variable directTypeVar = Pattern.var();
        return ImmutableSet.of(
                EquivalentFragmentSets.isa(this, start, directTypeVar, true),
                EquivalentFragmentSets.sub(this, directTypeVar, type().var())
        );
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
