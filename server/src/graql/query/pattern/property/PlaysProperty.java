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
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Reperesents the {@code plays} property on a Type.
 * This property relates a Type and a Role. It indicates that an
 * Thing whose type is this Type is permitted to be a role-player
 * playing the role of the given Role.
 */
public class PlaysProperty extends VarProperty {

    public static final String NAME = "plays";
    private final Statement role;
    private final boolean required;

    public PlaysProperty(Statement role, boolean required) {
        if (role == null) {
            throw new NullPointerException("Null role");
        }
        this.role = role;
        this.required = required;
    }

    public Statement role() {
        return role;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return role.getPrintableName();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(EquivalentFragmentSets.plays(this, start, role.var(), required));
    }

    @Override
    public Stream<Statement> getTypes() {
        return Stream.of(role);
    }

    @Override
    public Stream<Statement> innerStatements() {
        return Stream.of(role);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof PlaysProperty) {
            PlaysProperty that = (PlaysProperty) o;
            return (this.role.equals(that.role))
                    && (this.required == that.required);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.role.hashCode();
        h *= 1000003;
        h ^= this.required ? 1231 : 1237;
        return h;
    }
}