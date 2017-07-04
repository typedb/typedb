/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

import java.util.Set;
import java.util.function.Function;

/**
 * Default implementation of {@link Var}.
 *
 * @author Felix Chapman
 */
final class VarImpl extends AbstractVarPattern implements Var {

    private final String value;

    private final boolean userDefinedName;

    VarImpl(String value, boolean userDefinedName) {
        this.value = value;
        this.userDefinedName = userDefinedName;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Var map(Function<String, String> mapper) {
        return Graql.var(mapper.apply(value));
    }

    @Override
    public boolean isUserDefinedName() {
        return userDefinedName;
    }

    @Override
    public Var asUserDefined() {
        if (userDefinedName) {
            return this;
        } else {
            return new VarImpl(value, true);
        }
    }

    @Override
    public String shortName() {
        return "$" + StringUtils.left(value, 3);
    }

    @Override
    public Var getVarName() {
        return this;
    }

    @Override
    protected Set<VarProperty> properties() {
        return ImmutableSet.of();
    }

    @Override
    public String toString() {
        return "$" + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarImpl varName = (VarImpl) o;

        return value.equals(varName.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
