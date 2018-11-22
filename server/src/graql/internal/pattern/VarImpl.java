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

package grakn.core.graql.internal.pattern;

import grakn.core.graql.query.Var;
import grakn.core.graql.admin.VarProperty;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Default implementation of {@link Var}.
 *
 */
@AutoValue
abstract class VarImpl extends AbstractVarPattern implements Var {

    private static final Pattern VALID_VAR = Pattern.compile("[a-zA-Z0-9_-]+");

    static VarImpl of(String value, Kind kind) {
        Preconditions.checkArgument(
                VALID_VAR.matcher(value).matches(), "Var value [%s] is invalid. Must match regex %s", value, VALID_VAR
        );

        return new AutoValue_VarImpl(value, kind);
    }

    @Override
    public boolean isUserDefinedName() {
        return kind() == Kind.UserDefined;
    }

    @Override
    public Var asUserDefined() {
        if (isUserDefinedName()) {
            return this;
        } else {
            return VarImpl.of(getValue(), Kind.UserDefined);
        }
    }

    @Memoized
    @Override
    public String name() {
        return kind().prefix() + getValue();
    }

    @Override
    public String shortName() {
        return kind().prefix() + StringUtils.right(getValue(), 3);
    }

    @Override
    public Var var() {
        return this;
    }

    @Override
    protected Set<VarProperty> properties() {
        return ImmutableSet.of();
    }

    @Override
    public String toString() {
        return "$" + getValue();
    }

    @Override
    public final boolean equals(Object o) {
        // This equals implementation is special: it ignores whether a variable is user-defined
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarImpl varName = (VarImpl) o;

        return getValue().equals(varName.getValue());
    }

    @Override
    public final int hashCode() {
        // This hashCode implementation is special: it ignores whether a variable is user-defined
        return getValue().hashCode();
     }
}
