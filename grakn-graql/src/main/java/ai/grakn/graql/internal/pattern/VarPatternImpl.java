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

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.HasAttributeProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

/**
 * Implementation of {@link VarPattern} interface
 */
@AutoValue
abstract class VarPatternImpl extends AbstractVarPattern {

    protected final Logger LOG = LoggerFactory.getLogger(VarPatternImpl.class);

    @Override
    public abstract Var var();

    @Override
    protected abstract Set<VarProperty> properties();

    @Override
    public final boolean equals(Object o) {
        // This equals implementation is special: it considers all non-user-defined vars as equivalent
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractVarPattern var = (AbstractVarPattern) o;

        if (var().isUserDefinedName() != var.var().isUserDefinedName()) return false;

        // "simplifying" this makes it harder to read
        //noinspection SimplifiableIfStatement
        if (!properties().equals(var.properties())) return false;

        return !var().isUserDefinedName() || var().equals(var.var());

    }

    @Override
    public final int hashCode() {
        // This hashCode implementation is special: it considers all non-user-defined vars as equivalent
        int result = properties().hashCode();
        if (var().isUserDefinedName()) result = 31 * result + var().hashCode();
        result = 31 * result + (var().isUserDefinedName() ? 1 : 0);
        return result;
    }

    @Override
    public final String toString() {
        Collection<VarPatternAdmin> innerVars = innerVarPatterns();
        innerVars.remove(this);
        getProperties(HasAttributeProperty.class)
                .map(HasAttributeProperty::attribute)
                .flatMap(r -> r.innerVarPatterns().stream())
                .forEach(innerVars::remove);

        if (innerVars.stream().anyMatch(VarPatternImpl::invalidInnerVariable)) {
            LOG.warn("printing a query with inner variables, which is not supported in native Graql");
        }

        StringBuilder builder = new StringBuilder();

        String name = var().isUserDefinedName() ? var().toString() : "";

        builder.append(name);

        if (var().isUserDefinedName() && !properties().isEmpty()) {
            // Add a space after the var name
            builder.append(" ");
        }

        boolean first = true;

        for (VarProperty property : properties()) {
            if (!first) {
                builder.append(" ");
            }
            first = false;
            property.buildString(builder);
        }

        return builder.toString();
    }

    private static boolean invalidInnerVariable(VarPatternAdmin var) {
        return var.getProperties().anyMatch(p -> !(p instanceof LabelProperty));
    }
}
