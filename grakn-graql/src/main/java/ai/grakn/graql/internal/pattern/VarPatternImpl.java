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
 */

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

/**
 * Implementation of {@link VarPattern} interface
 */
class VarPatternImpl extends AbstractVarPattern {

    protected final Logger LOG = LoggerFactory.getLogger(VarPatternImpl.class);

    private final Var name;

    private final Set<VarProperty> properties;

    VarPatternImpl(Var name, Set<VarProperty> properties) {
        this.name = name;
        this.properties = properties;
    }

    @Override
    public Var getVarName() {
        return name;
    }

    @Override
    protected Set<VarProperty> properties() {
        return properties;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractVarPattern var = (AbstractVarPattern) o;

        if (getVarName().isUserDefinedName() != var.getVarName().isUserDefinedName()) return false;

        // "simplifying" this makes it harder to read
        //noinspection SimplifiableIfStatement
        if (!properties().equals(var.properties())) return false;

        return !getVarName().isUserDefinedName() || getVarName().equals(var.getVarName());

    }

    @Override
    public final int hashCode() {
        int result = properties().hashCode();
        if (getVarName().isUserDefinedName()) result = 31 * result + getVarName().hashCode();
        result = 31 * result + (getVarName().isUserDefinedName() ? 1 : 0);
        return result;
    }

    @Override
    public final String toString() {
        Collection<VarPatternAdmin> innerVars = getInnerVars();
        innerVars.remove(this);
        getProperties(HasResourceProperty.class)
                .map(HasResourceProperty::getResource)
                .flatMap(r -> r.getInnerVars().stream())
                .forEach(innerVars::remove);

        if (innerVars.stream().anyMatch(VarPatternImpl::invalidInnerVariable)) {
            LOG.warn("printing a query with inner variables, which is not supported in native Graql");
        }

        StringBuilder builder = new StringBuilder();

        String name = getVarName().isUserDefinedName() ? getVarName().toString() : "";

        builder.append(name);

        if (getVarName().isUserDefinedName() && !properties().isEmpty()) {
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
