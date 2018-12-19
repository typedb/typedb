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

package grakn.core.graql.query.pattern;

import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

/**
 * Implementation of {@link Statement} interface
 */
abstract class StatementBase extends Statement {

    private final Variable var;
    private final Set<VarProperty> properties;
    protected final Logger LOG = LoggerFactory.getLogger(StatementBase.class);
    private int hashCode = 0;

    StatementBase(Variable var, Set<VarProperty> properties) {
        if (var == null) {
            throw new NullPointerException("Null var");
        }
        this.var = var;
        if (properties == null) {
            throw new NullPointerException("Null properties");
        }
        this.properties = properties;
    }

    @Override
    public Variable var() {
        return var;
    }

    @Override
    protected Set<VarProperty> properties() {
        return properties;
    }

    @Override
    public Conjunction<?> asConjunction() {
        return Patterns.and(Collections.singleton(this));
    }

    @Override
    public boolean isConjunction() {
        return true;
    }

    @Override
    public final boolean equals(Object o) {
        // This equals implementation is special: it considers all non-user-defined vars as equivalent
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Statement var = (Statement) o;

        if (var().isUserDefinedName() != var.var().isUserDefinedName()) return false;

        // "simplifying" this makes it harder to read
        //noinspection SimplifiableIfStatement
        if (!properties().equals(var.properties())) return false;

        return !var().isUserDefinedName() || var().equals(var.var());

    }

    @Override
    public final int hashCode() {
        if (hashCode == 0) {
            // This hashCode implementation is special: it considers all non-user-defined vars as equivalent
            hashCode = properties().hashCode();
            if (var().isUserDefinedName()) hashCode = 31 * hashCode + var().hashCode();
            hashCode = 31 * hashCode + (var().isUserDefinedName() ? 1 : 0);
            hashCode = 31 * hashCode + (var().isPositive() ? 1 : 0);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        Collection<Statement> innerVars = innerStatements();
        innerVars.remove(this);
        getProperties(HasAttributeProperty.class)
                .map(HasAttributeProperty::attribute)
                .flatMap(r -> r.innerStatements().stream())
                .forEach(innerVars::remove);

        if (innerVars.stream().anyMatch(StatementBase::invalidInnerVariable)) {
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
            builder.append(property.toString());
        }

        return builder.toString();
    }

    private static boolean invalidInnerVariable(Statement var) {
        return var.getProperties().anyMatch(p -> !(p instanceof LabelProperty));
    }
}
