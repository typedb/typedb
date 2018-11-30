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
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.property.RegexAtom;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.util.StringUtil;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code regex} property on a {@link AttributeType}. This property can be queried and inserted.
 * This property introduces a validation constraint on instances of this {@link AttributeType}, stating that their
 * values must conform to the given regular expression.
 */
public class RegexProperty extends VarProperty {

    private final String regex;

    public RegexProperty(String regex) {
        if (regex == null) {
            throw new NullPointerException("Null regex");
        }
        this.regex = regex;
    }

    public String regex() {
        return regex;
    }

    @Override
    public String getName() {
        return "regex";
    }

    @Override
    public String getProperty() {
        return "/" + StringUtil.escapeString(regex()) + "/";
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public Atomic mapToAtom(Statement var, Set<Statement> vars, ReasonerQuery parent) {
        return RegexAtom.create(var.var(), this, parent);
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(EquivalentFragmentSets.regex(this, start, regex()));
    }

    @Override
    public Collection<PropertyExecutor> define(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.get(var).asAttributeType().regex(regex());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            AttributeType<Object> attributeType = executor.get(var).asAttributeType();
            if (!attributeType.isDeleted() && regex().equals(attributeType.regex())) {
                attributeType.regex(null);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var).build());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RegexProperty) {
            RegexProperty that = (RegexProperty) o;
            return (this.regex.equals(that.regex()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.regex.hashCode();
        return h;
    }
}
