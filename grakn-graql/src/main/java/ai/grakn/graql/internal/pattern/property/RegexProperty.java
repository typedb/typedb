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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.property.RegexAtom;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code regex} property on a {@link ResourceType}.
 *
 * This property can be queried and inserted.
 *
 * This property introduces a validation constraint on instances of this {@link ResourceType}, stating that their
 * values must conform to the given regular expression.
 *
 * @author Felix Chapman
 */
public class RegexProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    private final String regex;

    public RegexProperty(String regex) {
        this.regex = regex;
    }

    public String getRegex() {
        return regex;
    }

    @Override
    public String getName() {
        return "regex";
    }

    @Override
    public String getProperty() {
        return StringConverter.valueToString(regex);
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return ImmutableSet.of(EquivalentFragmentSets.regex(start, regex));
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        concept.asResourceType().setRegex(regex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegexProperty that = (RegexProperty) o;

        return regex.equals(that.regex);

    }

    @Override
    public int hashCode() {
        return regex.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        return new RegexAtom(var.getVarName(), this, parent);
    }
}
