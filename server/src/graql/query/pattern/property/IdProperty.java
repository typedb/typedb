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
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.util.StringUtil;

import java.util.Collection;

/**
 * Represents the {@code id} property on a Concept.
 * This property can be queried. While this property cannot be inserted, if used in an insert query any existing concept
 * with the given ID will be retrieved.
 */
public class IdProperty extends VarProperty {

    public static final String NAME = "id";

    private final ConceptId id;

    public IdProperty(ConceptId id) {
        if (id == null) {
            throw new NullPointerException("Null id");
        }
        this.id = id;
    }

    public ConceptId id() {
        return id;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return StringUtil.idToString(id());
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public boolean uniquelyIdentifiesConcept() {
        return true;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(EquivalentFragmentSets.id(this, start, id()));
    }

    @Override
    public Collection<PropertyExecutor> insert(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).id(id());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }

    @Override
    public Collection<PropertyExecutor> define(Variable var) throws GraqlQueryException {
        // This property works in both insert and define queries, because it is only for look-ups
        return insert(var);
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        // This property works in undefine queries, because it is only for look-ups
        return insert(var);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IdProperty) {
            IdProperty that = (IdProperty) o;
            return (this.id.equals(that.id()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.id.hashCode();
        return h;
    }
}
