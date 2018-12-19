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
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Stream;

import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.relates;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.sub;

/**
 * Represents the {@code relates} property on a RelationshipType.
 * This property can be queried, inserted or deleted.
 * This property relates a RelationshipType and a Role. It indicates that a Relationship whose
 * type is this RelationshipType may have a role-player playing the given Role.
 */
public class RelatesProperty extends VarProperty {

    private final Statement role;
    private final Statement superRole;

    public RelatesProperty(Statement role, @Nullable Statement superRole) {
        if (role == null) {
            throw new NullPointerException("Null role");
        }
        this.role = role;
        this.superRole = superRole;
    }

    public Statement role() {
        return role;
    }

    public Statement superRole() {
        return superRole;
    }

    @Override
    public String getName() {
        return "relates";
    }

    @Override
    public String getProperty() {
        StringBuilder builder = new StringBuilder(role.getPrintableName());
        if (superRole != null) {
            builder.append(" as ").append(superRole.getPrintableName());
        }
        return builder.toString();
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        Statement superRole = this.superRole;
        EquivalentFragmentSet relates = relates(this, start, role.var());
        if (superRole == null) {
            return ImmutableSet.of(relates);
        } else {
            return ImmutableSet.of(relates, sub(this, role.var(), superRole.var()));
        }
    }

    @Override
    public Stream<Statement> getTypes() {
        return Stream.of(role);
    }

    @Override
    public Stream<Statement> innerStatements() {
        return superRole == null ? Stream.of(role) : Stream.of(superRole, role);
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            RelationshipType relationshipType = executor.get(var).asRelationshipType();
            Role role = executor.get(this.role.var()).asRole();

            if (!relationshipType.isDeleted() && !role.isDeleted()) {
                relationshipType.unrelate(role);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, role.var()).build());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RelatesProperty) {
            RelatesProperty that = (RelatesProperty) o;
            return (this.role.equals(that.role))
                    && ((this.superRole == null) ? (that.superRole == null) : this.superRole.equals(that.superRole));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.role.hashCode();
        h *= 1000003;
        h ^= (superRole == null) ? 0 : this.superRole.hashCode();
        return h;
    }
}
