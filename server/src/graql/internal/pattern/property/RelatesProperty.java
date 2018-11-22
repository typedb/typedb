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

package grakn.core.graql.internal.pattern.property;

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.query.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.reasoner.atom.binary.RelatesAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.relates;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.sub;
import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code relates} property on a {@link RelationshipType}.
 * <p>
 * This property can be queried, inserted or deleted.
 * <p>
 * This property relates a {@link RelationshipType} and a {@link Role}. It indicates that a {@link Relationship} whose
 * type is this {@link RelationshipType} may have a role-player playing the given {@link Role}.
 *
 */
@AutoValue
public abstract class RelatesProperty extends AbstractVarProperty {

    public static RelatesProperty of(VarPatternAdmin role, @Nullable VarPatternAdmin superRole) {
        return new AutoValue_RelatesProperty(role, superRole);
    }

    public static RelatesProperty of(VarPatternAdmin role) {
        return RelatesProperty.of(role, null);
    }

    abstract VarPatternAdmin role();

    @Nullable
    abstract VarPatternAdmin superRole();

    @Override
    public String getName() {
        return "relates";
    }

    @Override
    public void buildString(StringBuilder builder) {
        VarPatternAdmin superRole = superRole();
        builder.append("relates").append(" ").append(role().getPrintableName());
        if (superRole != null) {
            builder.append(" as ").append(superRole.getPrintableName());
        }
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        VarPatternAdmin superRole = superRole();
        EquivalentFragmentSet relates = relates(this, start, role().var());
        if (superRole == null) {
            return ImmutableSet.of(relates);
        } else {
            return ImmutableSet.of(relates, sub(this, role().var(), superRole.var()));
        }
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(role());
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return superRole() == null ? Stream.of(role()) : Stream.of(superRole(), role());
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        Var roleVar = role().var();

        PropertyExecutor.Method relatesMethod = executor -> {
            Role role = executor.get(roleVar).asRole();
            executor.get(var).asRelationshipType().relates(role);
        };

        PropertyExecutor relatesExecutor = PropertyExecutor.builder(relatesMethod).requires(var, roleVar).build();

        // This allows users to skip stating `$roleVar sub role` when they say `$var relates $roleVar`
        PropertyExecutor.Method isRoleMethod = executor -> executor.builder(roleVar).isRole();

        PropertyExecutor isRoleExecutor = PropertyExecutor.builder(isRoleMethod).produces(roleVar).build();

        VarPatternAdmin superRoleVarPattern = superRole();
        if (superRoleVarPattern != null) {
            Var superRoleVar = superRoleVarPattern.var();
            PropertyExecutor.Method subMethod = executor -> {
                Role superRole = executor.get(superRoleVar).asRole();
                executor.builder(roleVar).sub(superRole);
            };

            PropertyExecutor subExecutor = PropertyExecutor.builder(subMethod)
                    .requires(superRoleVar).produces(roleVar).build();

            return ImmutableSet.of(relatesExecutor, isRoleExecutor, subExecutor);
        } else {
            return ImmutableSet.of(relatesExecutor, isRoleExecutor);
        }
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            RelationshipType relationshipType = executor.get(var).asRelationshipType();
            Role role = executor.get(this.role().var()).asRole();

            if (!relationshipType.isDeleted() && !role.isDeleted()) {
                relationshipType.unrelate(role);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, role().var()).build());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.var().asUserDefined();
        VarPatternAdmin roleVar = this.role();
        Var roleVariable = roleVar.var();
        IdPredicate predicate = getIdPredicate(roleVariable, roleVar, vars, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;
        return RelatesAtom.create(varName, roleVariable, predicateId, parent);
    }
}
