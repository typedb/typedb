/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.reasoner.atom.binary.RelatesAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.relates;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.sub;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code relates} property on a {@link RelationshipType}.
 * <p>
 * This property can be queried, inserted or deleted.
 * <p>
 * This property relates a {@link RelationshipType} and a {@link Role}. It indicates that a {@link Relationship} whose
 * type is this {@link RelationshipType} may have a role-player playing the given {@link Role}.
 *
 * @author Felix Chapman
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
        return Stream.of(role());
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
                relationshipType.deleteRelates(role);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, role().var()).build());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.var().asUserDefined();
        VarPatternAdmin roleVar = this.role();
        Var roleVariable = roleVar.var().asUserDefined();
        IdPredicate predicate = getIdPredicate(roleVariable, roleVar, vars, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;
        return RelatesAtom.create(varName, roleVariable, predicateId, parent);
    }
}
