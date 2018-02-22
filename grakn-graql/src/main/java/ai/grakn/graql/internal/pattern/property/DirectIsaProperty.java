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

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.binary.type.IsaAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code direct-isa} property on a {@link Thing}.
 * <p>
 * This property can be queried and inserted.
 * </p>
 * <p>
 * THe property is defined as a relationship between an {@link Thing} and a {@link Type}.
 * </p>
 * <p>
 * When matching, any subtyping is ignored. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it only follows {@code $bob isa person}, not {@code bob isa entity}.
 * </p>
 *
 * @author Felix Chapman
 * @author Jason Liu
 */
@AutoValue
public abstract class DirectIsaProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    public static final String NAME = "direct-isa";

    public static DirectIsaProperty of(VarPatternAdmin directType) {
        return new AutoValue_DirectIsaProperty(directType);
    }

    public abstract VarPatternAdmin directType();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return directType().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(
                EquivalentFragmentSets.isa(this, start, directType().var(), true)
        );
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(directType());
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(directType());
    }

    @Override
    public Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(this.directType().var()).asType();
            executor.builder(var).directIsa(type);
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(directType().var()).produces(var).build());
    }

    @Override
    public void checkValidProperty(GraknTx graph, VarPatternAdmin var) throws GraqlQueryException {
        directType().getTypeLabel().ifPresent(typeLabel -> {
            SchemaConcept theSchemaConcept = graph.getSchemaConcept(typeLabel);
            if (theSchemaConcept != null && !theSchemaConcept.isType()) {
                throw GraqlQueryException.cannotGetInstancesOfNonType(typeLabel);
            }
        });
    }

    @Nullable
    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationshipProperty.class)) return null;

        Var varName = var.var().asUserDefined();
        VarPatternAdmin typePattern = this.directType();
        Var typeVariable = typePattern.var();

        IdPredicate predicate = getIdPredicate(typeVariable, typePattern, vars, parent);

        //isa part
        VarPatternAdmin isaVar = varName.isa(typeVariable).admin();
        return new IsaAtom(isaVar, typeVariable, predicate, parent);
    }
}
