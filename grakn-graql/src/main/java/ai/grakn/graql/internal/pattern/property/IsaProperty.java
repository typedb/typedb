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

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
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
 * Represents the {@code isa} property on a {@link Thing}.
 *
 * This property can be queried and inserted.
 *
 * THe property is defined as a relationship between an {@link Thing} and a {@link Type}.
 *
 * When matching, any subtyping is respected. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it follows that {@code $bob isa person} and {@code bob isa entity}.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class IsaProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    public static final String NAME = "isa";

    public static IsaProperty of(VarPatternAdmin type) {
        return new AutoValue_IsaProperty(Graql.var(), type);
    }

    public abstract Var directType();

    public abstract VarPatternAdmin type();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return type().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(
                EquivalentFragmentSets.isa(this, start, directType(), true),
                EquivalentFragmentSets.sub(this, directType(), type().var())
        );
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(type());
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(type());
    }

    @Override
    public PropertyExecutor insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(this.type().var()).asType();
            executor.builder(var).isa(type);
        };

        return PropertyExecutor.builder(method).requires(type().var()).produces(var).build();
    }

    @Override
    public void checkValidProperty(GraknTx graph, VarPatternAdmin var) throws GraqlQueryException {
        type().getTypeLabel().ifPresent(typeLabel -> {
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
        VarPatternAdmin typePattern = this.type();

        //TODO do not force user definedness
        Var typeVariable = typePattern.var();

        IdPredicate predicate = getIdPredicate(typeVariable, typePattern, vars, parent);

        //isa part
        VarPatternAdmin isaVar = varName.isa(typeVariable).admin();
        return new IsaAtom(isaVar, typeVariable, predicate, parent);
    }

    // TODO: These are overridden so we ignore `directType`, which ideally shouldn't be necessary
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IsaProperty) {
            IsaProperty that = (IsaProperty) o;
            return this.type().equals(that.type());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.type().hashCode();
        return h;
    }
}
