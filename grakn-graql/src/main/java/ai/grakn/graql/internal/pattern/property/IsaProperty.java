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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.ShortcutTraversal;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.Utility.getIdPredicate;

/**
 * Represents the {@code isa} property on a {@link Instance}.
 *
 * This property can be queried and inserted.
 *
 * THe property is defined as a relationship between an {@link Instance} and a {@link Type}.
 *
 * When matching, any subtyping is respected. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it follows that {@code $bob isa person} and {@code bob isa entity}.
 *
 * @author Felix Chapman
 */
public class IsaProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    private final VarAdmin type;

    public IsaProperty(VarAdmin type) {
        this.type = type;
    }

    public VarAdmin getType() {
        return type;
    }

    @Override
    public String getName() {
        return "isa";
    }

    @Override
    public String getProperty() {
        return type.getPrintableName();
    }

    @Override
    public void modifyShortcutTraversal(ShortcutTraversal shortcutTraversal) {
        Optional<TypeLabel> typeLabel = type.getTypeLabel();
        if (typeLabel.isPresent()) {
            shortcutTraversal.setType(typeLabel.get());
        } else {
            shortcutTraversal.setInvalid();
        }
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return ImmutableSet.of(EquivalentFragmentSets.isa(start, type.getVarName()));
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(type);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(type);
    }

    @Override
    public void checkValidProperty(GraknGraph graph, VarAdmin var) throws IllegalStateException {
        type.getTypeLabel().ifPresent(typeLabel -> {
            Type theType = graph.getType(typeLabel);
            if (theType != null && theType.isRoleType()) {
                throw new IllegalStateException(ErrorMessage.INSTANCE_OF_ROLE_TYPE.getMessage(typeLabel));
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IsaProperty that = (IsaProperty) o;

        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationProperty.class)) return null;

        VarName varName = var.getVarName();
        VarAdmin typeVar = this.getType();
        VarName typeVariable = typeVar.getVarName();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        //isa part
        VarAdmin resVar = Graql.var(varName).isa(Graql.var(typeVariable)).admin();
        return new TypeAtom(resVar, predicate, parent);
    }
}
