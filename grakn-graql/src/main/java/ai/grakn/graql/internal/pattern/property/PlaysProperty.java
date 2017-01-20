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

import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.Utility.getIdPredicate;

public class PlaysProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin role;

    public PlaysProperty(VarAdmin role) {
        this.role = role;
    }

    public VarAdmin getRole() {
        return role;
    }

    @Override
    public String getName() {
        return "plays";
    }

    @Override public String getProperty() {
        return role.getPrintableName();
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(role);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(role);
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        VarName casting = VarName.anon();

        return Sets.newHashSet(
                EquivalentFragmentSet.create(
                        Fragments.inRolePlayer(start, casting),
                        Fragments.outRolePlayer(casting, start)
                ),
                EquivalentFragmentSet.create(
                        Fragments.outIsaCastings(casting, role.getVarName()),
                        Fragments.inIsaCastings(role.getVarName(), casting)
                )
        );
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        VarName varName = var.getVarName();
        VarAdmin typeVar = this.getRole();
        VarName typeVariable = typeVar.isUserDefinedName() ?
                typeVar.getVarName() : varName.map(name -> name + "-" + getName() + "-" + UUID.randomUUID().toString());
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        VarAdmin resVar = Graql.var(varName).plays(Graql.var(typeVariable)).admin();
        return new TypeAtom(resVar, predicate, parent);
    }
}
