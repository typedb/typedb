package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.pattern.Patterns;
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
        VarName casting = Patterns.varName();

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

        VarAdmin resVar = Graql.var(varName).playsRole(Graql.var(typeVariable)).admin();
        return new TypeAtom(resVar, predicate, parent);
    }
}
