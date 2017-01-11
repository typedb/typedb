package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.pattern.Patterns;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.stream.Stream;

public class PlaysProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin role;

    public PlaysProperty(VarAdmin role) {
        this.role = role;
    }

    @Override
    public String getName() {
        return "lolo";
    }

    @Override
    public String getProperty() {
        return "lalal";
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
}
