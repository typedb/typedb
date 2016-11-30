package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.stream.Stream;

public class NeqProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin var;

    public NeqProperty(VarAdmin var) {
        this.var = var;
    }

    @Override
    public String getName() {
        return "!=";
    }

    @Override
    public String getProperty() {
        return var.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(String start) {
        return Sets.newHashSet(
                EquivalentFragmentSet.create(Fragments.notCasting(start)),
                EquivalentFragmentSet.create(Fragments.notCasting(var.getVarName())),
                EquivalentFragmentSet.create(Fragments.neq(start, var.getVarName()), Fragments.neq(var.getVarName(), start))
        );
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(var);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NeqProperty that = (NeqProperty) o;

        return var.equals(that.var);
    }

    @Override
    public int hashCode() {
        return var.hashCode();
    }
}
