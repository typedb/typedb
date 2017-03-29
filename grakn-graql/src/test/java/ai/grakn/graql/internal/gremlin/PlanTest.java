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
 *
 */

package ai.grakn.graql.internal.gremlin;

import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outIsa;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.shortcut;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class PlanTest {


    @Test
    public void planComplexityShouldAlwaysEqualCostFromGraqlTraversal() {
        VarName a = VarName.of("a");
        VarName x = VarName.of("x");
        VarName y = VarName.of("y");
        VarName z = VarName.of("z");

        Fragment outIsa = outIsa(y, a);
        outIsa.setEquivalentFragmentSet(mock(EquivalentFragmentSet.class));

        Fragment shortcut = shortcut(Optional.empty(), Optional.empty(), Optional.empty(), y, z);
        shortcut.setEquivalentFragmentSet(mock(EquivalentFragmentSet.class));

        Plan plan = Plan.base();
        plan.tryPush(outIsa);
        plan.tryPush(shortcut);

        List<Fragment> fragments = plan.fragments();

        double traversalComplexity = GraqlTraversal.create(ImmutableSet.of(fragments)).getComplexity();
        double planCost = plan.cost();

        assertEquals(traversalComplexity, planCost, 10.0);
    }
}