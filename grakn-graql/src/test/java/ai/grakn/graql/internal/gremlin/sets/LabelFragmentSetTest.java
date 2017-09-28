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

package ai.grakn.graql.internal.gremlin.sets;

import ai.grakn.GraknTx;
import ai.grakn.concept.Type;
import ai.grakn.concept.Label;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.isa;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.label;
import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.sub;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LabelFragmentSetTest {

    private static final Var generatedVar = Graql.var();
    private static final Var otherGeneratedVar = Graql.var();
    private static final Var userDefinedVar = Graql.var("x");
    private static final Label EXISTING_LABEL = Label.of("something");
    private static final Label NON_EXISTENT_LABEL = Label.of("doesn't exist");

    private GraknTx graph;

    @Before
    public void setUp() {
        graph = mock(GraknTx.class);

        when(graph.getSchemaConcept(EXISTING_LABEL)).thenReturn(mock(Type.class));
        when(graph.getSchemaConcept(NON_EXISTENT_LABEL)).thenReturn(null);
    }

    @Test
    public void whenOptimisingQueryWithGeneratedVarLabel_EliminateLabelFragmentSet() {
        EquivalentFragmentSet labelFragment = label(null, generatedVar, ImmutableSet.of(EXISTING_LABEL));

        Set<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                labelFragment,
                isa(null, Graql.var("abc"), Graql.var("def"), true)
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, graph);

        assertEquals(Sets.difference(originalFragmentSets, ImmutableSet.of(labelFragment)), fragmentSets);
    }

    @Test
    public void whenOptimisingQueryContainingOnlyASingleFragment_DoNotEliminateLabelFragmentSet() {
        Collection<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, generatedVar, ImmutableSet.of(EXISTING_LABEL))
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, graph);

        assertEquals(originalFragmentSets, fragmentSets);
    }

    @Test
    public void whenOptimisingQueryWithUserDefinedVarLabel_DoNotEliminateLabelFragmentSet() {
        Collection<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, userDefinedVar, ImmutableSet.of(EXISTING_LABEL))
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, graph);

        assertEquals(originalFragmentSets, fragmentSets);
    }

    @Test
    public void whenOptimisingQueryWithLabelConnectedToAnyVar_DoNotEliminateLabelFragmentSet() {
        Set<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, generatedVar, ImmutableSet.of(EXISTING_LABEL)),
                sub(null, otherGeneratedVar, generatedVar)
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, graph);

        assertEquals(originalFragmentSets, fragmentSets);
    }

    @Test
    public void whenOptimisingQueryWithLabelReferringToNonExistentType_DoNotEliminateLabelFragmentSet() {
        Collection<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, generatedVar, ImmutableSet.of(NON_EXISTENT_LABEL))
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, graph);

        assertEquals(originalFragmentSets, fragmentSets);
    }
}