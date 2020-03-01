/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.planning.gremlin.sets;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.statement.Variable;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.isa;
import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.label;
import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.sub;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LabelFragmentSetTest {

    private static final Variable generatedVar = new Variable();
    private static final Variable otherGeneratedVar = new Variable();
    private static final Variable userDefinedVar = new Variable("x");
    private static final Label EXISTING_LABEL = Label.of("something");
    private static final Label NON_EXISTENT_LABEL = Label.of("doesn't exist");

    private ConceptManager conceptManager;

    @Before
    public void setUp() {
        conceptManager = mock(ConceptManager.class);
        when(conceptManager.getSchemaConcept(EXISTING_LABEL)).thenReturn(mock(Type.class));
        when(conceptManager.getSchemaConcept(NON_EXISTENT_LABEL)).thenReturn(null);
    }

    @Test
    public void whenOptimisingQueryWithGeneratedVarLabel_EliminateLabelFragmentSet() {
        EquivalentFragmentSet labelFragment = label(null, generatedVar, ImmutableSet.of(EXISTING_LABEL));

        Set<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                labelFragment,
                isa(null, new Variable("abc"), new Variable("def"), true)
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, conceptManager);

        assertEquals(Sets.difference(originalFragmentSets, ImmutableSet.of(labelFragment)), fragmentSets);
    }

    @Test
    public void whenOptimisingQueryContainingOnlyASingleFragment_DoNotEliminateLabelFragmentSet() {
        Collection<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, generatedVar, ImmutableSet.of(EXISTING_LABEL))
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, conceptManager);

        assertEquals(originalFragmentSets, fragmentSets);
    }

    @Test
    public void whenOptimisingQueryWithUserDefinedVarLabel_DoNotEliminateLabelFragmentSet() {
        Collection<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, userDefinedVar, ImmutableSet.of(EXISTING_LABEL))
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, conceptManager);

        assertEquals(originalFragmentSets, fragmentSets);
    }

    @Test
    public void whenOptimisingQueryWithLabelConnectedToAnyVar_DoNotEliminateLabelFragmentSet() {
        Set<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, generatedVar, ImmutableSet.of(EXISTING_LABEL)),
                sub(null, otherGeneratedVar, generatedVar)
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, conceptManager);

        assertEquals(originalFragmentSets, fragmentSets);
    }

    @Test
    public void whenOptimisingQueryWithLabelReferringToNonExistentType_DoNotEliminateLabelFragmentSet() {
        Collection<EquivalentFragmentSet> originalFragmentSets = ImmutableSet.of(
                label(null, generatedVar, ImmutableSet.of(NON_EXISTENT_LABEL))
        );

        Collection<EquivalentFragmentSet> fragmentSets = Sets.newHashSet(originalFragmentSets);

        LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION.apply(fragmentSets, conceptManager);

        assertEquals(originalFragmentSets, fragmentSets);
    }
}