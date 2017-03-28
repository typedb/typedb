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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.TypeName;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collection;

import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.ConceptProperty.INDEX;
import static ai.grakn.util.Schema.generateResourceIndex;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HasResourcePropertyTest {

    TypeName resourceTypeWithoutSubTypes = TypeName.of("name");
    TypeName resourceTypeWithSubTypes = TypeName.of("resource");
    String literalValue = "Bob";
    VarAdmin literalVar = var().value(literalValue).admin();
    VarAdmin comparatorVar = var().value(gt(literalValue)).admin();

    @Test
    public void whenPropertyRefersToATypeWithoutSubTypesAndALiteralValue_UseResourceIndex() {
        HasResourceProperty hasResource = HasResourceProperty.of(resourceTypeWithoutSubTypes, literalVar);

        Collection<EquivalentFragmentSet> fragmentSets = hasResource.match(VarName.of("x"));

        assertThat(fragmentSets, hasSize(1));
        EquivalentFragmentSet fragmentSet = fragmentSets.iterator().next();

        assertThat(fragmentSet.fragments(), hasSize(1));
        Fragment fragment = fragmentSet.fragments().iterator().next();

        //noinspection unchecked
        GraphTraversal<Vertex, Vertex> traversal = mock(GraphTraversal.class);

        fragment.applyTraversal(traversal);

        verify(traversal).has(INDEX.name(), generateResourceIndex(resourceTypeWithoutSubTypes, literalValue));
    }

    @Test
    public void whenPropertyRefersToATypeWithSubtypes_DoNotUseResourceIndex() {
        HasResourceProperty hasResource = HasResourceProperty.of(resourceTypeWithSubTypes, literalVar);

        Collection<EquivalentFragmentSet> fragmentSets = hasResource.match(VarName.of("x"));

        assertThat(fragmentSets, hasSize(1));
        EquivalentFragmentSet fragmentSet = fragmentSets.iterator().next();
        assertThat(fragmentSet.fragments(), hasSize(2));
    }

    @Test
    public void whenPropertyRefersToAResourceComparator_DoNotUseResourceIndex() {
        HasResourceProperty hasResource = HasResourceProperty.of(resourceTypeWithoutSubTypes, comparatorVar);

        Collection<EquivalentFragmentSet> fragmentSets = hasResource.match(VarName.of("x"));

        assertThat(fragmentSets, hasSize(1));
        EquivalentFragmentSet fragmentSet = fragmentSets.iterator().next();
        assertThat(fragmentSet.fragments(), hasSize(2));
    }

    @Test
    public void whenPropertyRefersToAResourceVariable_DoNotUseResourceIndex() {
        HasResourceProperty hasResource = HasResourceProperty.of(
                resourceTypeWithoutSubTypes, var("y").value(literalValue).admin()
        );

        Collection<EquivalentFragmentSet> fragmentSets = hasResource.match(VarName.of("x"));

        assertThat(fragmentSets, hasSize(1));
        EquivalentFragmentSet fragmentSet = fragmentSets.iterator().next();
        assertThat(fragmentSet.fragments(), hasSize(2));
    }

    @Test
    public void whenPropertyDoesNotHaveAResourceType_DoNotUseResourceIndex() {
        HasResourceProperty hasResource = HasResourceProperty.of(literalVar);

        Collection<EquivalentFragmentSet> fragmentSets = hasResource.match(VarName.of("x"));

        assertThat(fragmentSets, hasSize(1));
        EquivalentFragmentSet fragmentSet = fragmentSets.iterator().next();
        assertThat(fragmentSet.fragments(), hasSize(2));
    }
}