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

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.common.collect.ImmutableList;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConjunctionQueryTest {
    private TypeLabel resourceTypeWithoutSubTypesLabel = TypeLabel.of("name");
    private TypeLabel resourceTypeWithSubTypesLabel = TypeLabel.of("resource");
    private Var resourceTypeWithoutSubTypes = Graql.label(resourceTypeWithoutSubTypesLabel);
    private Var resourceTypeWithSubTypes = Graql.label(resourceTypeWithSubTypesLabel);
    private String literalValue = "Bob";
    private GraknGraph graph;
    private VarName x = VarName.of("x");
    private VarName y = VarName.of("y");

    @Before
    public void setUp() {
        graph = mock(GraknGraph.class);

        Type resourceTypeWithoutSubTypesMock = mock(Type.class);
        doReturn(ImmutableList.of(resourceTypeWithoutSubTypesMock)).when(resourceTypeWithoutSubTypesMock).subTypes();

        Type resourceTypeWithSubTypesMock = mock(Type.class);
        doReturn(ImmutableList.of(resourceTypeWithoutSubTypesMock, resourceTypeWithSubTypesMock))
                .when(resourceTypeWithSubTypesMock).subTypes();

        when(graph.getType(resourceTypeWithoutSubTypesLabel)).thenReturn(resourceTypeWithoutSubTypesMock);
        when(graph.getType(resourceTypeWithSubTypesLabel)).thenReturn(resourceTypeWithSubTypesMock);
    }

    @Test
    public void whenVarRefersToATypeWithoutSubTypesAndALiteralValue_UseResourceIndex() {
        assertThat(var(x).isa(resourceTypeWithoutSubTypes).val(literalValue), usesResourceIndex());
    }

    @Test
    public void whenVarHasTwoResources_UseResourceIndexForBoth() {
        Pattern pattern = and(
                var(x).isa(resourceTypeWithoutSubTypes).val("Foo"),
                var(y).isa(resourceTypeWithoutSubTypes).val("Bar")
        );

        assertThat(pattern, allOf(usesResourceIndex(x, "Foo"), usesResourceIndex(y, "Bar")));
    }

    @Test
    public void whenVarRefersToATypeWithAnExplicitVarName_UseResourceIndex() {
        assertThat(var(x).isa(var(y).label(resourceTypeWithoutSubTypesLabel)).val(literalValue), usesResourceIndex());
    }

    @Test
    public void whenQueryUsesHasSyntax_UseResourceIndex() {
        assertThat(
                var(x).has(resourceTypeWithoutSubTypesLabel, var(y).val(literalValue)),
                usesResourceIndex(y, literalValue)
        );
    }

    @Test
    public void whenVarCanUseResourceIndexAndHasOtherProperties_UseResourceIndex() {
        assertThat(
                var(x).isa(resourceTypeWithoutSubTypes).val(literalValue).id(ConceptId.of("123")),
                usesResourceIndex()
        );
    }

    @Test
    public void whenVarRefersToATypeWithSubtypes_DoNotUseResourceIndex() {
        assertThat(var(x).isa(resourceTypeWithSubTypes).val(literalValue), not(usesResourceIndex()));
    }

    @Test
    public void whenVarHasAValueComparator_DoNotUseResourceIndex() {
        assertThat(var(x).isa(resourceTypeWithoutSubTypes).val(gt(literalValue)), not(usesResourceIndex()));
    }

    @Test
    public void whenVarDoesNotHaveAType_DoNotUseResourceIndex() {
        assertThat(var(x).val(literalValue), not(usesResourceIndex()));
    }

    @Test
    public void whenVarDoesNotHaveAValue_DoNotUseResourceIndex() {
        assertThat(var(x).val(resourceTypeWithoutSubTypes), not(usesResourceIndex()));
    }

    private Matcher<Pattern> usesResourceIndex() {
        return usesResourceIndex(x, literalValue);
    }

    private Matcher<Pattern> usesResourceIndex(VarName varName, Object value) {
        Fragment resourceIndexFragment = Fragments.resourceIndex(varName, resourceTypeWithoutSubTypesLabel, value);

        return feature(hasItem(contains(resourceIndexFragment)), "fragment sets", pattern -> {
            Conjunction<VarAdmin> conjunction = pattern.admin().getDisjunctiveNormalForm().getPatterns().iterator().next();
            return new ConjunctionQuery(conjunction, graph).getEquivalentFragmentSets();
        });
    }

    private <T, U> Matcher<T> feature(Matcher<? super U> subMatcher, String name, Function<T, U> extractor) {
        return new FeatureMatcher<T, U>(subMatcher, name, name) {

            @Override
            protected U featureValueOf(T actual) {
                return extractor.apply(actual);
            }
        };
    }
}