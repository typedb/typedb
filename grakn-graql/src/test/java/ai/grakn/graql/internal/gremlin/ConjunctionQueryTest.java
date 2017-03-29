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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import com.google.common.collect.Sets;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.function.Function;

import static ai.grakn.graql.Graql.gt;
import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.pattern.Patterns.conjunction;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class ConjunctionQueryTest {
    private TypeName resourceTypeName = TypeName.of("name");
    private Var resourceTypeWithoutSubTypes = name(resourceTypeName);
    private Var resourceTypeWithSubTypes = name(TypeName.of("resource"));
    private String literalValue = "Bob";

    @Test
    public void whenVarRefersToATypeWithoutSubTypesAndALiteralValue_UseResourceIndex() {
        assertThat(var("x").isa(resourceTypeWithoutSubTypes).value(literalValue), usesResourceIndex());
    }

    @Test
    public void whenVarCanUseResourceIndexAndHasOtherProperties_UseResourceIndex() {
        assertThat(
                var("x").isa(resourceTypeWithoutSubTypes).value(literalValue).id(ConceptId.of("123")),
                usesResourceIndex()
        );
    }

    @Test
    public void whenVarRefersToATypeWithSubtypes_DoNotUseResourceIndex() {
        assertThat(var("x").isa(resourceTypeWithSubTypes).value(literalValue), not(usesResourceIndex()));
    }

    @Test
    public void whenVarHasAValueComparator_DoNotUseResourceIndex() {
        assertThat(var("x").isa(resourceTypeWithoutSubTypes).value(gt(literalValue)), not(usesResourceIndex()));
    }

    @Test
    public void whenVarHasMultipleLiteralValues_DoNotUseResourceIndex() {
        assertThat(
                var("x").isa(resourceTypeWithoutSubTypes).value(literalValue).value("something else"),
                not(usesResourceIndex())
        );
    }

    @Test
    public void whenVarDoesNotHaveAType_DoNotUseResourceIndex() {
        assertThat(var("x").value(literalValue), not(usesResourceIndex()));
    }

    @Test
    public void whenVarDoesNotHaveAValue_DoNotUseResourceIndex() {
        assertThat(var("x").value(resourceTypeWithoutSubTypes), not(usesResourceIndex()));
    }

    private Matcher<Var> usesResourceIndex() {
        Fragment resourceIndexFragment = Fragments.resourceIndex(VarName.of("x"), resourceTypeName, literalValue);

        return feature(hasItem(contains(resourceIndexFragment)), "fragment sets", var ->
                new ConjunctionQuery(conjunction(Sets.newHashSet(var.admin()))).getEquivalentFragmentSets()
        );
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