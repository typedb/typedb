/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.gremlin;

import grakn.core.concept.Label;
import grakn.core.concept.type.Type;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.fragment.Fragments;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Stream;

import static grakn.core.graql.gremlin.GraqlMatchers.feature;
import static graql.lang.Graql.and;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConjunctionQueryTest {
    private Label resourceTypeWithoutSubTypesLabel = Label.of("name");
    private Label resourceTypeWithSubTypesLabel = Label.of("resource");
    private Statement resourceTypeWithoutSubTypes = Graql.type(resourceTypeWithoutSubTypesLabel.getValue());
    private Statement resourceTypeWithSubTypes = Graql.type(resourceTypeWithSubTypesLabel.getValue());
    private String literalValue = "Bob";
    private TransactionOLTP tx;
    private Statement x = Graql.var("x");
    private Statement y = Graql.var("y");

    @SuppressWarnings("ResultOfMethodCallIgnored") // Mockito confuses IntelliJ
    @Before
    public void setUp() {
        tx = mock(TransactionOLTP.class);

        Type resourceTypeWithoutSubTypesMock = mock(Type.class);
        doAnswer((answer) -> Stream.of(resourceTypeWithoutSubTypesMock)).when(resourceTypeWithoutSubTypesMock).subs();
        when(resourceTypeWithoutSubTypesMock.label()).thenReturn(resourceTypeWithoutSubTypesLabel);

        Type resourceTypeWithSubTypesMock = mock(Type.class);
        doAnswer((answer) -> Stream.of(resourceTypeWithoutSubTypesMock, resourceTypeWithSubTypesMock))
                .when(resourceTypeWithSubTypesMock).subs();
        when(resourceTypeWithSubTypesMock.label()).thenReturn(resourceTypeWithSubTypesLabel);

        when(tx.getSchemaConcept(resourceTypeWithoutSubTypesLabel)).thenReturn(resourceTypeWithoutSubTypesMock);
        when(tx.getSchemaConcept(resourceTypeWithSubTypesLabel)).thenReturn(resourceTypeWithSubTypesMock);
    }

    @Test
    public void whenVarRefersToATypeWithoutSubTypesAndALiteralValue_UseResourceIndex() {
        assertThat(x.isa(resourceTypeWithoutSubTypes).val(literalValue), usesResourceIndex());
    }

    @Test
    public void whenVarHasTwoResources_UseResourceIndexForBoth() {
        Pattern pattern = and(
                x.isa(resourceTypeWithoutSubTypes).val("Foo"),
                y.isa(resourceTypeWithoutSubTypes).val("Bar")
        );

        assertThat(pattern, allOf(usesResourceIndex(x.var(), "Foo"), usesResourceIndex(y.var(), "Bar")));
    }

    @Test
    public void whenVarRefersToATypeWithAnExplicitVarName_UseResourceIndex() {
        assertThat(x.isa(y.type(resourceTypeWithoutSubTypesLabel.getValue())).val(literalValue), usesResourceIndex());
    }

    @Test
    public void whenQueryUsesHasSyntax_UseResourceIndex() {
        assertThat(
                x.has(resourceTypeWithoutSubTypesLabel.getValue(), y.val(literalValue)),
                usesResourceIndex(y.var(), literalValue)
        );
    }

    @Test
    public void whenVarCanUseResourceIndex_UseResourceIndex() {
        assertThat(
                x.isa(resourceTypeWithoutSubTypes).val(literalValue),
                usesResourceIndex()
        );
    }

    @Test
    public void whenVarCanUseResourceIndexAndThereIsAnotherVarThatCannot_UseResourceIndex() {
        assertThat(
                and(x.isa(resourceTypeWithoutSubTypes).val(literalValue), y.val(literalValue)),
                usesResourceIndex()
        );

        assertThat(
                and(y.isa(resourceTypeWithoutSubTypes).val(literalValue), x.val(literalValue)),
                usesResourceIndex(y.var(), literalValue)
        );
    }

    @Test
    public void whenVarRefersToATypeWithSubtypes_DoNotUseResourceIndex() {
        assertThat(x.isa(resourceTypeWithSubTypes).val(literalValue), not(usesResourceIndex()));
    }

    @Test
    public void whenVarHasAValueComparator_DoNotUseResourceIndex() {
        assertThat(x.isa(resourceTypeWithoutSubTypes).gt(literalValue), not(usesResourceIndex()));
    }

    @Test
    public void whenVarDoesNotHaveAType_DoNotUseResourceIndex() {
        assertThat(x.val(literalValue), not(usesResourceIndex()));
    }

    @Test
    public void whenVarHasAValuePredicateThatRefersToAVar_DoNotUseResourceIndex() {
        assertThat(x.eq(y).isa(resourceTypeWithoutSubTypes), not(usesResourceIndex(x.var(), y.var())));
    }

    private Matcher<Pattern> usesResourceIndex() {
        return usesResourceIndex(x.var(), literalValue);
    }

    private Matcher<Pattern> usesResourceIndex(Variable varName, Object value) {
        Fragment resourceIndexFragment = Fragments.attributeIndex(null, varName, resourceTypeWithoutSubTypesLabel, value);

        return feature(hasItem(contains(resourceIndexFragment)), "fragment sets", pattern -> {
            Conjunction<Statement> conjunction = pattern.getDisjunctiveNormalForm().getPatterns().iterator().next();
            return new ConjunctionQuery(conjunction, tx).getEquivalentFragmentSets();
        });
    }
}