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

package grakn.core.graql.internal.template.macro;

import grakn.core.server.exception.GraqlQueryException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Locale;

import static grakn.core.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DoubleMacroTest {

    private final DoubleMacro doubleMacro = new DoubleMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyDoubleMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        doubleMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyDoubleMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        doubleMacro.apply(ImmutableList.of("1.0", "2.0"));
    }

    @Test
    public void applyDoubleMacroToOneArgument_ItReturnsNonNull(){
        assertNotNull(doubleMacro.apply(ImmutableList.of("1.0")));
    }

    @Test
    public void applyDoubleMacroToInvalidValue_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        doubleMacro.apply(ImmutableList.of("invalid"));
    }

    @Test
    public void applyDoubleMacroToIntValue_ReturnsCorrectIntAsDouble(){
        assertEquals(new Double(1.0), doubleMacro.apply(ImmutableList.of("1")));
    }

    @Test
    public void applyDoubleMacroToDoubleValue_ReturnsCorrectDouble(){
        assertEquals(new Double(15.0), doubleMacro.apply(ImmutableList.of("15.0")));
    }

    @Test
    public void applyDoubleMacroToScientificNotation_ReturnsCorrectDouble(){
        assertEquals(new Double(180000000000.0), doubleMacro.apply(ImmutableList.of("1.8E+11")));
    }

    @Test
    public void applyDoubleMacroToLargeDouble_ReturnsCorrectDouble(){
        assertEquals(new Double(191588629.5), doubleMacro.apply(ImmutableList.of("191588629.5")));
    }

    @Test
    public void whenUsingDoubleMacroInTemplate_ResultIsAsExpected(){
        String template = "insert $x == @double(<value>);";
        String expected = "insert $x0 == 4.0;";

        assertParseEquals(template, Collections.singletonMap("value", "4.0"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4.0), expected);
    }

    @Test
    public void whenUsingDoubleMacroToParseString_Throw(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.wrongMacroArgumentType(doubleMacro, "-").getMessage());

        doubleMacro.apply(ImmutableList.of("-"));
    }

    @Test
    public void whenParsingDoubleInFrenchLocale_DontUseComma(){
        Locale defaultLocale = Locale.getDefault();

        try {
            Locale.setDefault(Locale.FRANCE);

            String template = "insert $x == @double(<value>);";
            String expected = "insert $x0 == 4.0;";

            assertParseEquals(template, Collections.singletonMap("value", "4.0"), expected);
            assertParseEquals(template, Collections.singletonMap("value", 4.0), expected);
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }
}
