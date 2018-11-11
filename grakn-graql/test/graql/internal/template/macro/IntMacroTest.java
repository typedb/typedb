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

import grakn.core.exception.GraqlQueryException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import static grakn.core.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IntMacroTest {

    private final IntMacro intMacro = new IntMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyIntMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        intMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyIntMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        intMacro.apply(ImmutableList.of("1.0", "2.0"));
    }

    @Test
    public void applyIntMacroToOneArgument_ItReturnsNonNull(){
        assertNotNull(intMacro.apply(ImmutableList.of("1")));
    }

    @Test
    public void applyIntMacroToInvalidValue_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.wrongMacroArgumentType(intMacro, "invalid").getMessage());

        intMacro.apply(ImmutableList.of("invalid"));
    }

    @Test
    public void applyIntMacroToIntValue_ReturnsCorrectInt(){
        assertEquals(new Integer(1), intMacro.apply(ImmutableList.of("1")));
    }

    @Test
    public void applyIntMacroToDoubleValue_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.wrongMacroArgumentType(intMacro, "15.0").getMessage());

        intMacro.apply(ImmutableList.of("15.0"));
    }

    @Test
    public void applyIntMacroToIntValueWithUnderscores_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.wrongMacroArgumentType(intMacro, "15_000").getMessage());

        intMacro.apply(ImmutableList.of("15_000"));
    }

    @Test
    public void whenUsingIntMacroInTemplate_ItExecutesCorrectly(){
        String template = "insert $x == @int(<value>);";
        String expected = "insert $x0 == 4;";

        assertParseEquals(template, Collections.singletonMap("value", "4"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4), expected);
    }

    @Test
    public void whenIntMacroIsWrongCase_ResolvedToLowerCase(){
        String template = "insert $x == @InT(<value>);";
        String expected = "insert $x0 == 4;";

        assertParseEquals(template, Collections.singletonMap("value", "4"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4), expected);
    }
}
