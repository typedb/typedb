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

import grakn.core.graql.exception.GraqlQueryException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import static grakn.core.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LongMacroTest {

    private final LongMacro longMacro = new LongMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyLongMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        longMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyLongMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        longMacro.apply(ImmutableList.of("1.0", "2.0"));
    }

    @Test
    public void applyLongMacroToOneArgument_ItReturnsNonNull(){
        assertNotNull(longMacro.apply(ImmutableList.of("1")));
    }

    @Test
    public void applyLongMacroToInvalidValue_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(containsString("is not a long"));

        longMacro.apply(ImmutableList.of("invalid"));
    }

    @Test
    public void applyLongMacroToIntValue_ReturnsCorrectLong(){
        assertEquals(new Long(1), longMacro.apply(ImmutableList.of("1")));
    }

    @Test
    public void applyLongMacroToVeryLongValue_ReturnsCorrectLong(){
        assertEquals(new Long(123456789123456789L), longMacro.apply(ImmutableList.of("123456789123456789")));
    }

    @Test
    public void applyLongMacroToDoubleValue_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(containsString("is not a long"));

        longMacro.apply(ImmutableList.of("15.0"));
    }

    @Test
    public void applyLongMacroToLongValueWithUnderscores_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(containsString("is not a long"));

        longMacro.apply(ImmutableList.of("15_000"));
    }

    @Test
    public void whenUsingLongMacroInTemplate_ItExecutesCorrectly(){
        String template = "insert $x == @long(<value>);";
        String expected = "insert $x0 == 4;";

        assertParseEquals(template, Collections.singletonMap("value", "4"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4), expected);
    }
}
