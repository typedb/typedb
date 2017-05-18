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
 */

package ai.grakn.graql.internal.template.macro;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Locale;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DoubleMacroTest {

    private final DoubleMacro doubleMacro = new DoubleMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyDoubleMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Wrong number of arguments");

        doubleMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyDoubleMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Wrong number of arguments");

        doubleMacro.apply(ImmutableList.of("1.0", "2.0"));
    }

    @Test
    public void applyDoubleMacroToOneArgument_ItReturnsNonNull(){
        assertNotNull(doubleMacro.apply(ImmutableList.of("1.0")));
    }

    @Test
    public void applyDoubleMacroToInvalidValue_ExceptionIsThrown(){
        exception.expect(NumberFormatException.class);

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
    public void applyDoubleMacroToDoubleValueWithUnderscores_ReturnsCorrectDouble(){
        assertEquals(new Double(15000.0), doubleMacro.apply(ImmutableList.of("15_000.0")));
    }

    @Test
    public void whenUsingDoubleMacroInTemplate_ResultIsAsExpected(){
        String template = "insert $x val @double(<value>);";
        String expected = "insert $x0 val 4.0;";

        assertParseEquals(template, Collections.singletonMap("value", "4.0"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4.0), expected);
    }

    @Test
    public void whenParsingDoubleInFrenchLocale_DontUseComma(){
        Locale.setDefault(Locale.FRANCE);
        String template = "insert $x val @double(<value>);";
        String expected = "insert $x0 val 4.0;";

        assertParseEquals(template, Collections.singletonMap("value", "4.0"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4.0), expected);
    }
}
