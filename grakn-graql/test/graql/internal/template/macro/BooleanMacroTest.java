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

package ai.grakn.graql.internal.template.macro;

import ai.grakn.exception.GraqlQueryException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BooleanMacroTest {

    private final BooleanMacro booleanMacro = new BooleanMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyBooleanMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        booleanMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyBooleanMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        booleanMacro.apply(ImmutableList.of(true, true));
    }

    @Test
    public void applyBooleanMacroToOneArgument_ItReturnsNonNull(){
        assertNotNull(booleanMacro.apply(ImmutableList.of("true")));
    }

    @Test
    public void applyBooleanMacroToInvalidValue_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        booleanMacro.apply(ImmutableList.of("invalid"));
    }

    @Test
    public void applyBooleanMacroToTrueString_ReturnsTrue(){
        assertTrue(booleanMacro.apply(ImmutableList.of("true")));
    }

    @Test
    public void applyBooleanMacroToFalseString_ReturnsFalse(){
        assertFalse(booleanMacro.apply(ImmutableList.of("false")));
    }

    @Test
    public void whenUsingBooleanMacroInTemplate_ResultIsAsExpected(){
        String template = "insert $x == @boolean(<value>);";
        String expected = "insert $x0 == true;";

        assertParseEquals(template, Collections.singletonMap("value", "true"), expected);
        assertParseEquals(template, Collections.singletonMap("value", "True"), expected);

        expected = "insert $x0 == false;";

        assertParseEquals(template, Collections.singletonMap("value", "false"), expected);
        assertParseEquals(template, Collections.singletonMap("value", "False"), expected);
    }
}
