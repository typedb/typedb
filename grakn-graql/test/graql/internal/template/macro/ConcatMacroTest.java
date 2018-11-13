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
import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import static grakn.core.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConcatMacroTest {

    private final ConcatMacro concatMacro = new ConcatMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyConcatMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        concatMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyConcatMacroThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        concatMacro.apply(ImmutableList.of("true"));
    }

    @Test
    public void applyConcatMacroToTwoOrMoreArguments_ResultIsNonNull(){
        String result = concatMacro.apply(ImmutableList.of("true", "false"));

        assertNotNull(result);
    }

    @Test
    public void applyConcatMacroToString_StringsAreConcatenated(){
        String result = concatMacro.apply(ImmutableList.of("this", " ", "that"));

        assertEquals("this that", result);
    }

    @Test
    public void applyConcatMacroToNumbers_NumbersAreConcatenated(){
        String result = concatMacro.apply(ImmutableList.of(4, 5));

        assertEquals("45", result);
    }

    @Test
    public void whenUsingConcatMacroInTemplate_ResultIsAsExpected(){
        String template = "insert $x == @concat(<value1>, <value2>);";
        String expected = "insert $x0 == \"thing5\";";

        ImmutableMap<String, Object> data = ImmutableMap.of(
                "value1", "thing",
                "value2", 5
        );

        assertParseEquals(template, data, expected);
    }
}
