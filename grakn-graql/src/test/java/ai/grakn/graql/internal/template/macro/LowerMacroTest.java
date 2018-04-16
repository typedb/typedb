/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.template.macro;

import ai.grakn.exception.GraqlQueryException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Map;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class LowerMacroTest {

    private final LowerMacro lowerMacro = new LowerMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyLowerMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        lowerMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyLowerMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        lowerMacro.apply(ImmutableList.of("1.0", "2.0"));
    }

    @Test
    public void applyLowerMacroToOneArgument_ItReturnsNonNull(){
        assertNotNull(lowerMacro.apply(ImmutableList.of("string")));
    }

    @Test
    public void applyLowerMacroToNumber_ItReturnsNumberAsString(){
        Number number = 0L;
        String numberLower = lowerMacro.apply(ImmutableList.of(number));
        assertEquals(number.toString(), numberLower);
    }

    @Test
    public void applyLowerMacroToUpperCaseString_ItReturnsStringInLowerCase(){
        String upperOriginal = "WHALE";
        String lower = lowerMacro.apply(ImmutableList.of(upperOriginal));
        assertEquals(upperOriginal.toLowerCase(), lower);
    }

    @Test
    public void whenUsingLowerMacroInTemplate_ResultIsAsExpected(){
        String template = "insert $this has something @lower(<value>);";
        String expected = "insert $this0 has something \"camelcasevalue\";";

        Map<String, Object> data = Collections.singletonMap("value", "camelCaseValue");
        assertParseEquals(template, data, expected);
    }
}
