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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
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

public class UpperMacroTest {

    private final UpperMacro upperMacro = new UpperMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyLowerMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        upperMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyLowerMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        upperMacro.apply(ImmutableList.of("1.0", "2.0"));
    }

    @Test
    public void applyLowerMacroToOneArgument_ItReturnsNonNull(){
        assertNotNull(upperMacro.apply(ImmutableList.of("string")));
    }

    @Test
    public void applyLowerMacroToNumber_ItReturnsNumberAsString(){
        Number number = 0L;
        String numberLower = upperMacro.apply(ImmutableList.of(number));
        assertEquals(number.toString(), numberLower);
    }

    @Test
    public void applyLowerMacroToUpperCaseString_ItReturnsStringInLowerCase(){
        String upperOriginal = "whale";
        String lower = upperMacro.apply(ImmutableList.of(upperOriginal));
        assertEquals(upperOriginal.toUpperCase(), lower);
    }

    @Test
    public void whenUsingUpperMacroInTemplate_ResultIsAsExpected(){
        String template = "insert $this has something @upper(<value>);";
        String expected = "insert $this0 has something \"CAMELCASEVALUE\";";

        Map<String, Object> data = Collections.singletonMap("value", "camelCaseValue");
        assertParseEquals(template, data, expected);
    }
}
