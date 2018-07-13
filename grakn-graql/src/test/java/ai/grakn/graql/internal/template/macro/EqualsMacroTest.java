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
import java.util.HashMap;
import java.util.Map;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EqualsMacroTest {

    private final EqualsMacro equalsMacro = new EqualsMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();


    @Test
    public void applyEqualsMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        equalsMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyEqualsMacroToOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        equalsMacro.apply(ImmutableList.of("1.0"));
    }

    @Test
    public void applyEqualsMacroToEqualObject_ItReturnsTrue(){
        assertTrue(equalsMacro.apply(ImmutableList.of("1", "1")));
    }

    @Test
    public void applyEqualsMacroToObjectsWithDifferentTypes_ItReturnsFalse(){
        assertFalse(equalsMacro.apply(ImmutableList.of("1", 1)));
    }

    @Test
    public void applyEqualsMacroToObjectsWithDifferentValues_ItReturnsTrue(){
        assertFalse(equalsMacro.apply(ImmutableList.of("1", "2")));
    }

    @Test
    public void whenUsingEqualsMacroInTemplate_ItExecutesCorrectly(){
        String template = "insert $x == @equals(<this>, <that>);";
        String expected = "insert $x0 == true;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", "50");
        data.put("that", "50");

        assertParseEquals(template, data, expected);

        template = "insert $x == @equals(<this>, <notThat>);";
        expected = "insert $x0 == false;";

        data = new HashMap<>();
        data.put("this", "50");
        data.put("notThat", "500");

        assertParseEquals(template, data, expected);

        template = "insert $x == @equals(<this>, <notThat>);";
        expected = "insert $x0 == false;";

        data = new HashMap<>();
        data.put("this", "50");
        data.put("notThat", 50);

        assertParseEquals(template, data, expected);

        template = "insert $x == @equals(<this>, <that>, <those>);";
        expected = "insert $x0 == true;";

        data = new HashMap<>();
        data.put("this", 50);
        data.put("that", 50);
        data.put("those", 50);

        assertParseEquals(template, data, expected);

        template = "insert $x == @equals(<this>, <that>, <notThat>);";
        expected = "insert $x0 == false;";

        data = new HashMap<>();
        data.put("this", 50);
        data.put("that", 50);
        data.put("notThat", 50.0);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void whenUsingEqualsMacroInTemplateFunction_ItExecutesCorrectly(){
        String template = "if (@equals(<this>, <that>)) do { insert $this isa equals; } else { insert $this isa not; }";
        String expected = "insert $this0 isa equals;";
        Map<String, Object> data = new HashMap<>();
        data.put("this", "50");
        data.put("that", "50");

        assertParseEquals(template, data, expected);

        expected = "insert $this0 isa not;";
        data = new HashMap<>();
        data.put("this", "50");
        data.put("that", "500");

        assertParseEquals(template, data, expected);
    }
}
