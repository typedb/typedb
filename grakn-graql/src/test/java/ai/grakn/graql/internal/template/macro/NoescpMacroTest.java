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
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertEquals;

public class NoescpMacroTest {

    private final NoescpMacro noescpMacro = new NoescpMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyNoescpMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Wrong number of arguments");

        noescpMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyNoescpMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Wrong number of arguments");

        noescpMacro.apply(ImmutableList.of("1.0", "2.0"));
    }

    @Test
    public void applyNoescpMacroToInt_UnescapedIsReturned(){
        assertEquals(Unescaped.class, noescpMacro.apply(ImmutableList.of(10L)).getClass());
    }

    @Test
    public void applyNoescpMacroToString_UnescapedIsReturned(){
        assertEquals(Unescaped.class, noescpMacro.apply(ImmutableList.of("string")).getClass());
    }

    @Test
    public void whenUsingNoescpMacroInTemplate_ResultIsAsExpected(){
        String template = "insert $this isa @noescp(<value>);";
        String expected = "insert $this0 isa whale;";

        Map<String, Object> data = Collections.singletonMap("value", "whale");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void whenUsingNoescpMacroInTemplateMultiple_ResultIsAsExpected(){
        String template = "insert $x has fn @noescp(<firstname>) has ln @noescp(<lastname>);";
        String expected = "insert $x0 has fn 4 has ln 5;";

        Map<String, Object> data = new HashMap<>();
        data.put("firstname", "4");
        data.put("lastname", "5");

        assertParseEquals(template, data, expected);
    }
}
