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
import java.util.HashMap;
import java.util.Map;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static org.junit.Assert.assertEquals;

public class NoescpMacroTest {

    private final NoescpMacro noescpMacro = new NoescpMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applyNoescpMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("Wrong number of arguments");

        noescpMacro.apply(Collections.emptyList());
    }

    @Test
    public void applyNoescpMacroToMoreThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);
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
    public void whenUsingNoescpMacroToConcatenateValues_ResultingStringIsConcatenated(){
        String template = "insert $x isa @noescp(<first>)-@noescp(<last>);";
        String expected = "insert $x0 isa one-two;";

        Map<String, Object> data = new HashMap<>();
        data.put("first", "one");
        data.put("last", "two");

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

    @Test
    public void whenUsingNoescpMacroInVariableConcatenatedWithString_ResultIsAsExpected(){
        String template = "insert $@noescp(<pokemon_id>)-pokemon isa pokemon;\n$@noescp(<type_id>)-type isa pokemon-type;";
        String expected = "insert $124-pokemon isa pokemon;\n$124-type isa pokemon-type;";

        Map<String, Object> data = new HashMap<>();
        data.put("pokemon_id", 124);
        data.put("type_id", 124);

        assertParseEquals(template, data, expected);
    }
}
