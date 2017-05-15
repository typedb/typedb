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

package ai.grakn.graql.internal.template;

import ai.grakn.graql.Graql;
import ai.grakn.graql.Query;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static ai.grakn.graql.Graql.parse;
import static junit.framework.TestCase.assertEquals;

public class MacroTest {


    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void noescpMacroOneVarTest(){
        String template = "insert $this isa @noescp(<value>);";
        String expected = "insert $this0 isa whale;";

        Map<String, Object> data = Collections.singletonMap("value", "whale");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void noescpMacroMultiVarTest(){
        String template = "insert $x has fn @noescp(<firstname>) has ln @noescp(<lastname>);";
        String expected = "insert $x0 has fn 4 has ln 5;";

        Map<String, Object> data = new HashMap<>();
        data.put("firstname", "4");
        data.put("lastname", "5");

        assertParseEquals(template, data, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noescpMacroBreaksWithWrongNumberArguments(){
        String template = "@noescp(<value>, <otherValue>)";
        //noinspection ResultOfMethodCallIgnored
        Graql.parseTemplate(template, new HashMap<>());
    }

    @Test
    public void stringMacroTest(){
        String template = "insert $this val @string(<value>);";
        String expected = "insert $this0 val \"1000\";";

        Map<String, Object> data = Collections.singletonMap("value", 1000);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void longMacroTest(){
        String template = "insert $x val @long(<value>);";
        String expected = "insert $x0 val 4;";

        assertParseEquals(template, Collections.singletonMap("value", "4"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4), expected);
    }

    @Test
    public void stringToUpperCaseTest(){
        String template = "insert $this has something @upper(<value>);";
        String expected = "insert $this0 has something \"CAMELCASEVALUE\";";

        Map<String, Object> data = Collections.singletonMap("value", "camelCaseValue");
        assertParseEquals(template, data, expected);
    }

    @Test
    public void stringToLowerCaseTest(){
        String template = "insert $this has something @lower(<value>);";
        String expected = "insert $this0 has something \"camelcasevalue\";";

        Map<String, Object> data = Collections.singletonMap("value", "camelCaseValue");
        assertParseEquals(template, data, expected);
    }

    @Test
    public void splitMacroTest() {
        String template = "insert $x for (val in @split(<list>, \",\") ) do { has description <val>};";
        String expected = "insert $x0 has description \"orange\" has description \"cat\" has description \"dog\";";

        assertParseEquals(template, Collections.singletonMap("list", "cat,dog,orange"), expected);
    }

    public static void assertParseEquals(String template, Map<String, Object> data, String expected){
        List<Query> result = Graql.parseTemplate(template, data);
        assertEquals(parse(expected), result.get(0));
    }
}
