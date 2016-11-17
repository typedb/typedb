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

package ai.grakn.test.graql.template;

import ai.grakn.graql.Graql;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class MacroTest {

    // noescp macro

    @Test
    public void noescpMacroOneVarTest(){
        String template = "insert $this isa @noescp(value);";
        String expected = "insert $this0 isa whale;";

        Map<String, Object> data = Collections.singletonMap("value", "whale");

        assertParseEquals(template, data, expected);
    }

    @Test
    public void noescpMacroMultiVarTest(){
        String template = "insert $x has fn @noescp(firstname) has ln @noescp(lastname);";
        String expected = "insert $x0 has fn 4 has ln 5;";

        Map<String, Object> data = new HashMap<>();
        data.put("firstname", "4");
        data.put("lastname", "5");

        assertParseEquals(template, data, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noescpMacroBreaksWithWrongNumberArguments(){
        String template = "@noescp(value otherValue)";
        Graql.parseTemplate(template, new HashMap<>());
    }

    // int macro

    @Test
    public void intMacroTest(){
        String template = "insert $x value @int(value);";
        String expected = "insert $x0 value 4;";

        assertParseEquals(template, Collections.singletonMap("value", "4"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4), expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void intMacroBreaksWithWrongNumberArguments0(){
        assertParseEquals("@int()", Collections.emptyMap(), "");
    }

    // double macro

    @Test
    public void doubleMacroTest(){
        String template = "insert $x value @double(value);";
        String expected = "insert $x0 value 4.0;";

        assertParseEquals(template, Collections.singletonMap("value", "4.0"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4.0), expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void doubleMacroBreaksWithWrongNumberArguments(){
        assertParseEquals("@double()", Collections.emptyMap(), "");
    }

    // equals

    @Test
    public void equalsMacroTest(){
        String template = "insert $x value @equals(this that);";
        String expected = "insert $x0 value true;";

        Map<String, Object> data = new HashMap<>();
        data.put("this", "50");
        data.put("that", "50");

        assertParseEquals(template, data, expected);

        template = "insert $x value @equals(this notThat);";
        expected = "insert $x0 value false;";

        data = new HashMap<>();
        data.put("this", "50");
        data.put("notThat", "500");

        assertParseEquals(template, data, expected);

        template = "insert $x value @equals(this notThat);";
        expected = "insert $x0 value false;";

        data = new HashMap<>();
        data.put("this", "50");
        data.put("notThat", 50);

        assertParseEquals(template, data, expected);

        template = "insert $x value @equals(this that those);";
        expected = "insert $x0 value true;";

        data = new HashMap<>();
        data.put("this", 50);
        data.put("that", 50);
        data.put("those", 50);

        assertParseEquals(template, data, expected);

        template = "insert $x value @equals(this that notThat);";
        expected = "insert $x0 value false;";

        data = new HashMap<>();
        data.put("this", 50);
        data.put("that", 50);
        data.put("notThat", 50.0);

        assertParseEquals(template, data, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void equalsMacroBreaksWithWrongNumberArguments(){
        assertParseEquals("@equals(this)", Collections.singletonMap("this", "one"), "");
    }

    @Test
    public void macroInArgumentTest(){
        String template = "if (@equals(this that)) do { insert $this isa equals; } else { insert $this isa not; }";
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

    @Test
    public void stringMacroTest(){
        String template = "insert $this isa @string(value);";
        String expected = "insert $this0 isa \"1000\";";

        Map<String, Object> data = Collections.singletonMap("value", 1000);

        assertParseEquals(template, data, expected);
    }

    @Test
    public void longMacroTest(){
        String template = "insert $x value @long(value);";
        String expected = "insert $x0 value 4;";

        assertParseEquals(template, Collections.singletonMap("value", "4"), expected);
        assertParseEquals(template, Collections.singletonMap("value", 4), expected);
    }

    @Test
    public void convertDateFormatMacroTest(){
        String template = "insert $x value @date(date \"mm/dd/yyyy\" \"dd/mm/yyyy\");";
        String expected = "insert $x0 value \"09\\/10\\/1993\";";

        assertParseEquals(template, Collections.singletonMap("date", "10/09/1993"), expected);
    }

    @Test
    public void convertDateToEpochMacroTest() throws Exception {
    	java.text.DateFormat format = new java.text.SimpleDateFormat("mm/dd/yyyy");
    	String dateAsString = "10/09/1993";
    	long time = format.parse(dateAsString).getTime();
        String template = "insert $x value @date(date \"mm/dd/yyyy\");";
        String expected = "insert $x0 value \"" + time + "\";";
        assertParseEquals(template, Collections.singletonMap("date", dateAsString), expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDateFormatTest(){
        String template = "insert $x value @date(date \"this is not a format\");";
        String expected = "insert $x0 value \"726538200000\";";

        assertParseEquals(template, Collections.singletonMap("date", "10/09/1993"), expected);
    }

    private void assertParseEquals(String template, Map<String, Object> data, String expected){
        String result = Graql.parseTemplate(template, data).toString();
        assertEquals(expected, result);
    }
}
